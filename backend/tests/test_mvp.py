from __future__ import annotations

import json
import io
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import pandas as pd
from fastapi import HTTPException
from fastapi.testclient import TestClient

from model_runtime.edim_model.core.integrated import (
    build_integrated_results,
    build_run_report_markdown,
    create_exchange_bundle_zip,
    validate_integrated_results,
)
from api_service.jobs import JobManager, JobQueueFullError
from model_runtime.edim_model.core.levers import load_lever_mappings
from api_service.main import create_app
from model_runtime.edim_model.core.mario_runtime import mario_inputs_health, run_mario_io_runtime
from api_service.runtime import (
    ArtifactRegistry,
    ExecutionAttemptRecord,
    ExecutionQueueMessage,
    LocalExecutionQueue,
    ModelExecutionContext,
    ModelExecutionRequest,
    ModelExecutionResult,
    ModelRuntimeManifest,
    RuntimeEventLog,
    build_model_run_bundle,
    load_artifact_manifest,
    load_model_runtime_manifest,
    validate_request_against_manifest,
)
from api_service.adapters import SubprocessModelRuntime
from model_runtime.edim_model.modules import get_energy_model_module, model_module_catalog, module_scenario_catalog
from model_runtime.edim_model.modules.base import ModelModuleError
from model_runtime.edim_model.core.orchestration import ModelStage, StageOrchestrator
from model_runtime.edim_model.core.scenario_package import (
    build_integrated_scenario_catalog,
    build_geography_alignment,
    build_mrio_direct_inputs,
    build_scenario_package,
    write_scenario_artifacts,
)
from model_runtime.edim_model.core.scenario_report import load_or_parse_scenario_report
from model_runtime.edim_model.core.runner import (
    _build_development_outputs,
    _results_health,
    _write_exchange_files_for_mario,
    _load_development_model_config,
    build_environment_setup_report,
)
from api_service.services.artifact_storage import LocalArtifactStorageService
from api_service.services.dataset_repository import LocalDatasetRepository
from api_service.services.artifact_storage import read_summary_json
from api_service.services.platform_repository import create_platform_repository
from model_runtime.edim_model.core.scenarios import load_scenario_metadata
from api_service.schemas import LeverValues, RunRequest
from api_service.settings import Settings
from model_runtime.edim_model.core.summarize import build_summary_diagnostics
from tools import backend_handoff_smoke


def _repo_calliope_root(repo_root: Path) -> Path:
    return repo_root / "model_runtime" / "model_modules" / "calliope" / "Calliope-Africa-main"


def _build_settings(base: Path, *, dedupe_enabled: bool = True, queue_capacity: int = 12) -> Settings:
    calliope_root = base / "model_runtime" / "model_modules" / "calliope" / "Calliope-Africa-main"
    calliope_root.mkdir(parents=True, exist_ok=True)
    (calliope_root / "model.yaml").write_text("model: {}", encoding="utf-8")
    (calliope_root / "overrides.yaml").write_text("{}", encoding="utf-8")

    runs_dir = base / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    config_dir = base / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "lever_mappings.csv").write_text(
        "\n".join(
            [
                "key,value",
                "renewables_tech,PV*",
                "fossil_tech,CCGT*",
                "capex_key_path,costs.monetary.energy_cap",
                "fuel_cost_key_path,costs.monetary.om_con",
                "carbon_price_path,run.objective_options.cost_class.co2",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "development_model.csv").write_text(
        "\n".join(
            [
                "parameter,value",
                "mario.uncertainty_relative_bounds.jobs_direct,0.12",
                "mario.uncertainty_relative_bounds.jobs_total,0.12",
                "mario.uncertainty_relative_bounds.gva_total_musd,0.12",
                "mario.uncertainty_relative_bounds.household_income_proxy_musd,0.12",
                "mario_direct.structural_reallocation_bridge_scale,0.25",
                "mario_direct.max_direct_to_bridge_ratio,1.0",
            ]
        ),
        encoding="utf-8",
    )
    repo_root = Path(__file__).resolve().parents[2]

    return Settings(
        calliope_root=calliope_root,
        runs_dir=runs_dir,
        config_dir=config_dir,
        dev_subset_start="2019-01-01",
        dev_subset_end="2019-01-02",
        analysis_subset_start="2019-01-01",
        analysis_subset_end="2019-03-31",
        dev_solver_time_limit_seconds=180.0,
        analysis_solver_time_limit_seconds=900.0,
        allow_full_year=True,
        solver="highs",
        cors_allow_origins=["http://localhost:8000"],
        summary_max_generation_techs=10,
        summary_max_generation_timesteps=240,
        summary_max_category_rows=25,
        summary_diagnostics_max_rows=100,
        run_retention_days=30,
        run_max_dirs=200,
        job_history_limit=200,
        job_dedupe_enabled=dedupe_enabled,
        job_queue_capacity=queue_capacity,
        job_execution_max_attempts=1,
        development_engine="mario",
        mario_db_path="",
        mario_timeout_seconds=120.0,
        mario_fail_on_error=False,
        model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
        dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
        platform_store_backend="sqlite",
        platform_sqlite_path=base / "platform" / "platform.sqlite3",
    )


def _wait_terminal(manager: JobManager, execution_id: str, timeout_seconds: float = 8.0):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        info = manager.get(execution_id)
        if str(info.status).lower() in {"succeeded", "failed", "cancelled"}:
            return info
        time.sleep(0.02)
    raise AssertionError(f"Job did not reach terminal status: {execution_id}")


def _summary_template(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "energy_scenario_key": "new_links",
        "mrio_scenario_id": "S2",
        "target_year": 2030,
        "run_profile": "dev",
        "system_cost": {"records": [{"costs": "monetary", "value": 100.0}]},
        "summary_diagnostics": {
            "physical_emissions": {"total_emissions": 50.0},
            "reliability": {"unserved_energy_share": 0.01, "unserved_total": 5.0},
        },
        "development_impacts": {
            "totals": {"jobs_total": 120.0, "gva_total_musd": 20.0},
            "inputs": {"investment_shock_total_musd": 10.0, "operating_shock_total_musd": 3.0},
            "by_region": {"records": [{"region": "A", "jobs_total": 120.0, "gva_total_musd": 20.0}]},
            "by_supplier_sector": {"records": [{"supplier_sector": "Import_goods", "shock_value_musd": 2.5}]},
        },
        "warnings": [],
    }


def _run_request() -> RunRequest:
    return RunRequest(
        energy_scenario_key="new_links",
        mrio_scenario_id="S2",
        target_year=2030,
        run_profile="dev",
        strict_validation=False,
        allow_placeholder_data=True,
        levers=LeverValues(),
    )


def _public_run_payload(run_name: str = "") -> dict:
    req = _run_request()
    return {
        "run_name": run_name,
        "model_architecture_id": req.model_architecture_id,
        "energy_model_engine": req.energy_model_engine,
        "scenario": {
            "energy_scenario_key": req.energy_scenario_key,
            "target_scenario_id": req.mrio_scenario_id,
            "target_year": req.target_year,
        },
        "run_profile": req.run_profile,
        "levers": req.levers.model_dump(mode="json"),
    }


class _FakeRuntime:
    def __init__(self, execute_fn):
        self.execute_fn = execute_fn

    def execute(self, execution_request, execution_context, *, progress_callback=None, cancel_requested=None):
        return self.execute_fn(execution_request, execution_context, progress_callback, cancel_requested)


def _successful_fake_runtime():
    def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
        if progress_callback:
            progress_callback("complete", 1.0, "Fake runtime completed")
        summary = _summary_template(execution_context.run_id)
        final_run_dir = execution_context.run_dir.parents[1] / execution_context.run_id
        registry = ArtifactRegistry(execution_context.run_id, final_run_dir, execution_request.artifact_policy)
        summary["artifact_catalog"] = []
        registry.write_json("summary_json", summary, dumps=lambda payload: json.dumps(payload, indent=2, sort_keys=True))
        summary["artifact_catalog"] = registry.exposed_descriptors()
        registry.write_json("summary_json", summary, dumps=lambda payload: json.dumps(payload, indent=2, sort_keys=True))
        return ModelExecutionResult(run_id=execution_context.run_id, summary=summary, warnings=[])

    return _FakeRuntime(_fake_run)


def _scenario_package() -> dict:
    return {
        "schema_version": "integrated_scenario_package_v1",
        "energy_scenario_key": "new_links",
        "mrio_scenario_id": "S2",
        "target_year": 2030,
        "run_profile": "dev",
        "levers": {},
        "energy": {"adapter": "calliope_v1", "model": "calliope", "scenario_key": "new_links"},
        "mrio_direct": {
            "adapter": "mrio_direct_heuristic",
            "scenario": {
                "scenario_id": "ZA-S2",
                "geography_code": "ZA",
                "geography": {"name": "South Africa", "type": "Country"},
                "summary": {
                    "fossil_delta_2030_numeric": -0.15,
                    "renewable_share_2030_numeric": 0.22,
                },
                "shock_categories": {
                    "A/Z": [
                        {"parameter": "Coal A-matrix coefficient", "target_2030": "Reduce by ~15% by 2030", "target_2050": "Reduce to 0 by 2050"},
                        {"parameter": "Renewable electricity share", "target_2030": "22% by 2030", "target_2050": "100% by 2050"},
                    ],
                    "E": [{"parameter": "CO2 emission intensity", "target_2030": "Reduce by ~15%", "target_2050": "0"}],
                    "Y": [{"parameter": "Solar/Wind addition", "target_2030": "+10%", "target_2050": "+full capacity"}],
                },
            },
            "report_source": {"source_file": "inputs/mario_inputs/scenario_report_scenarios.csv", "source_sha256": "test"},
        },
        "geography_alignment": {"status": "aligned", "calliope_locations": ["ZAF"]},
        "provenance": {"source": "test"},
    }


def _seed_mario_inputs(config_dir: Path) -> Path:
    mario_dir = config_dir / "mario_inputs"
    mario_dir.mkdir(parents=True, exist_ok=True)
    (mario_dir / "calliope_tech_to_mario_sector.csv").write_text(
        "calliope_tech,mario_sector,shock_channel\nCCGT_pp,Gas_supply_chain,opex\nPV_New,Electrical_equipment,capex\n",
        encoding="utf-8",
    )
    (mario_dir / "capex_sector_split.csv").write_text(
        "calliope_tech,mario_region,mario_sector,share\nPV_New,East_Africa,Electrical_equipment,1.0\n",
        encoding="utf-8",
    )
    (mario_dir / "opex_sector_split.csv").write_text(
        "calliope_tech,mario_region,mario_sector,opex_type,share\nCCGT_pp,East_Africa,Gas_supply_chain,fuel,1.0\nCCGT_pp,East_Africa,Maintenance_services,om,1.0\n",
        encoding="utf-8",
    )
    (mario_dir / "calliope_cost_to_mario_account.csv").write_text(
        "calliope_cost_class,calliope_component,mario_account,mario_flow_type\nmonetary,energy_cap,Intermediate_demand,investment\n",
        encoding="utf-8",
    )
    (mario_dir / "country_to_pool.csv").write_text(
        "calliope_location,power_pool,mario_region\nEGY,EAPP,East_Africa\n",
        encoding="utf-8",
    )
    (mario_dir / "employment_intensity.csv").write_text(
        "mario_region,mario_sector,jobs_per_musd_direct,jobs_per_musd_total\nEast_Africa,Gas_supply_chain,4.0,7.0\nEast_Africa,Maintenance_services,6.0,11.0\nEast_Africa,Electrical_equipment,5.0,9.0\n",
        encoding="utf-8",
    )
    (mario_dir / "value_added_intensity.csv").write_text(
        "mario_region,mario_sector,gva_per_musd_output,household_income_per_musd_output\nEast_Africa,Gas_supply_chain,0.35,0.15\nEast_Africa,Maintenance_services,0.4,0.22\nEast_Africa,Electrical_equipment,0.3,0.12\n",
        encoding="utf-8",
    )
    (mario_dir / "scenario_assumptions.csv").write_text(
        "assumption_key,scenario_key,value,unit,effective_year,source,notes\ncarbon_price,baseline,25,usd_per_tco2,2019,expert,calibrated baseline carbon proxy\n",
        encoding="utf-8",
    )
    (mario_dir / "scenario_report_scenarios.csv").write_text(
        "scenario_id,geography_code,geography_name,geography_type,scenario_code,scenario_type,label,description,target_years,renewable_share_2030,renewable_share_2030_numeric,renewable_share_2050,renewable_share_2050_numeric,fossil_delta_2030,fossil_delta_2030_numeric,net_zero_year,net_zero_year_numeric,shock_type,shock_category,parameter,target_2030,target_2050,implementation_notes,policy_sources,source_dataset\n"
        "ZA-S2,ZA,South Africa,Country,S2,policy_target,National Policy Target,Test structured scenario,2030|2050,22%,0.22,100%,1.0,-15%,-0.15,2050,2050,Policy,A/Z,Coal A-matrix coefficient,Reduce by 15%,Reduce to 0,,,inputs/mario_inputs/scenario_report_scenarios.csv\n",
        encoding="utf-8",
    )
    generated_dir = config_dir / "generated"
    generated_dir.mkdir(parents=True, exist_ok=True)
    (generated_dir / "scenario_report_scenarios.json").write_text(
        json.dumps(
            {
                "schema_version": "scenario_report_v1",
                "source_file": "inputs/mario_inputs/scenario_report_scenarios.csv",
                "source_sha256": "test",
                "scenario_ids": ["ZA-S2"],
                "scenarios": {
                    "ZA-S2": {
                        "scenario_id": "ZA-S2",
                        "geography_code": "ZA",
                        "geography": {"name": "South Africa", "type": "Country"},
                        "scenario_code": "S2",
                        "scenario_type": "policy_target",
                        "label": "National Policy Target",
                        "description": "Test structured scenario",
                        "target_years": [2030, 2050],
                        "summary": {
                            "scenario_id": "ZA-S2",
                            "renewable_share_2030_numeric": 0.22,
                            "fossil_delta_2030_numeric": -0.15,
                        },
                        "targets": [],
                        "shock_categories": {"A/Z": [], "E": [], "Y": []},
                    }
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (mario_dir / "development_indicator_mapping.csv").write_text(
        "indicator_id,indicator_name,driver_metric,aggregation_rule,unit,lag_years,notes\njobs_total,Total employment impact,jobs_per_musd_total,sum,jobs,0,from MARIO + intensity tables\ngva_total,Gross value added impact,gva_per_musd_output,sum,musd_2019,0,from MARIO output\nco2_cost_burden,Carbon cost burden,co2_cost_proxy,sum,musd_2019,0,proxy based on physical emissions and carbon price\n",
        encoding="utf-8",
    )
    (mario_dir / "exchange_output_schema.csv").write_text(
        "file_name,column_name,required,dtype,description\nenergy_service_balance.csv,run_id,yes,string,x\n",
        encoding="utf-8",
    )
    return mario_dir


class SchemaTests(unittest.TestCase):
    def test_run_request_profile_normalization(self):
        req = RunRequest(
            energy_scenario_key="new_links",
            mrio_scenario_id="S2",
            target_year=2030,
            run_profile="analysis",
            strict_validation=False,
            allow_placeholder_data=True,
        )
        self.assertEqual(req.run_profile, "analysis")
        self.assertEqual(req.energy_scenario_key, "new_links")
        self.assertEqual(req.mrio_scenario_id, "S2")
        self.assertTrue(req.strict_validation)

    def test_run_request_rejects_legacy_scenario_shape(self):
        with self.assertRaises(Exception):
            RunRequest(
                scenario="new_links",
                fast_dev_mode=True,
                energy_scenario_key="new_links",
                mrio_scenario_id="S2",
                target_year=2030,
                run_profile="dev",
                strict_validation=False,
                allow_placeholder_data=True,
            )


class ScenarioReportTests(unittest.TestCase):
    def test_root_report_parser_recovers_expected_scenarios(self):
        repo_root = Path(__file__).resolve().parents[2]
        report = load_or_parse_scenario_report(repo_root / "inputs")
        expected = {"ZA-S1", "ZA-S2", "IN-S1", "IN-S2", "BR-S1", "BR-S2", "WF-S1", "WF-S2", "WM-S1", "WM-S2"}
        self.assertEqual(set(report["scenario_ids"]), expected)
        za = report["scenarios"]["ZA-S2"]
        self.assertEqual(za["geography_code"], "ZA")
        self.assertTrue(za["shock_categories"]["A/Z"])
        self.assertIn(2030, za["target_years"])

    def test_scenario_package_and_direct_inputs_are_complete(self):
        repo_root = Path(__file__).resolve().parents[2]
        package = build_scenario_package(
            config_dir=repo_root / "inputs",
            calliope_root=_repo_calliope_root(repo_root),
            energy_scenario_key="new_links",
            mrio_scenario_id="S2",
            target_year=2030,
            run_profile="dev",
            levers={},
            strict_validation=False,
            allow_placeholder_data=True,
        )
        self.assertEqual(package["mrio_scenario_id"], "S2")
        self.assertEqual(package["target_scenario"]["scenario_id"], "S2")
        self.assertEqual(package["mrio_direct"]["shock_mapping"]["mapping_id"], "mrio_direct_heuristic_v1")
        self.assertEqual(package["mrio_direct"]["scenario"]["scenario_id"], "S2")
        self.assertGreater(package["mrio_direct"]["scenario"]["national_record_count"], 1)
        self.assertEqual(package["geography_alignment"]["alignment_level"], "africa_national_placeholder_to_calliope_locations")
        self.assertEqual(package["geography_alignment"]["status"], "aligned")
        direct = build_mrio_direct_inputs(
            scenario_package=package,
            bridge_total_shock_musd=100.0,
            direct_config={"structural_reallocation_bridge_scale": 0.25, "max_direct_to_bridge_ratio": 1.0},
        )
        self.assertEqual(direct["method"], "mrio_direct_heuristic_v1")
        self.assertEqual(direct["scenario_id"], "S2")
        self.assertTrue(direct["diagnostics"]["national_placeholder_dataset"])
        self.assertGreater(len(direct["shock_rows"]), 0)
        direct_regions = {str(row.get("mario_region", "")) for row in direct["shock_rows"]}
        source_ids = {str(row.get("source_report_scenario_id", "")) for row in direct["shock_rows"]}
        self.assertIn("ZAF", direct_regions)
        self.assertTrue(any(region and region != "ZAF" for region in direct_regions))
        self.assertIn("ZA-S2", source_ids)
        self.assertIn("WF-S2", source_ids)
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = write_scenario_artifacts(Path(tmp), package, direct)
            self.assertTrue((Path(tmp) / "scenario_package.json").exists())
            self.assertTrue((Path(tmp) / "scenario" / "mrio_direct_inputs.json").exists())
            self.assertTrue((Path(tmp) / "scenario" / "mrio_direct_shocks.csv").exists())
            self.assertIn("scenario_package_json", artifacts)

    def test_integrated_catalog_splits_targets_from_mrio_shock_mapping(self):
        repo_root = Path(__file__).resolve().parents[2]
        catalog = build_integrated_scenario_catalog(
            repo_root / "inputs",
            _repo_calliope_root(repo_root),
            energy_scenarios=[{"key": "new_links", "title": "New links"}],
        )
        self.assertEqual(catalog["schema_version"], "integrated_scenario_catalog_v1")
        self.assertEqual(catalog["defaults"]["mrio_scenario_id"], "S2")
        self.assertEqual(catalog["defaults"]["target_scenario_id"], "S2")
        self.assertEqual(catalog["defaults"]["mrio_shock_mapping_id"], "mrio_direct_heuristic_v1")
        self.assertGreater(catalog["report"]["africa_national_country_count"], 50)
        self.assertIn("target_scenarios", catalog)
        self.assertIn("mrio_shock_mappings", catalog)
        self.assertEqual([row["scenario_id"] for row in catalog["target_scenarios"]], ["S1", "S2"])
        self.assertIn("target_profiles", catalog["target_scenarios"][1])
        self.assertEqual(catalog["mrio_shock_mappings"][0]["mapping_id"], "mrio_direct_heuristic_v1")
        self.assertTrue((repo_root / "inputs" / "generated" / "africa_national_mrio_placeholder_scenarios.json").exists())

    def test_geography_alignment_fans_out_national_and_regional_scenarios(self):
        repo_root = Path(__file__).resolve().parents[2]
        report = load_or_parse_scenario_report(repo_root / "inputs")
        za_alignment = build_geography_alignment(
            config_dir=repo_root / "inputs",
            calliope_root=_repo_calliope_root(repo_root),
            mrio_scenario=report["scenarios"]["ZA-S2"],
        )
        wf_alignment = build_geography_alignment(
            config_dir=repo_root / "inputs",
            calliope_root=_repo_calliope_root(repo_root),
            mrio_scenario=report["scenarios"]["WF-S2"],
        )
        br_alignment = build_geography_alignment(
            config_dir=repo_root / "inputs",
            calliope_root=_repo_calliope_root(repo_root),
            mrio_scenario=report["scenarios"]["BR-S2"],
        )
        self.assertIn("ZAF", za_alignment["calliope_locations"])
        self.assertGreater(wf_alignment["calliope_location_count"], 1)
        self.assertFalse(wf_alignment["blocking_mismatch"])
        self.assertEqual(br_alignment["status"], "mrio_only")

    def test_lever_mappings_csv_loader(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            (config_dir / "lever_mappings.csv").write_text(
                "\n".join(
                    [
                        "key,value",
                        "renewables_tech,PV*",
                        "renewables_tech,Wind*",
                        "fossil_tech,CCGT*",
                        "capex_key_path,costs.monetary.energy_cap",
                        "fuel_cost_key_path,costs.monetary.om_con",
                        "carbon_price_path,run.objective_options.cost_class.co2",
                    ]
                ),
                encoding="utf-8",
            )
            mappings = load_lever_mappings(config_dir)
            self.assertIn("PV*", mappings.renewables_techs)
            self.assertIn("CCGT*", mappings.fossil_techs)
            self.assertEqual(mappings.capex_key_path, ["costs", "monetary", "energy_cap"])
            self.assertEqual(mappings.fuel_cost_key_path, ["costs", "monetary", "om_con"])
            self.assertEqual(
                mappings.carbon_price_path,
                ["run", "objective_options", "cost_class", "co2"],
            )

    def test_development_model_csv_loader(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            (config_dir / "development_model.csv").write_text(
                "\n".join(
                    [
                        "parameter,value",
                        "mario.uncertainty_relative_bounds.jobs_total,0.18",
                        "mario_direct.max_direct_to_bridge_ratio,0.75",
                    ]
                ),
                encoding="utf-8",
            )
            cfg = _load_development_model_config(config_dir)
            self.assertAlmostEqual(cfg["mario"]["uncertainty_relative_bounds"]["jobs_total"], 0.18)
            self.assertAlmostEqual(cfg["mario_direct"]["max_direct_to_bridge_ratio"], 0.75)

    def test_results_health_handles_missing_results(self):
        class _NoResults:
            results = None

        health = _results_health(_NoResults())
        self.assertEqual(health["var_count"], 0)
        self.assertEqual(health["termination_condition"], "")

    def test_results_health_reads_attrs_and_data_vars(self):
        class _Results:
            attrs = {"termination_condition": "optimal", "solution_time": 12.5}
            data_vars = {"carrier_prod": object(), "cost": object()}

        class _Model:
            results = _Results()

        health = _results_health(_Model())
        self.assertEqual(health["var_count"], 2)
        self.assertEqual(health["termination_condition"], "optimal")


class IntegratedResultsTests(unittest.TestCase):
    def test_build_integrated_results_contains_expected_fields(self):
        payload = build_integrated_results(_summary_template("run1"))
        self.assertEqual(payload["run_id"], "run1")
        self.assertTrue(payload["integrated_overview"]["metrics"])
        self.assertIn("development_drivers", payload)
        self.assertIn("development_uncertainty", payload)
        self.assertIn("scenario_provenance", payload)
        self.assertIn("source_channels", payload)

    def test_build_integrated_results_uses_selected_totals_and_exposes_channels(self):
        summary = _summary_template("run1")
        summary["development_impacts"]["bridge"] = {"totals": {"jobs_total": 120.0, "gva_total_musd": 20.0}}
        summary["development_impacts"]["mrio_direct"] = {
            "method": "mrio_direct_heuristic",
            "totals": {"jobs_total": 30.0, "gva_total_musd": 5.0},
        }
        summary["development_impacts"]["selected_totals"] = {"jobs_total": 120.0, "gva_total_musd": 20.0}
        summary["development_impacts"]["combined_totals"] = {"jobs_total": 150.0, "gva_total_musd": 25.0}
        summary["development_impacts"]["overlap_diagnostics"] = {
            "overlap_exists": True,
            "selected_totals_source": "bridge",
            "merge_logic": "bridge_authoritative_overlap_handling",
        }
        payload = build_integrated_results(
            summary,
            coupling_manifest={
                "mrio_direct_heuristic": True,
                "selected_totals_source": "bridge",
                "overlap_policy": "bridge_authoritative_for_headline_totals",
            },
        )
        self.assertEqual(payload["source_channels"]["selected_totals"]["jobs_total"], 120.0)
        self.assertEqual(payload["source_channels"]["combined_totals"]["jobs_total"], 150.0)
        self.assertEqual(payload["development_confidence"]["selected_totals_source"], "bridge")
        self.assertIn(payload["model_quality"]["status"], {"analyst_review", "exploratory_only"})

    def test_validate_integrated_results_rejects_missing_metric(self):
        payload = build_integrated_results(_summary_template("run1"))
        payload["integrated_overview"]["metrics"] = [
            row for row in payload["integrated_overview"]["metrics"] if row.get("key") != "jobs_total"
        ]
        with self.assertRaises(ValueError):
            validate_integrated_results(payload)

    def test_build_integrated_results_loads_assumptions_and_indicators(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            _seed_mario_inputs(config_dir)
            payload = build_integrated_results(
                _summary_template("run1"),
                config_dir=config_dir,
                lever_values={"carbon_price_usd_per_tco2": 10.0},
            )
            self.assertTrue(payload["scenario_assumptions"]["records"])
            self.assertGreaterEqual(payload["development_indicators"]["available_count"], 1)

    def test_build_integrated_results_exposes_model_quality_and_resolution(self):
        summary = _summary_template("run1")
        summary["summary_diagnostics"]["physical_emissions"] = {
            "method": "cost_class_co2_direct",
            "factor_coverage_share": 0.25,
            "factor_method_gap_share": 0.0,
            "total_emissions": 50.0,
        }
        summary["summary_diagnostics"]["energy_balance"] = {
            "max_abs_balance_gap_share": 0.0,
        }
        payload = build_integrated_results(
            summary,
            coupling_manifest={
                "development_engine_mode": "mario",
                "mapping_coverage_share": 1.0,
                "unmapped_mapping_share": 0.0,
                "placeholder_input_row_count": 0,
                "allow_placeholder_data": False,
            },
        )
        self.assertEqual(payload["model_quality"]["status"], "production_ready")
        self.assertTrue(payload["metric_resolution"]["records"])

    def test_energy_only_quality_ignores_skipped_mrio_diagnostics(self):
        summary = _summary_template("run1")
        summary["model_architecture_id"] = "energy-only"
        summary["summary_diagnostics"]["physical_emissions"] = {
            "method": "cost_class_co2_direct",
            "factor_coverage_share": 1.0,
            "factor_method_gap_share": 0.0,
            "total_emissions": 50.0,
        }
        summary["summary_diagnostics"]["energy_balance"] = {
            "max_abs_balance_gap_share": 0.0,
        }
        payload = build_integrated_results(
            summary,
            coupling_manifest={
                "development_engine_mode": "skipped_energy_only",
                "integration_architecture": "energy_only",
                "mapping_coverage_share": 0.0,
                "placeholder_input_row_count": 12,
                "mrio_direct_heuristic": True,
            },
        )
        issue_codes = {row["code"] for row in payload["model_quality"]["issues"]}
        self.assertEqual(payload["development_confidence"]["coupling_mode"], "skipped_energy_only")
        self.assertEqual(payload["model_quality"]["status"], "production_ready")
        self.assertNotIn("placeholder_inputs", issue_codes)
        self.assertNotIn("mapping_coverage", issue_codes)
        self.assertNotIn("mrio_direct_heuristic", issue_codes)


class SummaryDiagnosticsTests(unittest.TestCase):
    class _FakeDA:
        def __init__(self, rows):
            self._rows = rows

        def to_dataframe(self, name="value"):
            return pd.DataFrame(self._rows)

    class _FakeResults(dict):
        def __init__(self, *args, **kwargs):
            attrs = kwargs.pop("attrs", None) or {}
            super().__init__(*args, **kwargs)
            self.attrs = attrs
            self.data_vars = self

    class _FakeModel:
        def __init__(self, results):
            self.results = results

    def test_build_summary_diagnostics_prefers_direct_co2_and_builds_balance(self):
        results = self._FakeResults(
            {
                "carrier_prod": self._FakeDA(
                    [
                        {"locs": "EGY", "techs": "CCGT_pp", "value": 100.0},
                        {"locs": "EGY", "techs": "PV1", "value": 50.0},
                    ]
                ),
                "carrier_con": self._FakeDA(
                    [
                        {"locs": "EGY", "techs": "Demand_power", "value": -150.0},
                    ]
                ),
                "unmet_demand": self._FakeDA(
                    [
                        {"locs": "EGY", "timesteps": "2019-01-01 00:00:00", "value": 0.0},
                    ]
                ),
                "cost": self._FakeDA(
                    [
                        {"locs": "EGY", "techs": "CCGT_pp", "costs": "co2", "value": 90.0},
                        {"locs": "EGY", "techs": "CCGT_pp", "costs": "monetary", "value": 10.0},
                    ]
                ),
                "energy_cap": self._FakeDA(
                    [
                        {"techs": "CCGT_pp", "value": 20.0},
                        {"techs": "PV1", "value": 30.0},
                    ]
                ),
            },
            attrs={
                "termination_condition": "optimal",
                "solution_time": 1.0,
                "objective_function_value": 2.0,
            },
        )
        model = self._FakeModel(results)
        diagnostics = build_summary_diagnostics(
            model=model,
            run_id="run1",
            scenario="new_links",
            calliope_root=None,
            tech_library={
                "techs": {
                    "CCGT_pp": {"costs": {"co2": {"om_prod": 0.9}}},
                    "PV1": {"costs": {"co2": {"om_prod": 0.0}}},
                }
            },
            max_rows=20,
            warnings=[],
        )
        self.assertEqual(diagnostics["physical_emissions"]["method"], "cost_class_co2_direct")
        self.assertAlmostEqual(diagnostics["physical_emissions"]["total_emissions"], 90.0)
        self.assertAlmostEqual(diagnostics["energy_balance"]["max_abs_balance_gap_share"], 0.0)
        self.assertGreater(diagnostics["system_structure"]["renewable_generation_share"], 0.0)


class MainAndArtifactTests(unittest.TestCase):
    def test_api_echoes_or_assigns_request_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            client = TestClient(create_app(settings=settings))
            generated = client.get("/health")
            self.assertEqual(generated.status_code, 200)
            self.assertTrue(generated.headers.get("X-Request-Id"))

            supplied = client.get("/health", headers={"X-Request-Id": "frontend-test-request"})
            self.assertEqual(supplied.headers.get("X-Request-Id"), "frontend-test-request")

    def test_read_summary_json_invalid_payload_returns_503(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "summary.json"
            path.write_text("{invalid-json}", encoding="utf-8")
            with self.assertRaises(HTTPException) as ctx:
                read_summary_json(path)
            self.assertEqual(ctx.exception.status_code, 503)

    def test_create_exchange_bundle_contains_core_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            registry = ArtifactRegistry("1234abcd", run_dir, {"artifacts": {"manifest": {}}})
            registry.path_for("energy_service_balance_csv").write_text("a,b\n1,2\n", encoding="utf-8")
            registry.register_existing("energy_service_balance_csv")
            registry.path_for("results_csv").write_text("variable,value\ncost,1\n", encoding="utf-8")
            registry.register_existing("results_csv")
            registry.path_for("development_impacts_json").write_text("{}", encoding="utf-8")
            registry.register_existing("development_impacts_json")
            registry.path_for("coupling_manifest_json").write_text("{}", encoding="utf-8")
            registry.register_existing("coupling_manifest_json")
            registry.path_for("integrated_results_json").write_text("{}", encoding="utf-8")
            registry.register_existing("integrated_results_json")

            zip_path = create_exchange_bundle_zip(run_dir, artifact_registry=registry)
            self.assertTrue(zip_path and zip_path.exists())
            with ZipFile(zip_path, "r") as zf:
                names = set(zf.namelist())
            self.assertIn("artifacts/intermediate/exchange/energy_service_balance.csv", names)
            self.assertIn("artifacts/final/results.csv", names)
            self.assertIn("artifacts/final/development_impacts.json", names)
            self.assertIn("artifacts/final/coupling_manifest.json", names)
            self.assertIn("artifacts/final/integrated_results.json", names)

    def test_artifact_manifest_and_pruning(self):
        manifest = load_artifact_manifest(
            {
                "artifacts": {
                    "manifest": {
                        "results_csv": {
                            "retain_on_success": False,
                            "drop_after_consumed_by": "development"
                        }
                    }
                }
            }
        )
        self.assertIn("results_csv", manifest)
        self.assertFalse(manifest["results_csv"].retain_on_success)
        self.assertEqual(manifest["results_csv"].drop_after_consumed_by, "development")

        with tempfile.TemporaryDirectory() as tmp:
            registry = ArtifactRegistry("abcd1234", Path(tmp), {"artifacts": {"manifest": {"results_csv": {"drop_after_consumed_by": "development"}}}})
            registry.write_text("results_csv", "x,y\n1,2\n")
            self.assertTrue(registry.path_for("results_csv").exists())
            registry.prune_consumed_by("development")
            self.assertFalse(registry.path_for("results_csv").exists())

    def test_model_runtime_manifest_and_bundle_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                config_dir=repo_root / "inputs",
                calliope_root=_repo_calliope_root(repo_root),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            manifest = load_model_runtime_manifest(settings.model_manifest_path)
            queue_message = ExecutionQueueMessage(
                execution_id="job123",
                run_id="run123",
                project_id="default",
                user_id="undp_analyst",
                request_payload=_run_request().model_dump(mode="json"),
                created_at="2026-01-01T00:00:00+00:00",
            ).to_dict()
            bundle = build_model_run_bundle(
                settings=settings,
                request=_run_request(),
                execution_id="job123",
                run_id="run123",
                manifest=manifest,
                dataset_manifest={"schema_version": "model_dataset_manifest_v1", "datasets": []},
                queue_message=queue_message,
            )
            self.assertEqual(bundle["schema_version"], "model_run_bundle_v1")
            self.assertEqual(bundle["model_runtime"]["model_id"], "edim-calliope-mrio")
            self.assertEqual(bundle["queue_message"]["schema_version"], "execution_queue_message")
            self.assertEqual(bundle["queue_message"]["execution_id"], "job123")
            self.assertIn("artifact_policy", bundle)
            self.assertEqual(bundle["artifact_handoff"]["schema_version"], "runtime_artifact_handoff_v1")
            self.assertEqual(bundle["artifact_handoff"]["mode"], "shared_filesystem")
            self.assertEqual(bundle["execution"]["artifact_handoff_mode"], "shared_filesystem")
            self.assertEqual(bundle["provenance"]["schema_version"], "edim_run_provenance")
            self.assertEqual(bundle["provenance"]["model_runtime"]["model_id"], "edim-calliope-mrio")
            self.assertTrue(bundle["provenance"]["model_runtime"]["manifest_sha256"])
            self.assertTrue(bundle["provenance"]["artifact_policy"]["sha256"])

    def test_execution_queue_message_roundtrip_and_local_queue(self):
        message = ExecutionQueueMessage(
            execution_id="exec123",
            run_id="run123",
            project_id="project123",
            user_id="undp_analyst",
            request_payload=_run_request().model_dump(mode="json"),
            attempt=2,
            created_at="2026-01-01T00:00:00+00:00",
            retry_policy={"schema_version": "execution_retry_policy", "max_attempts": 3},
        )
        payload = message.to_dict()
        restored = ExecutionQueueMessage.from_dict(payload)
        self.assertEqual(restored.execution_id, "exec123")
        self.assertEqual(restored.attempt, 2)
        self.assertEqual(restored.retry_policy["schema_version"], "execution_retry_policy")

        attempt = ExecutionAttemptRecord(
            execution_id="exec123",
            run_id="run123",
            attempt=2,
            worker_id="local-thread:1:test",
            status="running",
            started_at="2026-01-01T00:00:00+00:00",
            heartbeat_at="2026-01-01T00:00:01+00:00",
            message="Running test attempt.",
        )
        restored_attempt = ExecutionAttemptRecord.from_dict(attempt.to_dict())
        self.assertEqual(restored_attempt.worker_id, "local-thread:1:test")
        self.assertEqual(restored_attempt.schema_version, "execution_attempt")

        queue = LocalExecutionQueue()
        queue.put(payload)
        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(queue.get()["schema_version"], "execution_queue_message")
        queue.task_done()

    def test_local_artifact_storage_publishes_shared_filesystem_handoff_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            run_id = "run123"
            run_dir = settings.runs_dir / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            service = LocalArtifactStorageService(settings)
            publication = service.publish_run_artifacts(
                run_id=run_id,
                run_dir=run_dir,
                artifact_catalog=[{"artifact_id": "summary_json", "expose_download": True}],
                handoff_mode="shared_filesystem",
            )
            self.assertEqual(publication["schema_version"], "runtime_artifact_publication_v1")
            self.assertEqual(publication["handoff_mode"], "shared_filesystem")
            self.assertTrue(publication["published"])
            self.assertEqual(publication["downloadable_artifact_count"], 1)

    def test_model_runtime_preflight_cli_uses_single_runtime_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                config_dir=repo_root / "inputs",
                calliope_root=_repo_calliope_root(repo_root),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            manifest = load_model_runtime_manifest(settings.model_manifest_path)
            run_id = "a" * 32
            execution_id = "b" * 32
            bundle = build_model_run_bundle(
                settings=settings,
                request=_run_request(),
                execution_id=execution_id,
                run_id=run_id,
                manifest=manifest,
                dataset_manifest={"schema_version": "model_dataset_manifest_v1", "datasets": []},
            )
            bundle_path = Path(tmp) / "request_bundle.json"
            bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

            env = os.environ.copy()
            env["PYTHONPATH"] = str(repo_root / "model_runtime") + os.pathsep + env.get("PYTHONPATH", "")
            proc = subprocess.run(
                [sys.executable, "-m", "edim_model.cli", "preflight", "--bundle", str(bundle_path)],
                cwd=repo_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            events = [json.loads(line) for line in proc.stdout.splitlines() if line.strip().startswith("{")]
            result = next(row for row in events if row.get("type") == "result")
            self.assertEqual(result["stage"], "preflight")
            self.assertEqual(result["payload"]["status"], "passed")
            self.assertIn("checks", result["payload"])

    def test_model_runtime_catalog_cli_returns_scenarios_and_architectures(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            env = os.environ.copy()
            env["PYTHONPATH"] = str(repo_root / "model_runtime") + os.pathsep + env.get("PYTHONPATH", "")
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "edim_model.cli",
                    "catalog",
                    "--config-dir",
                    str(repo_root / "inputs"),
                    "--calliope-root",
                    str(_repo_calliope_root(repo_root)),
                    "--manifest",
                    str(repo_root / "model_runtime" / "edim_model" / "model_manifest.json"),
                    "--architecture-catalog",
                    str(repo_root / "model_runtime" / "edim_model" / "architecture_catalog.json"),
                ],
                cwd=repo_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["schema_version"], "edim_model_catalog")
            self.assertEqual(payload["scenario_catalog"]["schema_version"], "model_scenario_catalog")
            self.assertTrue(payload["architecture_catalog"]["architectures"])

    def test_runs_router_lists_artifacts_from_summary_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            run_id = "abcd1234"
            run_dir = settings.runs_dir / run_id
            registry = ArtifactRegistry(run_id, run_dir, settings.runtime_config)
            registry.write_text("results_csv", "variable,value\ncost,1\n")
            summary = {"run_id": run_id, "artifact_catalog": registry.exposed_descriptors(), "warnings": []}
            registry.write_json("summary_json", summary, dumps=lambda payload: json.dumps(payload))
            create_platform_repository(settings).create_run_record(
                project_id="default",
                run_id=run_id,
                request_payload=_run_request().model_dump(mode="json"),
                status="succeeded",
                dataset_snapshot={},
                user_id="undp_analyst",
            )

            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get(f"/api/runs/{run_id}/artifacts")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["run_id"], run_id)
            self.assertEqual(payload["artifacts"][0]["artifact_id"], "results_csv")
            hidden = client.get(f"/api/runs/{run_id}/artifacts", headers={"X-EDIM-User-Id": "country_officer"})
            self.assertEqual(hidden.status_code, 404)

    def test_runs_router_downloads_artifact_by_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            run_id = "abcd1234"
            run_dir = settings.runs_dir / run_id
            registry = ArtifactRegistry(run_id, run_dir, settings.runtime_config)
            registry.write_text("report_markdown", "# Report\n")
            summary = {"run_id": run_id, "artifact_catalog": registry.exposed_descriptors(), "warnings": []}
            registry.write_json("summary_json", summary, dumps=lambda payload: json.dumps(payload))
            create_platform_repository(settings).create_run_record(
                project_id="default",
                run_id=run_id,
                request_payload=_run_request().model_dump(mode="json"),
                status="succeeded",
                dataset_snapshot={},
                user_id="undp_analyst",
            )

            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get(f"/api/runs/{run_id}/artifacts/report_markdown")
            self.assertEqual(response.status_code, 200)
            self.assertIn("# Report", response.text)
            hidden = client.get(f"/api/runs/{run_id}/artifacts/report_markdown", headers={"X-EDIM-User-Id": "country_officer"})
            self.assertEqual(hidden.status_code, 404)

    def test_scenarios_router_returns_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                config_dir=repo_root / "inputs",
                calliope_root=_repo_calliope_root(repo_root),
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get("/api/scenarios")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["schema_version"], "model_scenario_catalog")
            self.assertIn("module_configurations", payload)
            self.assertIn("scenario_channels", payload)
            self.assertNotIn("energy_scenarios", payload)
            self.assertNotIn("target_scenarios", payload)
            self.assertNotIn("mrio_shock_mappings", payload)
            self.assertTrue(any(row["module_id"] == "calliope" for row in payload["module_configurations"]))
            self.assertTrue(any(row["config_key"] == "scenario.energy_scenario_key" for row in payload["scenario_channels"]))

    def test_input_datasets_router_returns_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            (settings.config_dir / "scenario_metadata.csv").write_text("key,title\nnew_links,New links\n", encoding="utf-8")
            (settings.config_dir / "scenario_geography_mapping.csv").write_text("mrio_geography_code,calliope_country_code,calliope_location,alignment_level,notes\nZA,ZA,ZA,national,\n", encoding="utf-8")
            (settings.config_dir / "development_model.csv").write_text("indicator,driver,coefficient\njobs,investment,1\n", encoding="utf-8")
            _seed_mario_inputs(settings.config_dir)
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get("/api/input-datasets")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertIn("datasets", payload)
            self.assertTrue(any(row["id"] == "calliope_model" for row in payload["datasets"]))

    def test_input_dataset_upload_creates_versioned_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.post(
                "/api/input-datasets/lever_mappings/upload",
                files={"file": ("lever_mappings.csv", b"key,value\nx,y\n", "text/csv")},
            )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertTrue(payload["version_id"])
            self.assertTrue(Path(payload["path"]).exists())
            self.assertEqual(payload["scope"], "user_override")
            self.assertTrue((settings.runs_dir.parent / "dataset_uploads" / "users" / "undp_analyst" / "active_versions.json").exists())

            catalog = client.get("/api/input-datasets").json()["datasets"]
            lever_row = next(row for row in catalog if row["id"] == "lever_mappings")
            self.assertTrue(lever_row["versioned_override"])

    def test_input_dataset_library_supports_create_rename_and_project_assignment(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)
            analyst_headers = {"X-EDIM-User-Id": "undp_analyst"}

            created = client.post(
                "/api/input-datasets",
                headers=analyst_headers,
                json={
                    "label": "National demand outlook",
                    "layer": "demand",
                    "role": "Scenario demand projection",
                    "scope": "user",
                    "upload_policy": "project_override",
                },
            )
            self.assertEqual(created.status_code, 200)
            dataset = created.json()["dataset"]
            self.assertEqual(dataset["id"], "national_demand_outlook")
            self.assertEqual(dataset["scope"], "user")
            self.assertFalse(dataset["exists"])

            upload = client.post(
                f"/api/input-datasets/{dataset['id']}/upload",
                headers=analyst_headers,
                files={"file": ("demand.csv", b"year,value\n2030,12\n", "text/csv")},
            )
            self.assertEqual(upload.status_code, 200)
            version_id = upload.json()["version_id"]

            renamed = client.patch(
                f"/api/input-datasets/{dataset['id']}",
                headers=analyst_headers,
                json={"label": "National electricity demand outlook"},
            )
            self.assertEqual(renamed.status_code, 200)
            self.assertEqual(renamed.json()["dataset"]["label"], "National electricity demand outlook")

            project = client.post(
                "/api/projects",
                headers=analyst_headers,
                json={"title": "Demand planning"},
            ).json()["project"]
            attached = client.post(
                f"/api/projects/{project['project_id']}/datasets",
                headers=analyst_headers,
                json={"dataset_id": dataset["id"], "version_id": version_id},
            )
            self.assertEqual(attached.status_code, 200)
            self.assertEqual(attached.json()["project_ids"], [project["project_id"]])

            versions = client.get(
                f"/api/input-datasets/{dataset['id']}/versions",
                headers=analyst_headers,
            ).json()["versions"]
            self.assertEqual(versions[0]["project_ids"], [project["project_id"]])
            catalog_row = next(
                row
                for row in client.get("/api/input-datasets", headers=analyst_headers).json()["datasets"]
                if row["id"] == dataset["id"]
            )
            self.assertEqual(catalog_row["project_ids"], [project["project_id"]])

            country_catalog = client.get(
                "/api/input-datasets",
                headers={"X-EDIM-User-Id": "country_officer"},
            ).json()["datasets"]
            self.assertFalse(any(row["id"] == dataset["id"] for row in country_catalog))
            hidden = client.post(
                f"/api/projects/{project['project_id']}/datasets",
                headers={"X-EDIM-User-Id": "country_officer"},
                json={"dataset_id": dataset["id"], "version_id": version_id},
            )
            self.assertEqual(hidden.status_code, 404)

    def test_input_dataset_upload_rejects_missing_required_csv_headers(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.post(
                "/api/input-datasets/lever_mappings/upload",
                files={"file": ("lever_mappings.csv", b"not_key,value\nx,y\n", "text/csv")},
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("missing required columns", response.json()["detail"])

    def test_platform_project_api_is_user_owned_with_admin_visibility(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            session = client.get("/api/session", headers={"X-EDIM-User-Id": "country_officer"}).json()
            self.assertEqual(session["auth_mode"], "test_user_header")
            self.assertEqual(session["user"]["user_id"], "country_officer")
            self.assertTrue(any(row["user_id"] == "admin" for row in session["available_users"]))
            unknown = client.get("/api/session", headers={"X-EDIM-User-Id": "unknown_user"})
            self.assertEqual(unknown.status_code, 401)
            created = client.post(
                "/api/projects",
                headers={"X-EDIM-User-Id": "undp_analyst"},
                json={"title": "National energy transition", "geography": "ZA"},
            )
            self.assertEqual(created.status_code, 200)
            project_id = created.json()["project"]["project_id"]
            analyst_projects = client.get("/api/projects", headers={"X-EDIM-User-Id": "undp_analyst"}).json()["projects"]
            country_projects = client.get("/api/projects", headers={"X-EDIM-User-Id": "country_officer"}).json()["projects"]
            admin_projects = client.get("/api/projects", headers={"X-EDIM-User-Id": "admin"}).json()["projects"]
            self.assertEqual(analyst_projects[0]["project_id"], project_id)
            self.assertEqual(country_projects, [])
            self.assertTrue(any(row["project_id"] == project_id for row in admin_projects))
            renamed = client.patch(
                f"/api/projects/{project_id}",
                headers={"X-EDIM-User-Id": "undp_analyst"},
                json={"title": "Renamed project"},
            )
            self.assertEqual(renamed.status_code, 200)
            self.assertEqual(renamed.json()["project"]["title"], "Renamed project")
            archived = client.patch(
                f"/api/projects/{project_id}",
                headers={"X-EDIM-User-Id": "undp_analyst"},
                json={"status": "archived"},
            )
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(archived.json()["project"]["status"], "archived")
            restored = client.patch(
                f"/api/projects/{project_id}",
                headers={"X-EDIM-User-Id": "undp_analyst"},
                json={"status": "active"},
            )
            self.assertEqual(restored.status_code, 200)
            deleted = client.delete(f"/api/projects/{project_id}", headers={"X-EDIM-User-Id": "undp_analyst"})
            self.assertEqual(deleted.status_code, 200)
            self.assertEqual(client.get(f"/api/projects/{project_id}", headers={"X-EDIM-User-Id": "undp_analyst"}).status_code, 404)

    def test_input_dataset_versions_can_be_filtered_activated_and_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            first = client.post(
                "/api/input-datasets/lever_mappings/upload",
                files={"file": ("lever_mappings.csv", b"key,value\nx,y\n", "text/csv")},
            ).json()
            second = client.post(
                "/api/input-datasets/lever_mappings/upload",
                files={"file": ("lever_mappings.csv", b"key,value\na,b\n", "text/csv")},
            ).json()

            filtered = client.get("/api/input-datasets?layer=adapter&input_property=lever").json()["datasets"]
            self.assertEqual([row["id"] for row in filtered], ["lever_mappings"])

            versions = client.get("/api/input-datasets/lever_mappings/versions").json()["versions"]
            self.assertGreaterEqual(len(versions), 2)
            activated = client.post(f"/api/input-datasets/lever_mappings/versions/{first['version_id']}/activate")
            self.assertEqual(activated.status_code, 200)
            active_row = next(row for row in client.get("/api/input-datasets").json()["datasets"] if row["id"] == "lever_mappings")
            self.assertEqual(active_row["active_version_id"], first["version_id"])
            version_download = client.get(f"/api/input-datasets/lever_mappings/versions/{first['version_id']}/download")
            self.assertEqual(version_download.status_code, 200)
            self.assertIn(b"key,value", version_download.content)

            deleted = client.delete(f"/api/input-datasets/lever_mappings/versions/{second['version_id']}")
            self.assertEqual(deleted.status_code, 200)
            remaining = client.get("/api/input-datasets/lever_mappings/versions").json()["versions"]
            self.assertFalse(any(row["version_id"] == second["version_id"] for row in remaining))

    def test_input_dataset_overrides_are_user_scoped_and_snapshotted(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            project_a = client.post("/api/projects", headers={"X-EDIM-User-Id": "undp_analyst"}, json={"title": "Project A"}).json()["project"]["project_id"]
            upload = client.post(
                "/api/input-datasets/lever_mappings/upload",
                headers={"X-EDIM-User-Id": "undp_analyst"},
                files={"file": ("lever_mappings.csv", b"key,value\nx,y\n", "text/csv")},
            )
            self.assertEqual(upload.status_code, 200)
            self.assertEqual(upload.json()["scope"], "user_override")

            analyst_row = next(row for row in client.get("/api/input-datasets", headers={"X-EDIM-User-Id": "undp_analyst"}).json()["datasets"] if row["id"] == "lever_mappings")
            country_row = next(row for row in client.get("/api/input-datasets", headers={"X-EDIM-User-Id": "country_officer"}).json()["datasets"] if row["id"] == "lever_mappings")
            self.assertTrue(analyst_row["versioned_override"])
            self.assertFalse(country_row["versioned_override"])

            draft = client.post(
                f"/api/projects/{project_a}/runs",
                headers={"X-EDIM-User-Id": "undp_analyst"},
                json=_public_run_payload(),
            ).json()["run"]
            submitted = client.post(f"/api/projects/{project_a}/runs/{draft['run_id']}/submit", headers={"X-EDIM-User-Id": "undp_analyst"}).json()["run"]
            deadline = time.time() + 8.0
            while time.time() < deadline:
                status_payload = client.get(f"/api/executions/{submitted['execution_id']}/status", headers={"X-EDIM-User-Id": "undp_analyst"}).json()
                if status_payload["status"] in {"succeeded", "failed", "cancelled"}:
                    break
                time.sleep(0.05)
            hidden = client.get(f"/api/projects/{project_a}/runs/{draft['run_id']}", headers={"X-EDIM-User-Id": "country_officer"})
            self.assertEqual(hidden.status_code, 404)
            record = client.get(f"/api/projects/{project_a}/runs/{draft['run_id']}/diagnostics", headers={"X-EDIM-User-Id": "admin"}).json()["run"]
            self.assertEqual(record["execution_queue_message"]["schema_version"], "execution_queue_message")
            self.assertEqual(record["execution_queue_message"]["execution_id"], submitted["execution_id"])
            message_versions = record["execution_queue_message"]["dataset_versions"]
            self.assertEqual(len(message_versions), 1)
            self.assertEqual(message_versions[0]["dataset_id"], "lever_mappings")
            self.assertEqual(message_versions[0]["version_id"], upload.json()["version_id"])
            message_rows = record["execution_queue_message"]["dataset_manifest"]["datasets"]
            self.assertTrue(next(row for row in message_rows if row["id"] == "lever_mappings")["versioned_override"])
            snapshot_rows = record["dataset_snapshot"]["datasets"]
            lever_snapshot = next(row for row in snapshot_rows if row["id"] == "lever_mappings")
            self.assertEqual(lever_snapshot["active_version_id"], upload.json()["version_id"])
            self.assertEqual(record["dataset_snapshot"]["dataset_staging"]["schema_version"], "dataset_staging_v1")
            self.assertEqual(record["dataset_snapshot"]["dataset_staging"]["mode"], "copy_to_run")

    def test_local_dataset_repository_can_copy_inputs_into_run_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            run_dir = settings.runs_dir / "_queued" / "exec123"
            repository = LocalDatasetRepository(settings)

            manifest = repository.stage_runtime_datasets(
                user_id="undp_analyst",
                run_dir=run_dir,
                staging_mode="copy_to_run",
            )

            self.assertEqual(manifest["dataset_staging"]["schema_version"], "dataset_staging_v1")
            self.assertEqual(manifest["dataset_staging"]["mode"], "copy_to_run")
            lever_row = next(row for row in manifest["datasets"] if row["id"] == "lever_mappings")
            self.assertEqual(lever_row["staging_status"], "copied")
            self.assertEqual(lever_row["storage_ref"]["storage_scope"], "run_input")
            self.assertTrue(Path(lever_row["path"]).exists())
            self.assertTrue(str(lever_row["staged_relative_path"]).startswith("inputs/datasets/lever_mappings/"))
            self.assertEqual(len(lever_row["content_sha256"]), 64)

    def test_dataset_path_resolver_prefers_active_override(self):
        from model_runtime.edim_model.dataset_paths import dataset_path, set_dataset_manifest

        with tempfile.TemporaryDirectory() as tmp:
            fallback = Path(tmp) / "fallback.csv"
            fallback.write_text("default\n", encoding="utf-8")
            override = Path(tmp) / "override.csv"
            override.write_text("override\n", encoding="utf-8")
            set_dataset_manifest(
                {
                    "datasets": [
                        {"id": "development_model", "path": str(override)},
                        {"id": "missing_file", "path": str(Path(tmp) / "absent.csv")},
                    ]
                }
            )
            try:
                self.assertEqual(dataset_path("development_model", fallback), override)
                self.assertEqual(dataset_path("missing_file", fallback), fallback)
                self.assertEqual(dataset_path("unknown_dataset", fallback), fallback)
            finally:
                set_dataset_manifest(None)

    def test_dataset_upload_mirrors_to_blob_only_when_configured(self):
        from api_service.services import dataset_blob

        with patch.dict("os.environ", {"EDIM_AZURITE_CONNECTION_STRING": "", "EDIM_BLOB_ACCOUNT_URL": ""}):
            self.assertEqual(
                dataset_blob.mirror_dataset_version(
                    user_id="undp_analyst",
                    dataset_id="lever_mappings",
                    version_id="v1",
                    filename="lever_mappings.csv",
                    content=b"key,value\n",
                ),
                "",
            )

        class _FakeContainer:
            def __init__(self):
                self.uploaded = {}

            def create_container(self):
                return None

            def upload_blob(self, name, data, overwrite=False):
                self.uploaded[name] = bytes(data)

            def get_blob_client(self, name):
                return type("_FakeBlob", (), {"url": f"https://blob.example/stg-input-datasets/{name}"})()

        fake_container = _FakeContainer()
        fake_client = type("_FakeClient", (), {"get_container_client": lambda self, name: fake_container})()
        with patch.dict("os.environ", {"EDIM_AZURITE_CONNECTION_STRING": "UseDevelopmentEmulator=true"}):
            with patch("api_service.services.blob_artifact_storage._blob_service_client_from_env", return_value=fake_client):
                url = dataset_blob.mirror_dataset_version(
                    user_id="undp_analyst",
                    dataset_id="lever_mappings",
                    version_id="v1",
                    filename="lever_mappings.csv",
                    content=b"key,value\n",
                )
        self.assertEqual(url, "https://blob.example/stg-input-datasets/undp_analyst/lever_mappings/v1.csv")
        self.assertEqual(fake_container.uploaded, {"undp_analyst/lever_mappings/v1.csv": b"key,value\n"})

    def test_model_loaders_honor_dataset_overrides(self):
        from model_runtime.edim_model.core.mario_runtime import load_development_indicator_mapping
        from model_runtime.edim_model.dataset_paths import set_dataset_manifest

        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "inputs"
            mario_dir = config_dir / "mario_inputs"
            mario_dir.mkdir(parents=True)
            default_mapping = mario_dir / "development_indicator_mapping.csv"
            default_mapping.write_text(
                "indicator_id,indicator_name,driver_metric,aggregation_rule,unit,lag_years,notes\n"
                "jobs_total,Total employment impact,jobs_total,sum,jobs,0,default row\n",
                encoding="utf-8",
            )
            override_mapping = Path(tmp) / "override_mapping.csv"
            override_mapping.write_text(
                "indicator_id,indicator_name,driver_metric,aggregation_rule,unit,lag_years,notes\n"
                "jobs_total,Total employment impact,e2e_override_marker,sum,jobs,0,override row\n",
                encoding="utf-8",
            )
            set_dataset_manifest({"datasets": [{"id": "development_indicator_mapping", "path": str(override_mapping)}]})
            try:
                loaded = load_development_indicator_mapping(config_dir)
                rows = loaded.get("rows") or loaded.get("records") or loaded.get("mappings") or []
                self.assertTrue(rows)
                self.assertEqual(rows[0].get("driver_metric"), "e2e_override_marker")
            finally:
                set_dataset_manifest(None)
            fallback = load_development_indicator_mapping(config_dir)
            rows = fallback.get("rows") or fallback.get("records") or fallback.get("mappings") or []
            self.assertEqual(rows[0].get("driver_metric"), "jobs_total")

    def test_environment_setup_router_returns_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            (settings.config_dir / "scenario_metadata.csv").write_text("key,title\nnew_links,New links\n", encoding="utf-8")
            (settings.config_dir / "scenario_geography_mapping.csv").write_text("mrio_geography_code,calliope_country_code,calliope_location,alignment_level,notes\nZA,ZA,ZA,national,\n", encoding="utf-8")
            (settings.config_dir / "development_model.csv").write_text("indicator,driver,coefficient\njobs,investment,1\n", encoding="utf-8")
            _seed_mario_inputs(settings.config_dir)
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get("/api/environment-setup")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertIn("checks", payload)
            self.assertIn("ok", payload)
            self.assertTrue(payload["runtime_preflight"]["ok"])

    def test_model_runtimes_router_returns_manifest_and_dataset_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                config_dir=repo_root / "inputs",
                calliope_root=_repo_calliope_root(repo_root),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get("/api/model-runtimes")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["default_model_id"], "edim-calliope-mrio")
            self.assertEqual(payload["artifact_handoff_mode"], "shared_filesystem")
            self.assertEqual(payload["dataset_staging_mode"], "copy_to_run")
            self.assertIn("configuration_schema", payload)
            self.assertIn("architecture_catalog", payload)
            self.assertIn("scenario_catalog", payload)
            self.assertTrue(payload["architecture_catalog"]["architectures"])
            self.assertTrue(any(row.get("graph") for row in payload["model_architectures"]))
            self.assertIn("energy-development", [row["id"] for row in payload["model_architectures"]])
            engine_channel = next(
                row for row in payload["scenario_catalog"]["scenario_channels"] if row["config_key"] == "energy_model_engine"
            )
            self.assertEqual({row["value"] for row in engine_channel["options"]}, {"calliope", "osemosys"})
            osemosys_option = next(row for row in engine_channel["options"] if row["value"] == "osemosys")
            self.assertTrue(osemosys_option.get("disabled"))
            self.assertTrue(any(row["artifact_id"] == "summary_json" for row in payload["declared_outputs"]))
            self.assertTrue(any(row["id"] == "calliope_model" for row in payload["datasets"]))

    def test_system_manifest_endpoint_exposes_backend_provider_and_schema_contracts(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            response = client.get("/api/system/manifest")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["schema_version"], "edim_system_manifest")
            self.assertTrue(payload["ok"])
            contracts = payload["contracts"]
            self.assertEqual(contracts["system_manifest"], "edim_system_manifest")
            self.assertEqual(contracts["model_run_bundle"], "model_run_bundle_v1")
            self.assertEqual(contracts["execution_queue_message"], "execution_queue_message")
            self.assertEqual(contracts["execution_retry_policy"], "execution_retry_policy")
            self.assertEqual(contracts["execution_attempt"], "execution_attempt")
            self.assertEqual(contracts["runtime_event"], "runtime_event_v1")
            self.assertIn("platform_repository", payload["provider_boundaries"])
            self.assertIn("POST /api/projects/{project_id}/runs/{run_id}/submit", payload["public_endpoints"]["runs"])
            self.assertIn("POST /api/input-datasets", payload["public_endpoints"]["datasets_and_runtime"])
            self.assertIn("PATCH /api/input-datasets/{dataset_id}", payload["public_endpoints"]["datasets_and_runtime"])
            self.assertIn("POST /api/projects/{project_id}/datasets", payload["public_endpoints"]["datasets_and_runtime"])
            self.assertEqual(payload["runtime"]["execution_retry_policy"]["schema_version"], "execution_retry_policy")
            self.assertIn("operational_notes", payload)

    def test_create_app_accepts_dataset_provider_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))

            class _InjectedDatasetRepository:
                def list_input_datasets(self, *, user_id, layer="", input_property="", role=""):
                    return [
                        {
                            "id": "injected_dataset",
                            "label": "Injected dataset",
                            "layer": "scenario",
                            "role": "provider injection test",
                            "required": True,
                            "scope": "system",
                            "upload_policy": "project_override",
                            "filename": "injected.csv",
                            "source_filename": "injected.csv",
                            "exists": True,
                            "size_bytes": 12,
                            "active_version_id": "",
                            "versioned_override": False,
                            "download_url": "/api/input-datasets/injected_dataset/download",
                        }
                    ]

                def runtime_dataset_manifest(self, *, user_id):
                    return {
                        "schema_version": "model_dataset_manifest_v1",
                        "datasets": [
                            {
                                "id": "injected_dataset",
                                "label": "Injected dataset",
                                "layer": "scenario",
                                "role": "provider injection test",
                                "path": "/object-store/injected.csv",
                                "required": True,
                                "scope": "system",
                                "upload_policy": "project_override",
                                "active_version_id": "",
                                "source_path": "/object-store/injected.csv",
                            }
                        ],
                    }

            app = create_app(settings=settings, dataset_repository=_InjectedDatasetRepository())
            client = TestClient(app)
            datasets = client.get("/api/input-datasets").json()["datasets"]
            self.assertEqual(datasets[0]["id"], "injected_dataset")
            runtimes = client.get("/api/model-runtimes").json()
            self.assertEqual(runtimes["datasets"][0]["path"], "/object-store/injected.csv")

    def test_create_app_accepts_event_store_provider_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))

            class _InjectedEventStore:
                runs_dir = settings.runs_dir

                def __init__(self):
                    self.events = {}

                def event_log_path(self, execution_id):
                    return settings.runs_dir / "_injected_events" / f"{execution_id}.jsonl"

                def append_event(self, execution_id, event):
                    self.events.setdefault(execution_id, []).append(dict(event))

                def read_events(self, execution_id):
                    return list(self.events.get(execution_id, []))

                def import_event_log(self, execution_id, source_path):
                    self.append_event(execution_id, {"type": "imported", "path": str(source_path)})

            event_store = _InjectedEventStore()
            app = create_app(settings=settings, event_store=event_store)
            client = TestClient(app)
            run_id = "eventstore123"
            execution_id = "execution123"
            app.state.platform_repository.create_run_record(
                project_id="default",
                run_id=run_id,
                execution_id=execution_id,
                request_payload=_run_request().model_dump(mode="json"),
                status="running",
                user_id="undp_analyst",
            )
            event_store.append_event(
                execution_id,
                {
                    "schema_version": "runtime_event_v1",
                    "type": "progress",
                    "stage": "solve_energy",
                    "progress": 0.5,
                    "message": "Injected event store",
                },
            )

            events = client.get(f"/api/executions/{execution_id}/events").json()["events"]
            self.assertEqual(events[0]["stage"], "solve_energy")
            logs = client.get(f"/api/runs/{run_id}/logs").json()
            self.assertEqual(logs["events"][0]["message"], "Injected event store")
            self.assertNotIn("record", logs)

    def test_openapi_exposes_system_manifest_schemas(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            openapi = client.get("/openapi.json").json()
            schemas = openapi["components"]["schemas"]
            for schema_name in (
                "InputDatasetListResponse",
                "InputDatasetResponse",
                "ProjectDatasetAssignmentResponse",
                "DatasetUploadResponse",
                "DatasetVersionsResponse",
                "DatasetDeleteResponse",
                "EnvironmentSetupResponse",
                "SystemManifestResponse",
                "ProjectListResponse",
                "ProjectRunListResponse",
                "ProjectRunResponse",
                "ReportListResponse",
                "ExportListResponse",
                "ModelRuntimeCatalogResponse",
                "RunEventsResponse",
                "RunArtifactListResponse",
            ):
                self.assertIn(schema_name, schemas)

            paths = openapi["paths"]
            self.assertEqual(
                paths["/api/input-datasets"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/InputDatasetListResponse",
            )
            self.assertEqual(
                paths["/api/input-datasets"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/InputDatasetResponse",
            )
            self.assertEqual(
                paths["/api/projects/{project_id}/datasets"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ProjectDatasetAssignmentResponse",
            )
            self.assertEqual(
                paths["/api/environment-setup"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/EnvironmentSetupResponse",
            )
            self.assertEqual(
                paths["/api/model-runtimes"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ModelRuntimeCatalogResponse",
            )
            self.assertEqual(
                paths["/api/projects/{project_id}/runs/validate"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/EnvironmentSetupResponse",
            )
            self.assertEqual(
                paths["/api/system/manifest"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/SystemManifestResponse",
            )
            self.assertNotIn("/api/handoff-contract", paths)
            self.assertEqual(
                paths["/api/executions/{execution_id}/events"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/RunEventsResponse",
            )
            self.assertEqual(
                paths["/api/runs/{run_id}/artifacts"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/RunArtifactListResponse",
            )
            self.assertEqual(
                paths["/api/projects"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ProjectListResponse",
            )
            self.assertEqual(
                paths["/api/projects/{project_id}/runs"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ProjectRunListResponse",
            )
            self.assertEqual(
                paths["/api/projects/{project_id}/runs/{run_id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/PublicProjectRunResponse",
            )
            self.assertEqual(
                paths["/api/projects/{project_id}/runs/{run_id}/diagnostics"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ProjectRunResponse",
            )

    def test_project_run_api_submits_and_reports_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            project = client.post("/api/projects", json={"title": "Run API smoke"}).json()["project"]
            project_id = project["project_id"]
            draft_response = client.post(
                f"/api/projects/{project_id}/runs",
                json=_public_run_payload(),
            )
            self.assertEqual(draft_response.status_code, 200)
            run_id = draft_response.json()["run"]["run_id"]
            response = client.post(f"/api/projects/{project_id}/runs/{run_id}/submit")
            self.assertEqual(response.status_code, 200)
            self.assertIn("run", response.json())
            execution_id = response.json()["run"]["execution_id"]
            self.assertTrue(run_id)
            deadline = time.time() + 8.0
            final = None
            while time.time() < deadline:
                status_payload = client.get(f"/api/executions/{execution_id}/status").json()
                if status_payload["status"] in {"succeeded", "failed", "cancelled"}:
                    final = status_payload
                    break
                time.sleep(0.05)
            self.assertIsNotNone(final)
            self.assertEqual(final["status"], "succeeded")
            self.assertEqual(final["run_id"], run_id)
            events = client.get(f"/api/executions/{execution_id}/events").json()["events"]
            self.assertTrue(any(row.get("type") == "result" for row in events))

            records = client.get(f"/api/projects/{project_id}/runs").json()["runs"]
            self.assertTrue(any(row["run_id"] == run_id and row["status"] == "succeeded" for row in records))
            logs = client.get(f"/api/runs/{run_id}/logs").json()
            self.assertEqual(logs["execution_id"], execution_id)
            self.assertTrue(any(row.get("type") == "result" for row in logs["events"]))
            self.assertNotIn("record", logs)

            restarted_app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            restarted_client = TestClient(restarted_app)
            persisted_status = restarted_client.get(f"/api/executions/{execution_id}/status")
            self.assertEqual(persisted_status.status_code, 200)
            self.assertEqual(persisted_status.json()["run_id"], run_id)
            self.assertEqual(persisted_status.json()["status"], "succeeded")

    def test_project_run_public_contract_hides_internal_execution_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            project = client.post("/api/projects", json={"title": "Compact contract"}).json()["project"]
            project_id = project["project_id"]
            public_payload = {
                "run_name": "Compact run",
                "model_architecture_id": "energy-development",
                "energy_model_engine": "calliope",
                "scenario": {
                    "energy_scenario_key": "new_links",
                    "target_scenario_id": "S2",
                    "target_year": 2030,
                },
                "run_profile": "analysis",
                "levers": {"demand_multiplier": 1.1},
            }

            draft = client.post(f"/api/projects/{project_id}/runs", json=public_payload)
            self.assertEqual(draft.status_code, 200)
            record = draft.json()["run"]
            self.assertEqual(record["project_id"], project_id)
            self.assertNotIn("request", record)
            self.assertNotIn("dataset_snapshot", record)
            self.assertNotIn("execution_queue_message", record)
            self.assertNotIn("execution_attempts", record)
            self.assertNotIn("artifact_catalog", record)
            self.assertEqual(record["configuration"]["scenario"]["energy_scenario_key"], "new_links")
            self.assertEqual(record["configuration"]["scenario"]["target_scenario_id"], "S2")
            diagnostic_record = client.get(f"/api/projects/{project_id}/runs/{record['run_id']}/diagnostics").json()["run"]
            self.assertEqual(diagnostic_record["request"]["energy_scenario_key"], "new_links")
            self.assertEqual(diagnostic_record["request"]["mrio_scenario_id"], "S2")
            self.assertTrue(diagnostic_record["request"]["strict_validation"])
            self.assertTrue(diagnostic_record["request"]["allow_placeholder_data"])

            rows = client.get(f"/api/projects/{project_id}/runs").json()["runs"]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertNotIn("request", row)
            self.assertNotIn("dataset_snapshot", row)
            self.assertNotIn("artifact_policy", row)
            self.assertNotIn("execution_queue_message", row)
            self.assertNotIn("execution_attempts", row)
            self.assertNotIn("artifact_catalog", row)
            self.assertEqual(row["configuration"]["scenario"]["energy_scenario_key"], "new_links")
            self.assertEqual(row["configuration"]["scenario"]["target_scenario_id"], "S2")
            self.assertEqual(row["configuration"]["run_profile"], "analysis")

            project_summary = client.get("/api/projects").json()["projects"][0]["visual_summary"]
            self.assertEqual(project_summary["model_count"], 1)
            self.assertEqual(project_summary["completed_count"], 0)
            self.assertEqual(project_summary["scenario_count"], 1)
            self.assertEqual(project_summary["models"][0]["run_id"], record["run_id"])
            self.assertEqual(project_summary["models"][0]["architecture_id"], "energy-development")
            self.assertEqual(project_summary["models"][0]["target_year"], 2030)
            self.assertEqual(project_summary["models"][0]["lever_count"], 1)

            validation = client.post(f"/api/projects/{project_id}/runs/validate", json={"configuration": public_payload})
            self.assertEqual(validation.status_code, 200)
            self.assertIn("runtime_preflight", validation.json())

            submitted = client.post(f"/api/projects/{project_id}/runs/{record['run_id']}/submit")
            self.assertEqual(submitted.status_code, 200)
            submitted_row = submitted.json()["run"]
            self.assertNotIn("request", submitted_row)
            self.assertNotIn("execution_queue_message", submitted_row)
            self.assertNotIn("execution_attempts", submitted_row)
            status = client.get(f"/api/executions/{submitted_row['execution_id']}/status")
            self.assertEqual(status.status_code, 200)
            status_row = status.json()
            self.assertNotIn("request", status_row)
            self.assertNotIn("dataset_snapshot", status_row)
            self.assertNotIn("execution_queue_message", status_row)
            self.assertNotIn("execution_attempts", status_row)
            deadline = time.time() + 8.0
            while time.time() < deadline:
                status_row = client.get(f"/api/executions/{submitted_row['execution_id']}/status").json()
                if status_row["status"] in {"succeeded", "failed", "cancelled"}:
                    break
                time.sleep(0.05)

    def test_project_run_draft_submit_duplicate_report_and_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            project = client.post("/api/projects", json={"title": "Single-user project", "geography": "ZA"}).json()["project"]
            project_id = project["project_id"]
            upload = client.post(
                "/api/input-datasets/lever_mappings/upload",
                files={"file": ("lever_mappings.csv", b"key,value\nx,y\n", "text/csv")},
            )
            self.assertEqual(upload.status_code, 200)
            uploaded_version_id = upload.json()["version_id"]
            draft = client.post(f"/api/projects/{project_id}/runs", json=_public_run_payload("Draft A"))
            self.assertEqual(draft.status_code, 200)
            draft_record = draft.json()["run"]
            run_id = draft_record["run_id"]
            self.assertEqual(draft_record["project_run_number"], 1)

            patched = client.patch(f"/api/projects/{project_id}/runs/{run_id}", json={"run_name": "Draft B"})
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(patched.json()["run"]["run_name"], "Draft B")

            submitted = client.post(f"/api/projects/{project_id}/runs/{run_id}/submit")
            self.assertEqual(submitted.status_code, 200)
            execution_id = submitted.json()["run"]["execution_id"]
            deadline = time.time() + 8.0
            while time.time() < deadline:
                status_payload = client.get(f"/api/executions/{execution_id}/status").json()
                if status_payload["status"] in {"succeeded", "failed", "cancelled"}:
                    break
                time.sleep(0.05)
            self.assertEqual(client.get(f"/api/projects/{project_id}/runs/{run_id}").json()["run"]["status"], "succeeded")
            rogue_intermediate = settings.runs_dir / run_id / "work" / "secret_intermediate.txt"
            rogue_intermediate.parent.mkdir(parents=True, exist_ok=True)
            rogue_intermediate.write_text("must not be bundled unless declared by artifact policy", encoding="utf-8")

            duplicate = client.post(f"/api/projects/{project_id}/runs/{run_id}/duplicate")
            self.assertEqual(duplicate.status_code, 200)
            self.assertEqual(duplicate.json()["run"]["status"], "draft")
            self.assertEqual(duplicate.json()["run"]["project_run_number"], 2)

            summary_path = settings.runs_dir / run_id / "artifacts" / "final" / "summary.json"
            evidence_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            evidence_summary["integrated_results"] = {
                "model_quality": {
                    "status": "exploratory_only",
                    "score": 42,
                    "summary": "Placeholder evidence requires analyst review.",
                    "issues": [{"code": "placeholder_inputs", "severity": "error"}],
                }
            }
            summary_path.write_text(json.dumps(evidence_summary, indent=2), encoding="utf-8")
            enriched_run = client.get(f"/api/projects/{project_id}/runs/{run_id}").json()["run"]
            self.assertEqual(enriched_run["model_id"], run_id)
            self.assertEqual(enriched_run["evidence_status"], "exploratory_only")
            unacknowledged_report = client.post(
                f"/api/projects/{project_id}/reports",
                json={"run_ids": [run_id]},
            )
            self.assertEqual(unacknowledged_report.status_code, 409)

            report = client.post(
                f"/api/projects/{project_id}/reports",
                json={
                    "run_ids": [run_id],
                    "options": {"acknowledge_exploratory": True},
                },
            ).json()["report"]
            self.assertEqual(report["status"], "succeeded")
            self.assertEqual(report["evidence_status"], "exploratory_only")
            self.assertTrue(report["requires_evidence_acknowledgement"])
            self.assertEqual(report["status_history"][-1]["status"], "succeeded")
            report_download = client.get(f"/api/projects/{project_id}/reports/{report['report_id']}/download")
            self.assertEqual(report_download.status_code, 200)
            self.assertIn("EDIM", report_download.text)
            self.assertEqual(report["source_schema_version"], "edim_project_report_source_v1")
            report_data = client.get(f"/api/projects/{project_id}/reports/{report['report_id']}/data")
            self.assertEqual(report_data.status_code, 200)
            report_source = report_data.json()
            self.assertEqual(report_source["schema_version"], "edim_project_report_source_v1")
            self.assertEqual(report_source["project"]["project_id"], project_id)
            self.assertEqual(report_source["runs"][0]["run_id"], run_id)
            self.assertIn("artifact_catalog", report_source["runs"][0])
            self.assertEqual(report_source["evidence"]["status"], "exploratory_only")
            self.assertEqual(report_source["runs"][0]["evidence"]["status"], "exploratory_only")

            export = client.post(f"/api/projects/{project_id}/exports", json={"run_ids": [run_id]}).json()["export"]
            self.assertEqual(export["status"], "succeeded")
            blocked_delete = client.delete(f"/api/input-datasets/lever_mappings/versions/{uploaded_version_id}")
            self.assertEqual(blocked_delete.status_code, 409)
            self.assertIn("referenced by submitted run snapshots", str(blocked_delete.json()["detail"]))
            export_download = client.get(f"/api/projects/{project_id}/exports/{export['export_id']}/download")
            self.assertEqual(export_download.status_code, 200)
            self.assertIn("storage_ref", export)
            self.assertEqual(export["storage_ref"]["storage_scope"], "platform")
            self.assertTrue(export["storage_ref"]["object_key"].startswith("exports/"))
            with ZipFile(io.BytesIO(export_download.content)) as zf:
                names = set(zf.namelist())
                self.assertIn("manifest.json", names)
                self.assertIn("EVIDENCE_STATUS.txt", names)
                self.assertIn("datasets/uploaded_dataset_manifest.json", names)
                manifest = json.loads(zf.read("manifest.json"))
                self.assertEqual(manifest["evidence"]["status"], "exploratory_only")
                self.assertIn("EXPLORATORY OUTPUT", zf.read("EVIDENCE_STATUS.txt").decode("utf-8"))
                self.assertTrue(any(name.startswith("reports/") and name.endswith(".md") for name in names))
                self.assertTrue(any(name.startswith("reports/") and name.endswith(".source.json") for name in names))
                self.assertTrue(any(name.startswith("datasets/users/undp_analyst/lever_mappings/") and name.endswith(".csv") for name in names))
                self.assertTrue(any(name == f"runs/{run_id}/artifacts/final/summary.json" for name in names))
                self.assertNotIn(f"runs/{run_id}/work/secret_intermediate.txt", names)

            deleted_project = client.delete(f"/api/projects/{project_id}?delete_files=true")
            self.assertEqual(deleted_project.status_code, 200)
            self.assertGreaterEqual(deleted_project.json()["deleted_runs"], 1)
            self.assertEqual(client.get(f"/api/projects/{project_id}").status_code, 404)
            self.assertEqual(client.get(f"/api/projects/{project_id}/runs/{run_id}").status_code, 404)

    def test_project_run_lifecycle_allows_completed_rename_but_rejects_configuration_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            project = client.post("/api/projects", json={"title": "Lifecycle project"}).json()["project"]
            project_id = project["project_id"]
            draft = client.post(f"/api/projects/{project_id}/runs", json=_public_run_payload()).json()["run"]
            run_id = draft["run_id"]

            submitted = client.post(f"/api/projects/{project_id}/runs/{run_id}/submit").json()["run"]
            execution_id = submitted["execution_id"]
            submitted_record = client.get(f"/api/projects/{project_id}/runs/{run_id}").json()["run"]
            self.assertEqual(submitted_record["run_id"], run_id)
            self.assertEqual(submitted_record["project_run_number"], 1)
            self.assertEqual(submitted_record["created_at"], draft["created_at"])
            duplicate_inflight_submit = client.post(f"/api/projects/{project_id}/runs/{run_id}/submit")
            self.assertEqual(duplicate_inflight_submit.status_code, 409)
            deadline = time.time() + 8.0
            while time.time() < deadline:
                status_payload = client.get(f"/api/executions/{execution_id}/status").json()
                if status_payload["status"] in {"succeeded", "failed", "cancelled"}:
                    break
                time.sleep(0.05)
            self.assertEqual(client.get(f"/api/projects/{project_id}/runs/{run_id}").json()["run"]["status"], "succeeded")

            original_request = client.get(
                f"/api/projects/{project_id}/runs/{run_id}/diagnostics"
            ).json()["run"]["request"]
            edit_response = client.patch(f"/api/projects/{project_id}/runs/{run_id}", json={"run_name": "Published model"})
            self.assertEqual(edit_response.status_code, 200)
            self.assertEqual(edit_response.json()["run"]["run_name"], "Published model")
            renamed_request = client.get(
                f"/api/projects/{project_id}/runs/{run_id}/diagnostics"
            ).json()["run"]["request"]
            self.assertEqual(renamed_request, original_request)
            configuration_response = client.patch(
                f"/api/projects/{project_id}/runs/{run_id}",
                json={"request": _public_run_payload("Changed configuration")},
            )
            self.assertEqual(configuration_response.status_code, 409)
            status_response = client.patch(f"/api/projects/{project_id}/runs/{run_id}", json={"status": "draft"})
            self.assertEqual(status_response.status_code, 422)
            resubmit_response = client.post(f"/api/projects/{project_id}/runs/{run_id}/submit")
            self.assertEqual(resubmit_response.status_code, 409)

    def test_cancel_project_run_restores_draft_and_clears_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                if progress_callback:
                    progress_callback("solve_energy", 0.4, "Solving")
                partial = settings.runs_dir / execution_context.run_id / "artifacts" / "partial.txt"
                partial.parent.mkdir(parents=True, exist_ok=True)
                partial.write_text("partial", encoding="utf-8")
                for _ in range(150):
                    if cancel_requested and cancel_requested():
                        raise RuntimeError("cancelled")
                    time.sleep(0.01)
                return ModelExecutionResult(run_id=execution_context.run_id, summary=_summary_template(execution_context.run_id), warnings=[])

            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_FakeRuntime(_fake_run)))
            client = TestClient(app)
            project = client.post("/api/projects", json={"title": "Cancel project"}).json()["project"]
            project_id = project["project_id"]
            draft = client.post(f"/api/projects/{project_id}/runs", json=_public_run_payload()).json()["run"]
            run_id = draft["run_id"]
            submitted = client.post(f"/api/projects/{project_id}/runs/{run_id}/submit").json()["run"]
            execution_id = submitted["execution_id"]
            deadline = time.time() + 4.0
            while time.time() < deadline:
                payload = client.get(f"/api/executions/{execution_id}/status").json()
                if payload["status"] == "running":
                    break
                time.sleep(0.03)

            cancel_response = client.post(f"/api/executions/{execution_id}/cancel")
            self.assertEqual(cancel_response.status_code, 200)
            status_payload = cancel_response.json()
            deadline = time.time() + 6.0
            while time.time() < deadline:
                status_payload = client.get(f"/api/executions/{execution_id}/status").json()
                if status_payload["status"] == "draft":
                    break
                time.sleep(0.03)
            self.assertEqual(status_payload["status"], "draft")
            self.assertIsNone(status_payload["artifacts"])
            record = client.get(f"/api/projects/{project_id}/runs/{run_id}").json()["run"]
            self.assertEqual(record["status"], "draft")
            self.assertFalse(record["summary_available"])
            self.assertEqual(record["execution_id"], "")
            diagnostic_record = client.get(f"/api/projects/{project_id}/runs/{run_id}/diagnostics").json()["run"]
            self.assertEqual(diagnostic_record["artifact_catalog"], [])
            records = client.get(f"/api/projects/{project_id}/runs").json()["runs"]
            self.assertFalse(any(row["status"] == "cancelled" for row in records))
            self.assertTrue(any(row["run_id"] == run_id and row["status"] == "draft" for row in records))
            self.assertFalse((settings.runs_dir / run_id).exists())
            self.assertFalse((settings.runs_dir / "_queued" / execution_id).exists())

    def test_sqlite_platform_store_persists_runs_reports_and_exports(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
                platform_store_backend="sqlite",
                platform_sqlite_path=Path(tmp) / "platform" / "platform.sqlite3",
            )
            app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            client = TestClient(app)

            project = client.post("/api/projects", json={"title": "SQLite-backed project", "geography": "ZA"}).json()["project"]
            project_id = project["project_id"]
            draft = client.post(f"/api/projects/{project_id}/runs", json=_public_run_payload()).json()["run"]
            submitted = client.post(f"/api/projects/{project_id}/runs/{draft['run_id']}/submit").json()["run"]
            execution_id = submitted["execution_id"]
            run_id = submitted["run_id"]
            deadline = time.time() + 8.0
            while time.time() < deadline:
                payload = client.get(f"/api/executions/{execution_id}/status").json()
                if payload["status"] in {"succeeded", "failed", "cancelled"}:
                    break
                time.sleep(0.05)
            self.assertTrue(settings.platform_sqlite_path.exists())

            restarted_app = create_app(settings=settings, job_manager=JobManager(settings, runtime=_successful_fake_runtime()))
            restarted_client = TestClient(restarted_app)
            status = restarted_client.get(f"/api/executions/{execution_id}/status")
            self.assertEqual(status.status_code, 200)
            self.assertEqual(status.json()["run_id"], run_id)
            self.assertEqual(status.json()["status"], "succeeded")
            records = restarted_client.get(f"/api/projects/{project_id}/runs").json()["runs"]
            self.assertTrue(any(row["run_id"] == run_id for row in records))

            report = restarted_client.post(f"/api/projects/{project_id}/reports", json={"run_ids": [run_id]}).json()["report"]
            self.assertEqual(report["status"], "succeeded")
            self.assertTrue(report["storage_ref"]["object_key"].startswith("reports/"))
            self.assertTrue(report["source_data_storage_ref"]["object_key"].startswith("reports/"))
            self.assertEqual(restarted_client.get(f"/api/projects/{project_id}/reports/{report['report_id']}/data").status_code, 200)
            export = restarted_client.post(f"/api/projects/{project_id}/exports", json={"run_ids": [run_id]}).json()["export"]
            self.assertEqual(export["status"], "succeeded")
            self.assertTrue(export["storage_ref"]["object_key"].startswith("exports/"))
            self.assertGreater(export["storage_ref"]["size_bytes"], 0)

    def test_report_markdown_contains_key_sections(self):
        summary = {
            "run_id": "abc12345",
            "energy_scenario_key": "new_links",
            "mrio_scenario_id": "ZA-S2",
            "target_year": 2030,
            "run_profile": "dev",
            "warnings": ["example warning"],
            "summary_diagnostics": {
                "run_metadata": {
                    "solver": "highs",
                    "termination_condition": "optimal",
                    "solution_time_seconds": 12.3,
                    "objective_function_value": 45.6,
                },
                "reliability": {
                    "demand_total": 100.0,
                    "unserved_total": 0.5,
                    "unserved_energy_share": 0.005,
                    "hours_with_unserved": 2,
                },
            },
        }
        integrated = {
            "integrated_overview": {
                "metrics": [{"key": "jobs_total", "label": "Jobs", "unit": "jobs", "value": 10.0}]
            },
            "development_drivers": {
                "capex_effect_musd": 1.0,
                "opex_effect_musd": 2.0,
                "reliability_penalty_proxy": 3.0,
                "import_leakage_musd": 4.0,
            },
            "development_confidence": {
                "coupling_mode": "mario",
                "mapping_coverage_share": 0.9,
                "unmapped_mapping_share": 0.1,
                "mario_runtime_executed": False,
                "mario_runtime_seconds": 0.0,
                "mario_runner_source": "",
            },
        }
        report = build_run_report_markdown(summary=summary, integrated=integrated)
        self.assertIn("# EDIM Run Report", report)
        self.assertIn("energy_scenario_key", report)
        self.assertIn("mrio_scenario_id", report)
        self.assertIn("## Integrated Metrics", report)
        self.assertIn("example warning", report)


class JobManagerTests(unittest.TestCase):
    def test_job_manager_runs_injected_runtime_and_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                model_manifest_path=repo_root / "model_runtime" / "edim_model" / "model_manifest.json",
                dataset_manifest_path=repo_root / "model_runtime" / "edim_model" / "dataset_manifest.json",
            )
            manager = JobManager(settings, runtime=_successful_fake_runtime())
            job = manager.submit(_run_request())
            final = _wait_terminal(manager, job.execution_id, timeout_seconds=10.0)
            self.assertEqual(final.status, "succeeded")
            self.assertIsNotNone(final.summary)
            events = RuntimeEventLog(manager.event_log_path(job.execution_id)).read()
            self.assertTrue(any(row.get("type") == "result" for row in events))

    def test_job_manager_publishes_runtime_artifacts_through_storage_provider(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp), dedupe_enabled=False)
            calls = []

            class _InjectedArtifactStorage:
                def publish_run_artifacts(self, *, run_id, run_dir, artifact_catalog, handoff_mode):
                    calls.append(
                        {
                            "run_id": run_id,
                            "run_dir": str(run_dir),
                            "artifact_catalog": list(artifact_catalog),
                            "handoff_mode": handoff_mode,
                        }
                    )
                    return {
                        "schema_version": "runtime_artifact_publication_v1",
                        "run_id": run_id,
                        "handoff_mode": handoff_mode,
                        "storage_provider": "injected_storage",
                        "status": "published",
                        "published": True,
                        "artifact_count": len(artifact_catalog),
                    }

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                summary = _summary_template(execution_context.run_id)
                summary["artifact_catalog"] = []
                return ModelExecutionResult(run_id=execution_context.run_id, summary=summary, warnings=[])

            manager = JobManager(
                settings,
                runtime=_FakeRuntime(_fake_run),
                artifact_storage=_InjectedArtifactStorage(),
            )
            job = manager.submit(_run_request())
            final = _wait_terminal(manager, job.execution_id, timeout_seconds=8.0)

            self.assertEqual(final.status, "succeeded")
            self.assertEqual(calls[0]["handoff_mode"], "shared_filesystem")
            self.assertIsNotNone(final.summary)
            self.assertEqual(final.summary.artifact_publication["storage_provider"], "injected_storage")

    def test_job_manager_stages_dataset_manifest_into_request_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                dataset_staging_mode="copy_to_run",
            )
            captured = {}

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                captured["dataset_manifest"] = execution_request.request_bundle["dataset_manifest"]
                captured["queue_message"] = execution_request.request_bundle["queue_message"]
                return ModelExecutionResult(run_id=execution_context.run_id, summary=_summary_template(execution_context.run_id), warnings=[])

            manager = JobManager(settings, runtime=_FakeRuntime(_fake_run))
            job = manager.submit(_run_request())
            final = _wait_terminal(manager, job.execution_id, timeout_seconds=8.0)

            self.assertEqual(final.status, "succeeded")
            self.assertEqual(captured["dataset_manifest"]["dataset_staging"]["mode"], "copy_to_run")
            self.assertEqual(captured["queue_message"]["schema_version"], "execution_queue_message")
            self.assertEqual(captured["queue_message"]["execution_id"], job.execution_id)
            self.assertEqual(captured["queue_message"]["run_id"], final.run_id)
            self.assertEqual(captured["queue_message"]["retry_policy"]["schema_version"], "execution_retry_policy")
            self.assertEqual(final.execution_queue_message["schema_version"], "execution_queue_message")
            self.assertTrue(final.worker_id.startswith("local-thread:"))
            self.assertEqual(len(final.execution_attempts), 1)
            self.assertEqual(final.execution_attempts[0]["schema_version"], "execution_attempt")
            self.assertEqual(final.execution_attempts[0]["status"], "succeeded")
            self.assertFalse(final.execution_attempts[0]["cancellation_requested"])
            lever_row = next(row for row in captured["dataset_manifest"]["datasets"] if row["id"] == "lever_mappings")
            self.assertEqual(lever_row["staging_status"], "copied")
            self.assertTrue(Path(lever_row["path"]).exists())

    def test_job_manager_uses_dataset_snapshot_from_submit_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp), dedupe_enabled=False)
            dataset_repository = LocalDatasetRepository(settings)
            first = dataset_repository.register_upload(
                "lever_mappings",
                "lever_mappings.csv",
                b"key,value\nfirst,1\n",
                user_id="undp_analyst",
            )
            release_worker = threading.Event()

            class _BlockingQueue:
                def __init__(self):
                    self._queue = queue.Queue()

                def put(self, message):
                    self._queue.put(message)

                def get(self):
                    release_worker.wait(timeout=5.0)
                    return self._queue.get()

                def task_done(self):
                    self._queue.task_done()

                def qsize(self):
                    return self._queue.qsize()

            captured = {}

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                captured["dataset_manifest"] = execution_request.request_bundle["dataset_manifest"]
                return ModelExecutionResult(run_id=execution_context.run_id, summary=_summary_template(execution_context.run_id), warnings=[])

            manager = JobManager(
                settings,
                runtime=_FakeRuntime(_fake_run),
                dataset_repository=dataset_repository,
                execution_queue=_BlockingQueue(),
            )
            job = manager.submit(_run_request(), user_id="undp_analyst")
            dataset_repository.register_upload(
                "lever_mappings",
                "lever_mappings.csv",
                b"key,value\nsecond,2\n",
                user_id="undp_analyst",
            )
            release_worker.set()
            final = _wait_terminal(manager, job.execution_id, timeout_seconds=8.0)

            self.assertEqual(final.status, "succeeded")
            lever_row = next(row for row in captured["dataset_manifest"]["datasets"] if row["id"] == "lever_mappings")
            self.assertEqual(lever_row["active_version_id"], first["version_id"])
            record = manager._run_repository.get_run_record(final.run_id, user_id="undp_analyst")  # noqa: SLF001
            snapshot_row = next(row for row in record["dataset_snapshot"]["datasets"] if row["id"] == "lever_mappings")
            self.assertEqual(snapshot_row["active_version_id"], first["version_id"])

    def test_job_manager_fails_nonlocal_handoff_without_published_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(
                _build_settings(Path(tmp), dedupe_enabled=False),
                runtime_artifact_handoff_mode="worker_staged_upload",
            )

            class _UnpublishedArtifactStorage:
                def publish_run_artifacts(self, *, run_id, run_dir, artifact_catalog, handoff_mode):
                    return {
                        "schema_version": "runtime_artifact_publication_v1",
                        "run_id": run_id,
                        "handoff_mode": handoff_mode,
                        "storage_provider": "unpublished_test_storage",
                        "status": "not_uploaded",
                        "published": False,
                        "artifact_count": len(artifact_catalog),
                    }

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                return ModelExecutionResult(run_id=execution_context.run_id, summary=_summary_template(execution_context.run_id), warnings=[])

            manager = JobManager(
                settings,
                runtime=_FakeRuntime(_fake_run),
                artifact_storage=_UnpublishedArtifactStorage(),
            )
            job = manager.submit(_run_request())
            final = _wait_terminal(manager, job.execution_id, timeout_seconds=8.0)

            self.assertEqual(final.status, "failed")
            self.assertIn("Runtime artifact handoff failed", final.error or "")

    def test_dedupes_identical_inflight_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp), dedupe_enabled=True)
            calls = {"count": 0}

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                calls["count"] += 1
                time.sleep(0.2)
                run_id = f"run-{calls['count']:02d}"
                (settings.runs_dir / run_id).mkdir(parents=True, exist_ok=True)
                req = RunRequest(**execution_request.request_payload)
                summary = {
                    "run_id": run_id,
                    "energy_scenario_key": req.energy_scenario_key,
                    "mrio_scenario_id": req.mrio_scenario_id,
                    "target_year": req.target_year,
                    "run_profile": req.run_profile,
                    "warnings": [],
                }
                return ModelExecutionResult(run_id=run_id, summary=summary, warnings=[])

            req = RunRequest(energy_scenario_key="new_links", mrio_scenario_id="ZA-S2", target_year=2030, run_profile="dev", strict_validation=False, allow_placeholder_data=True, levers=LeverValues())
            manager = JobManager(settings, runtime=_FakeRuntime(_fake_run))
            first = manager.submit(req)
            time.sleep(0.03)
            second = manager.submit(req)
            self.assertEqual(first.execution_id, second.execution_id)
            final = _wait_terminal(manager, first.execution_id)
            self.assertEqual(final.status, "succeeded")
            self.assertEqual(calls["count"], 1)

    def test_cancel_running_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp), dedupe_enabled=False)

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                if progress_callback:
                    progress_callback("solve_energy", 0.5, "Solving")
                partial_artifact = settings.runs_dir / execution_context.run_id / "artifacts" / "partial.txt"
                partial_artifact.parent.mkdir(parents=True, exist_ok=True)
                partial_artifact.write_text("partial output that must be cleared on cancel", encoding="utf-8")
                for _ in range(120):
                    if cancel_requested and cancel_requested():
                        raise RuntimeError("Run cancelled by user request.")
                    time.sleep(0.01)
                run_id = "run-long"
                (settings.runs_dir / run_id).mkdir(parents=True, exist_ok=True)
                req = RunRequest(**execution_request.request_payload)
                summary = {
                    "run_id": run_id,
                    "energy_scenario_key": req.energy_scenario_key,
                    "mrio_scenario_id": req.mrio_scenario_id,
                    "target_year": req.target_year,
                    "run_profile": req.run_profile,
                    "warnings": [],
                }
                return ModelExecutionResult(run_id=run_id, summary=summary, warnings=[])

            req = RunRequest(energy_scenario_key="new_links", mrio_scenario_id="ZA-S2", target_year=2030, run_profile="dev", strict_validation=False, allow_placeholder_data=True, levers=LeverValues())
            manager = JobManager(settings, runtime=_FakeRuntime(_fake_run))
            job = manager.submit(req)
            deadline = time.time() + 2.0
            while time.time() < deadline:
                if manager.get(job.execution_id).status == "running":
                    break
                time.sleep(0.02)
            manager.cancel(job.execution_id)
            deadline = time.time() + 4.0
            final = manager.get(job.execution_id)
            while time.time() < deadline:
                final = manager.get(job.execution_id)
                if final.status == "draft":
                    break
                time.sleep(0.02)
            self.assertEqual(final.status, "draft")
            self.assertFalse(final.cancellation_requested)
            self.assertEqual(final.stage, "draft")
            self.assertEqual(final.progress, 0.0)
            self.assertEqual(final.execution_attempts, [])
            self.assertIsNone(final.artifacts)
            self.assertFalse((settings.runs_dir / job.run_id).exists())
            self.assertFalse((settings.runs_dir / "_queued" / job.execution_id).exists())

    def test_queue_capacity_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp), dedupe_enabled=False, queue_capacity=1)

            def _fake_run(execution_request, execution_context, progress_callback=None, cancel_requested=None):
                time.sleep(0.25)
                run_id = "run-one"
                (settings.runs_dir / run_id).mkdir(parents=True, exist_ok=True)
                req = RunRequest(**execution_request.request_payload)
                summary = {
                    "run_id": run_id,
                    "energy_scenario_key": req.energy_scenario_key,
                    "mrio_scenario_id": req.mrio_scenario_id,
                    "target_year": req.target_year,
                    "run_profile": req.run_profile,
                    "warnings": [],
                }
                return ModelExecutionResult(run_id=run_id, summary=summary, warnings=[])

            req = RunRequest(energy_scenario_key="new_links", mrio_scenario_id="ZA-S2", target_year=2030, run_profile="dev", strict_validation=False, allow_placeholder_data=True, levers=LeverValues())
            manager = JobManager(settings, runtime=_FakeRuntime(_fake_run))
            first = manager.submit(req)
            with self.assertRaises(JobQueueFullError):
                manager.submit(req)
            _wait_terminal(manager, first.execution_id)


class SubprocessModelRuntimeTests(unittest.TestCase):
    def test_subprocess_runtime_uses_safe_environment_allowlist(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(
                _build_settings(Path(tmp)),
                runtime_config={"model_runtime": {"safe_env": ["EDIM_RUNTIME_CACHE_DIR", "EDIM_EXTERNAL_API_KEY"]}},
            )
            manifest = ModelRuntimeManifest(
                schema_version="model_runtime_manifest_v1",
                model_id="test-runtime",
                model_version="test",
                label="Test runtime",
                description="",
                entrypoint_type="subprocess",
                entrypoint=[sys.executable, "-c", "print('ok')"],
                resource_requirements={},
            )
            runtime = SubprocessModelRuntime(settings, manifest)
            with patch.dict(
                os.environ,
                {
                    "EDIM_EXTERNAL_API_KEY": "secret",
                    "EDIM_DATABASE_PASSWORD": "secret",
                    "EDIM_RUNTIME_CACHE_DIR": "/tmp/edim-runtime-cache",
                    "PATH": os.environ.get("PATH", ""),
                },
                clear=False,
            ):
                env = runtime._env()  # noqa: SLF001
            self.assertNotIn("EDIM_EXTERNAL_API_KEY", env)
            self.assertNotIn("EDIM_DATABASE_PASSWORD", env)
            self.assertEqual(env["EDIM_RUNTIME_CACHE_DIR"], "/tmp/edim-runtime-cache")
            self.assertEqual(env["PYTHONUNBUFFERED"], "1")
            self.assertIn(str(runtime._cwd()), env["PYTHONPATH"])  # noqa: SLF001

    def test_subprocess_runtime_drains_stderr_without_deadlock(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = _build_settings(base)
            run_id = "subproc123"
            script = (
                "import json, sys;"
                "[print('stderr line %s' % i, file=sys.stderr, flush=True) for i in range(400)];"
                "print(json.dumps({'schema_version':'runtime_event_v1','type':'result','stage':'complete',"
                "'progress':1.0,'run_id':'subproc123','payload':{'summary':{'run_id':'subproc123',"
                "'energy_scenario_key':'new_links','mrio_scenario_id':'S2','target_year':2030,"
                "'run_profile':'dev','warnings':[]}}}), flush=True)"
            )
            manifest = ModelRuntimeManifest(
                schema_version="model_runtime_manifest_v1",
                model_id="stderr-runtime",
                model_version="test",
                label="stderr runtime",
                description="",
                entrypoint_type="subprocess",
                entrypoint=[sys.executable, "-c", script],
                resource_requirements={"timeout_seconds": 10},
            )
            bundle_path = base / "bundle.json"
            bundle_path.write_text('{"schema_version":"model_run_bundle_v1"}', encoding="utf-8")
            context = ModelExecutionContext(
                run_id=run_id,
                run_dir=settings.runs_dir / "_queued" / "stderr-test",
                request_bundle_path=bundle_path,
                artifact_policy={},
                event_log_path=base / "events.jsonl",
            )
            request = ModelExecutionRequest(run_id=run_id, request_payload={}, scenario_package={}, artifact_policy={}, run_profile="dev")

            result = SubprocessModelRuntime(settings, manifest).execute(request, context)

            self.assertEqual(result.run_id, run_id)
            events = RuntimeEventLog(base / "events.jsonl").read()
            self.assertTrue(any(row.get("type") == "stderr" for row in events))

    def test_subprocess_runtime_enforces_manifest_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = _build_settings(base)
            manifest = ModelRuntimeManifest(
                schema_version="model_runtime_manifest_v1",
                model_id="timeout-runtime",
                model_version="test",
                label="timeout runtime",
                description="",
                entrypoint_type="subprocess",
                entrypoint=[sys.executable, "-c", "import time; time.sleep(2)"],
                resource_requirements={"timeout_seconds": 0.2},
            )
            bundle_path = base / "bundle.json"
            bundle_path.write_text('{"schema_version":"model_run_bundle_v1"}', encoding="utf-8")
            context = ModelExecutionContext(
                run_id="timeout123",
                run_dir=settings.runs_dir / "_queued" / "timeout-test",
                request_bundle_path=bundle_path,
                artifact_policy={},
                event_log_path=base / "timeout-events.jsonl",
            )
            request = ModelExecutionRequest(run_id="timeout123", request_payload={}, scenario_package={}, artifact_policy={}, run_profile="dev")

            with self.assertRaisesRegex(RuntimeError, "exceeded timeout"):
                SubprocessModelRuntime(settings, manifest).execute(request, context)


class BackendHandoffSmokeToolTests(unittest.TestCase):
    def test_handoff_smoke_request_shape_and_runtime_helpers(self):
        args = backend_handoff_smoke.parse_args(
            [
                "--base-url",
                "http://127.0.0.1:8000/",
                "--user-id",
                "energy_planner",
                "--energy-scenario-key",
                "new_links",
                "--mrio-scenario-id",
                "S1",
                "--target-year",
                "2040",
                "--run-profile",
                "dev",
                "--carbon-price-usd-per-tco2",
                "25",
            ]
        )
        req = backend_handoff_smoke._run_request("project-123", args)
        self.assertEqual(req["model_architecture_id"], "energy-only")
        self.assertEqual(req["energy_model_engine"], "calliope")
        self.assertEqual(req["scenario"]["energy_scenario_key"], "new_links")
        self.assertEqual(req["scenario"]["target_scenario_id"], "S1")
        self.assertEqual(req["scenario"]["target_year"], 2040)
        self.assertEqual(req["run_profile"], "dev")
        self.assertNotIn("strict_validation", req)
        self.assertNotIn("allow_placeholder_data", req)
        self.assertEqual(req["levers"]["carbon_price_usd_per_tco2"], 25.0)
        self.assertEqual(args.timeout_seconds, backend_handoff_smoke.DEFAULT_RUN_TIMEOUT_SECONDS)
        self.assertEqual(
            backend_handoff_smoke._absolute_url("http://127.0.0.1:8000", "/api/projects"),
            "http://127.0.0.1:8000/api/projects",
        )

    def test_handoff_smoke_timeout_guidance_identifies_solver_timeout(self):
        guidance = backend_handoff_smoke._timeout_guidance({"status": "running", "stage": "solve_energy"})
        self.assertIn("real energy solver", guidance)
        self.assertIn("--timeout-seconds 900", guidance)

    def test_handoff_smoke_can_request_full_energy_development_architecture(self):
        args = backend_handoff_smoke.parse_args(["--model-architecture-id", "energy-development"])
        req = backend_handoff_smoke._run_request("project-123", args)
        self.assertEqual(req["model_architecture_id"], "energy-development")


class MetadataTests(unittest.TestCase):
    def test_generic_stage_orchestrator_runs_ordered_stages_without_model_knowledge(self):
        events = []
        orchestrator = StageOrchestrator(
            emit_progress=lambda stage, progress, message: events.append(("progress", stage, progress, message)),
            check_cancel=lambda: events.append(("cancel_check",)),
        )

        orchestrator.run(
            {"value": 2},
            [
                ModelStage(
                    stage_id="first",
                    label="First stage",
                    start_progress=0.2,
                    start_message="Starting first",
                    handler=lambda context: events.append(("handler", "first", context["value"])),
                ),
                ModelStage(
                    stage_id="second",
                    label="Second stage",
                    handler=lambda context: events.append(("handler", "second", context["value"] * 2)),
                ),
            ],
        )

        self.assertEqual(
            events,
            [
                ("cancel_check",),
                ("progress", "first", 0.2, "Starting first"),
                ("handler", "first", 2),
                ("cancel_check",),
                ("cancel_check",),
                ("handler", "second", 4),
                ("cancel_check",),
            ],
        )

    def test_model_runtime_entrypoint_delegates_to_edim_pipeline(self):
        from model_runtime.edim_model.core import runner as core_runner

        expected = ("run-1", {"ok": True}, [], Path("/tmp/run-1"))
        with patch("model_runtime.edim_model.core.edim_pipeline.run_edim_pipeline", return_value=expected) as mocked:
            result = core_runner.run_model_synchronously(settings=object(), req=object())

        self.assertEqual(result, expected)
        mocked.assert_called_once()

    def test_model_runtime_entrypoint_does_not_expose_model_specific_aliases(self):
        from model_runtime.edim_model.core import runner as core_runner
        from model_runtime.edim_model import local_runtime

        self.assertFalse(hasattr(core_runner, "run_calliope_synchronously"))
        self.assertFalse(hasattr(local_runtime, "run_local_model"))

    def test_model_architecture_catalog_matches_runtime_contracts(self):
        repo_root = Path(__file__).resolve().parents[2]
        model_manifest = json.loads(
            (repo_root / "model_runtime" / "edim_model" / "model_manifest.json").read_text(encoding="utf-8")
        )
        catalog_path = Path(str(model_manifest.get("architecture_catalog_path") or ""))
        if not catalog_path.is_absolute():
            catalog_path = repo_root / catalog_path
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        architectures = catalog.get("architectures") if isinstance(catalog.get("architectures"), list) else []
        self.assertTrue(architectures)
        architecture_ids = {row.get("id") for row in architectures if isinstance(row, dict)}
        self.assertIn(catalog.get("defaultArchitectureId"), architecture_ids)
        supported = set(model_manifest.get("supported_model_architectures") or [])
        self.assertTrue(architecture_ids.issubset(supported))

        module_catalog = {row["module_id"]: row for row in model_module_catalog()}
        manifest_modules = {row["module_id"]: row for row in model_manifest.get("modules") or []}
        self.assertTrue(set(module_catalog).issubset(set(manifest_modules)))
        self.assertEqual(module_catalog["calliope"]["implementation_status"], "ready")
        self.assertEqual(
            module_catalog["calliope"]["asset_root"],
            "model_runtime/model_modules/calliope/Calliope-Africa-main",
        )
        self.assertTrue((repo_root / module_catalog["calliope"]["asset_root"]).exists())
        self.assertEqual(module_catalog["osemosys"]["implementation_status"], "planned")
        self.assertEqual(module_catalog["osemosys"].get("supported_engines"), [])
        self.assertEqual(module_catalog["mrio"]["kind"], "development")
        self.assertEqual(set(model_manifest.get("supported_energy_model_engines") or []), {"calliope"})
        for row in model_manifest.get("modules") or []:
            self.assertTrue(row.get("scenario_channels"), row.get("module_id"))

        artifact_ids = set(load_artifact_manifest({}).keys())
        for architecture in architectures:
            box_ids = {box.get("id") for box in architecture.get("boxes", []) if isinstance(box, dict)}
            graph = architecture.get("graph") if isinstance(architecture.get("graph"), dict) else {}
            node_ids = {node.get("id") for node in graph.get("nodes", []) if isinstance(node, dict)}
            self.assertTrue(node_ids.issubset(box_ids))
            for edge in graph.get("edges", []) or []:
                self.assertIn(edge.get("from"), node_ids)
                self.assertIn(edge.get("to"), node_ids)
            for artifact in architecture.get("outputArtifacts", []) or []:
                self.assertIn(artifact.get("key"), artifact_ids)

    def test_module_scenario_catalog_declares_module_owned_channels(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(__file__).resolve().parents[2]
            settings = replace(
                _build_settings(Path(tmp)),
                config_dir=repo_root / "inputs",
                calliope_root=_repo_calliope_root(repo_root),
            )
            manifest = load_model_runtime_manifest(repo_root / "model_runtime" / "edim_model" / "model_manifest.json").to_dict()
            catalog = module_scenario_catalog(settings, manifest)
            self.assertEqual(catalog["schema_version"], "model_scenario_catalog")
            module_ids = {row["module_id"] for row in catalog["module_configurations"]}
            self.assertIn("calliope", module_ids)
            self.assertIn("osemosys", module_ids)
            self.assertIn("mrio", module_ids)
            channels = {row["config_key"]: row for row in catalog["scenario_channels"]}
            self.assertEqual(channels["scenario.energy_scenario_key"]["module_id"], "calliope")
            self.assertEqual(channels["scenario.target_scenario_id"]["module_id"], "mrio")
            self.assertEqual({row["value"] for row in channels["energy_model_engine"]["options"]}, {"calliope", "osemosys"})
            self.assertTrue(next(row for row in channels["energy_model_engine"]["options"] if row["value"] == "osemosys").get("disabled"))
            self.assertEqual(catalog["defaults"]["energy_model_engine"], "calliope")
            self.assertEqual(catalog["defaults"]["target_scenario_id"], "S2")

    def test_model_module_registry_rejects_unknown_and_marks_planned_energy_engine(self):
        manifest = load_model_runtime_manifest(
            Path(__file__).resolve().parents[2] / "model_runtime" / "edim_model" / "model_manifest.json"
        )
        issues = validate_request_against_manifest(
            {
                "energy_model_engine": "osemosys",
                "model_architecture_id": "energy-only",
            },
            manifest,
        )
        self.assertTrue([issue for issue in issues if "energy_model_engine" in issue])
        planned_module = get_energy_model_module("osemosys")
        self.assertEqual(planned_module.info.implementation_status, "planned")
        with self.assertRaisesRegex(ModelModuleError, "OSeMOSYS is registered as a planned energy module"):
            planned_module.resolve_model_definition(Path("."))
        with self.assertRaisesRegex(ModelModuleError, "Unknown energy model engine"):
            get_energy_model_module("unknown")

    def test_scenario_metadata_parses_tags_and_templates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "scenario_metadata.csv"
            path.write_text(
                "\n".join(
                    [
                        "key,title,description,tags,policy_question,expected_tradeoff,user_label,demand_multiplier,renewables_capex_multiplier,fossil_fuel_price_multiplier,carbon_price_usd_per_tco2",
                        "s1,Scenario 1,,policy,,,,1.0,1.0,1.0,40",
                        "s2,Scenario 2,,ndc|2040,,,,1.0,1.0,1.0,",
                    ]
                ),
                encoding="utf-8",
            )
            data = load_scenario_metadata(path)
            self.assertEqual(data["s1"].tags, ["policy"])
            self.assertEqual(data["s2"].tags, ["ndc", "2040"])
            self.assertEqual(data["s1"].preset_levers["carbon_price_usd_per_tco2"], 40.0)


class EnvironmentSetupTests(unittest.TestCase):
    def test_environment_setup_reports_ready_when_required_inputs_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            _seed_mario_inputs(settings.config_dir)
            report = build_environment_setup_report(
                settings=settings,
                queue_stats={"capacity": 3, "active_jobs": 1},
                energy_scenario_key="",
                run_profile="dev",
            )
            self.assertTrue(report["ok"])
            self.assertIn(report["solver_resolved"], {"highs", "appsi_highs"})

    def test_environment_setup_reports_not_ready_on_missing_model_and_full_queue(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            (settings.calliope_root / "model.yaml").unlink()
            report = build_environment_setup_report(
                settings=settings,
                queue_stats={"capacity": 1, "active_jobs": 1},
                energy_scenario_key="",
                run_profile="dev",
            )
            self.assertFalse(report["ok"])
            statuses = {(row["name"], row["status"]) for row in report.get("checks", [])}
            self.assertIn(("calliope_model", "error"), statuses)
            self.assertIn(("queue_capacity", "warn"), statuses)

    def test_environment_setup_mario_mode_requires_mario_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base_settings = _build_settings(Path(tmp))
            settings = replace(base_settings, development_engine="mario")
            report = build_environment_setup_report(
                settings=settings,
                queue_stats={"capacity": 2, "active_jobs": 0},
                energy_scenario_key="",
                run_profile="dev",
            )
            self.assertFalse(report["ok"])
            statuses = {(row["name"], row["status"]) for row in report.get("checks", [])}
            self.assertIn(("mario_inputs", "error"), statuses)

    def test_environment_setup_strict_mode_rejects_placeholder_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = replace(_build_settings(base), development_engine="mario")
            _seed_mario_inputs(settings.config_dir)
            (settings.config_dir / "mario_inputs" / "employment_intensity.csv").write_text(
                "mario_region,mario_sector,jobs_per_musd_direct,jobs_per_musd_total,reference_year,source,notes\nEast_Africa,Gas_supply_chain,4.0,7.0,2019,placeholder,replace with calibrated estimate\n",
                encoding="utf-8",
            )
            report = build_environment_setup_report(
                settings=settings,
                queue_stats={"capacity": 2, "active_jobs": 0},
                energy_scenario_key="new_links",
                run_profile="dev",
                strict_validation=True,
            )
            self.assertFalse(report["ok"])
            statuses = {(row["name"], row["status"]) for row in report.get("checks", [])}
            self.assertIn(("mario_placeholder_inputs", "error"), statuses)

    def test_environment_setup_can_allow_placeholder_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = replace(_build_settings(base), development_engine="mario")
            _seed_mario_inputs(settings.config_dir)
            (settings.config_dir / "mario_inputs" / "employment_intensity.csv").write_text(
                "mario_region,mario_sector,jobs_per_musd_direct,jobs_per_musd_total,reference_year,source,notes\nEast_Africa,Gas_supply_chain,4.0,7.0,2019,seeded_placeholder,seeded placeholder estimate\n",
                encoding="utf-8",
            )
            report = build_environment_setup_report(
                settings=settings,
                queue_stats={"capacity": 2, "active_jobs": 0},
                energy_scenario_key="new_links",
                run_profile="dev",
                strict_validation=True,
                allow_placeholder_data=True,
            )
            self.assertTrue(report["ok"])
            statuses = {(row["name"], row["status"]) for row in report.get("checks", [])}
            self.assertIn(("mario_placeholder_inputs", "warn"), statuses)


class MarioRuntimeTests(unittest.TestCase):
    def test_mario_inputs_health_detects_missing_and_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            health_missing = mario_inputs_health(config_dir)
            self.assertFalse(health_missing["ok"])
            _seed_mario_inputs(config_dir)
            health_present = mario_inputs_health(config_dir)
            self.assertTrue(health_present["ok"])
            self.assertTrue(health_present["expert_inputs_ready"])

    def test_mario_inputs_health_detects_seeded_placeholder_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            _seed_mario_inputs(config_dir)
            (config_dir / "mario_inputs" / "scenario_assumptions.csv").write_text(
                "assumption_key,scenario_key,value,unit,effective_year,source,notes\ncarbon_price,new_links,10,usd_per_tco2,2019,seeded_placeholder,seeded placeholder carbon path\n",
                encoding="utf-8",
            )
            health = mario_inputs_health(config_dir)
            self.assertIn("scenario_assumptions.csv", health["placeholder_files"])

    def test_run_mario_io_runtime_produces_development_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_dir = base / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            _seed_mario_inputs(config_dir)

            exchange_dir = base / "exchange"
            exchange_dir.mkdir(parents=True, exist_ok=True)
            (exchange_dir / "investment_shocks.csv").write_text(
                "\n".join(
                    [
                        "run_id,scenario,year,region,location,technology,mario_sector,shock_channel,tech_group,shock_value_musd",
                        "run1,new_links,2019,East_Africa,EGY,PV_New,Electrical_equipment,capex,VRE,10.0",
                    ]
                ),
                encoding="utf-8",
            )
            (exchange_dir / "operating_shocks.csv").write_text(
                "\n".join(
                    [
                        "run_id,scenario,year,region,location,technology,mario_sector,shock_channel,tech_group,shock_value_musd",
                        "run1,new_links,2019,East_Africa,EGY,CCGT_pp,Gas_supply_chain,opex,Fossil,5.0",
                        "run1,new_links,2019,East_Africa,EGY,CCGT_pp,Maintenance_services,opex,Fossil,2.0",
                    ]
                ),
                encoding="utf-8",
            )

            development, runtime_meta, warnings = run_mario_io_runtime(
                exchange_dir=exchange_dir,
                config_dir=config_dir,
                run_id="run1",
                scenario="new_links",
                year=2019,
            )
            self.assertEqual(development["method"], "mario_io_runtime_v1")
            self.assertGreater(development["totals"]["jobs_total"], 0.0)
            self.assertTrue(development["by_region"]["records"])
            self.assertTrue(runtime_meta["mario_runtime_executed"])
            self.assertEqual(warnings, [])

    def test_run_mario_io_runtime_returns_zero_payload_when_shocks_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_dir = base / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            _seed_mario_inputs(config_dir)

            exchange_dir = base / "exchange"
            exchange_dir.mkdir(parents=True, exist_ok=True)
            header = "run_id,scenario,year,region,location,technology,mario_sector,shock_channel,tech_group,shock_value_musd\n"
            (exchange_dir / "investment_shocks.csv").write_text(header, encoding="utf-8")
            (exchange_dir / "operating_shocks.csv").write_text(header, encoding="utf-8")

            development, runtime_meta, warnings = run_mario_io_runtime(
                exchange_dir=exchange_dir,
                config_dir=config_dir,
                run_id="run-empty",
                scenario="new_links",
                year=2019,
            )
            self.assertEqual(development["totals"]["jobs_total"], 0.0)
            self.assertEqual(runtime_meta["shock_record_count"], 0)
            self.assertTrue(runtime_meta["mario_runtime_executed"])
            self.assertTrue(any("no exchange shock rows" in w.lower() for w in warnings))

    def test_exchange_builder_requires_tech_level_cost_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = _build_settings(base)
            _seed_mario_inputs(settings.config_dir)
            run_dir = settings.runs_dir / "abcd1234"
            run_dir.mkdir(parents=True, exist_ok=True)
            req = RunRequest(energy_scenario_key="new_links", mrio_scenario_id="ZA-S2", target_year=2030, run_profile="dev", strict_validation=False, allow_placeholder_data=True, levers=LeverValues())

            class _NoCostModel:
                results = {}

            summary = {
                "system_cost": {"records": [{"costs": "monetary", "value": 2_500_000.0}]},
                "summary_diagnostics": {
                    "reliability": {"demand_total": 1000.0, "unserved_total": 0.0},
                    "trade_matrix": {},
                    "cost_decomposition": {"component_records": [], "class_totals": {"records": []}},
                },
            }

            with self.assertRaisesRegex(RuntimeError, "no tech-level monetary component rows"):
                _write_exchange_files_for_mario(
                    model=_NoCostModel(),
                    settings=settings,
                    req=req,
                    run_id="abcd1234",
                    run_dir=run_dir,
                    summary_diagnostics=summary["summary_diagnostics"],
                    summary=summary,
                )

    def test_exchange_builder_uses_cost_var_for_operating_shocks_when_om_components_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = _build_settings(base)
            _seed_mario_inputs(settings.config_dir)
            run_dir = settings.runs_dir / "efgh5678"
            run_dir.mkdir(parents=True, exist_ok=True)
            req = RunRequest(energy_scenario_key="new_links", mrio_scenario_id="ZA-S2", target_year=2030, run_profile="dev", strict_validation=False, allow_placeholder_data=True, levers=LeverValues())

            import pandas as pd

            class _FakeDA:
                def __init__(self, rows):
                    self._rows = rows

                def to_dataframe(self, name="value"):
                    return pd.DataFrame(self._rows)

            class _FakeModel:
                def __init__(self):
                    self.results = {
                        "cost_investment": _FakeDA(
                            [
                                {
                                    "costs": "monetary",
                                    "loc_techs_investment_cost": "EGY::PV_New",
                                    "value": 2_000_000.0,
                                }
                            ]
                        ),
                        "cost_var": _FakeDA(
                            [
                                {
                                    "timesteps": "2019-01-01 00:00:00",
                                    "costs": "monetary",
                                    "loc_techs_om_cost": "EGY::CCGT_pp",
                                    "value": 7_000_000.0,
                                },
                                {
                                    "timesteps": "2019-01-01 00:00:00",
                                    "costs": "monetary",
                                    "loc_techs_om_cost": "EGY::PV_New",
                                    "value": 3_000_000.0,
                                },
                            ]
                        ),
                    }

            summary = {
                "system_cost": {"records": [{"costs": "monetary", "value": 12_000_000.0}]},
                "summary_diagnostics": {
                    "reliability": {"demand_total": 1000.0, "unserved_total": 0.0},
                    "trade_matrix": {},
                    "cost_decomposition": {"component_records": [], "class_totals": {"records": []}},
                },
            }

            _, shock_meta, warnings = _write_exchange_files_for_mario(
                model=_FakeModel(),
                settings=settings,
                req=req,
                run_id="efgh5678",
                run_dir=run_dir,
                summary_diagnostics=summary["summary_diagnostics"],
                summary=summary,
            )
            self.assertGreater(shock_meta["operating_rows"], 0)
            source_map = shock_meta.get("source_variable_by_component") or {}
            self.assertEqual(source_map.get("variable_con"), "cost_var")
            self.assertEqual(source_map.get("variable_prod"), "cost_var")
            self.assertFalse(any("cost_var" in w for w in warnings))

    def test_mario_engine_raises_on_runtime_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(Path(tmp))
            settings = replace(settings, development_engine="mario", mario_fail_on_error=False)
            req = RunRequest(energy_scenario_key="new_links", mrio_scenario_id="ZA-S2", target_year=2030, run_profile="dev", strict_validation=False, allow_placeholder_data=True, levers=LeverValues())
            summary = {"summary_diagnostics": {}, "warnings": []}
            with patch("model_runtime.edim_model.core.runner._build_mario_development_outputs", side_effect=RuntimeError("boom")):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    out = _build_development_outputs(
                        settings=settings,
                        model=object(),
                        summary=summary,
                        req=req,
                        run_id="x",
                        run_dir=Path(tmp),
                        development_model_config={},
                        mapping_quality={},
                        scenario_package=_scenario_package(),
                    )


if __name__ == "__main__":
    unittest.main()
