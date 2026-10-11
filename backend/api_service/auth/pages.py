from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi_users import exceptions

from .backends import auth_backend, resolve_session_context
from .db import _bind
from .invites import consume_invite, mint_invite, parse_invite, register_invite
from .manager import get_user_manager
from .models import AuthInvite, AuthUser
from .schemas import UserCreate, UserUpdate

logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


def _render(request: Request, name: str, *, status_code: int = 200, **context):
    return templates.TemplateResponse(request, name, context, status_code=status_code)


def _notice(kind: str, text: str) -> dict:
    return {"kind": kind, "text": text}


def _copy_cookies(target, source) -> None:
    for name, value in source.raw_headers:
        if name.lower() == b"set-cookie":
            target.headers.append("set-cookie", value.decode("latin-1"))


async def _guard(request: Request):
    context = await resolve_session_context(request)
    if context is None:
        return None, RedirectResponse(url="/auth/login", status_code=303)
    return context, None


async def _guard_admin(request: Request):
    context, denied = await _guard(request)
    if denied is not None:
        return None, denied
    if not context.is_admin:
        return None, _render(
            request,
            "notice.html",
            status_code=403,
            heading="Not allowed",
            notice=_notice("error", "Administrator role required."),
            link_href="/ui/",
            link_text="Go to the workspace",
        )
    return context, None


def _safe_next(value: str | None, default: str = "/ui/") -> str:
    text = str(value or "").strip()
    if text.startswith("/") and not text.startswith("//") and "\\" not in text:
        return text
    return default


router = APIRouter(include_in_schema=False)


@router.get("/auth/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/ui/"):
    target = _safe_next(next)
    if await resolve_session_context(request) is not None:
        return RedirectResponse(url=target, status_code=303)
    return _render(request, "login.html", next_url=target, email="", notice=None)


@router.post("/auth/login")
async def login_submit(
    request: Request,
    email: str = Form(""),
    password: str = Form(""),
    next: str = Form("/ui/"),
    user_manager=Depends(get_user_manager),
):
    target = _safe_next(next)
    user = await user_manager.authenticate(SimpleNamespace(username=email.strip(), password=password))
    if user is None or not user.is_active:
        return _render(
            request,
            "login.html",
            status_code=401,
            next_url=target,
            email=email,
            notice=_notice("error", "Incorrect e-mail or password."),
        )
    backend = auth_backend()
    response = RedirectResponse(url=target, status_code=303)
    _copy_cookies(response, await backend.login(backend.get_strategy(), user))
    return response


@router.api_route("/auth/logout", methods=["GET", "POST"])
async def logout(request: Request, user_manager=Depends(get_user_manager)):
    context = await resolve_session_context(request)
    response = RedirectResponse(url="/auth/login", status_code=303)
    if context is not None:
        backend = auth_backend()
        user = await _load_user(user_manager, context.user_id)
        if user is not None:
            token = await backend.transport.scheme(request)
            _copy_cookies(response, await backend.logout(backend.get_strategy(), user, str(token)))
    return response


async def _load_user(user_manager, user_id: str) -> AuthUser | None:
    stmt = _select_by_user_id(user_id)
    rows = (await user_manager.user_db.session.execute(stmt)).scalars().all()
    return rows[0] if rows else None


def _select_by_user_id(user_id: str):
    from sqlalchemy import select

    return select(AuthUser).where(AuthUser.user_id == user_id)





@router.get("/auth/change-password", response_class=HTMLResponse)
async def change_password_page(request: Request):
    context, denied = await _guard(request)
    if denied is not None:
        return denied
    return _render(
        request,
        "change_password.html",
        subtitle=f"Signed in as {context.email or context.user_id}.",
        notice=None,
    )


@router.post("/auth/change-password")
async def change_password_submit(
    request: Request,
    current: str = Form(""),
    new: str = Form(""),
    confirm: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    context, denied = await _guard(request)
    if denied is not None:
        return denied
    subtitle = f"Signed in as {context.email or context.user_id}."
    if new != confirm:
        return _render(
            request, "change_password.html", status_code=400, subtitle=subtitle,
            notice=_notice("error", "The two passwords do not match."),
        )
    user = await _load_user(user_manager, context.user_id)
    if user is None:
        return RedirectResponse(url="/auth/login", status_code=303)
    valid, _updated = user_manager.password_helper.verify_and_update(current, user.hashed_password)
    if not valid:
        return _render(
            request, "change_password.html", status_code=401, subtitle=subtitle,
            notice=_notice("error", "The current password is not correct."),
        )
    try:
        await user_manager.update(UserUpdate(password=new), user, safe=False, request=request)
    except exceptions.InvalidPasswordException as exc:
        return _render(
            request, "change_password.html", status_code=400, subtitle=subtitle,
            notice=_notice("error", exc.reason),
        )
    return RedirectResponse(url="/ui/", status_code=303)


@router.get("/auth/forgot-password", response_class=HTMLResponse)
async def forgot_password_page(request: Request):
    return _render(request, "forgot_password.html", email="", notice=None, reset_link=None)


@router.post("/auth/forgot-password")
async def forgot_password_submit(
    request: Request,
    email: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    generic = _notice("success", "If that address is registered, a reset link has been generated.")
    try:
        user = await user_manager.get_by_email(email.strip())
    except exceptions.UserNotExists:
        return _render(request, "forgot_password.html", email=email, notice=generic, reset_link=None)
    try:
        await user_manager.forgot_password(user, request)
    except exceptions.UserInactive:
        return _render(request, "forgot_password.html", email=email, notice=generic, reset_link=None)

    pending = getattr(request.app.state, "auth_pending_tokens", None) or {}
    record = pending.get(str(user.id))
    reset_link = None
    notice = generic
    sent_flags = getattr(request.app.state, "auth_email_last_sent", None) or {}
    email_sent = bool(sent_flags.get(str(user.id)))
    if record and not email_sent and getattr(request.app.state, "auth_dev_token_display", False):
        reset_link = f"{_origin(request)}/auth/reset-password?token={record['token']}"
        notice = _notice(
            "info",
            "E-mail delivery is not configured on this deployment, so the reset link is shown here.",
        )
    return _render(request, "forgot_password.html", email=email, notice=notice, reset_link=reset_link)


def _origin(request: Request) -> str:
    return str(request.base_url).rstrip("/")


@router.get("/auth/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request, token: str = ""):
    return _render(request, "reset_password.html", token=token, notice=None)


@router.post("/auth/reset-password")
async def reset_password_submit(
    request: Request,
    token: str = Form(""),
    password: str = Form(""),
    confirm: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    expired = _notice("error", "This reset link is invalid or has expired.")
    if password != confirm:
        return _render(
            request, "reset_password.html", status_code=400, token=token,
            notice=_notice("error", "The two passwords do not match."),
        )
    try:
        await user_manager.reset_password(token, password, request)
    except (exceptions.InvalidResetPasswordToken, exceptions.UserNotExists):
        return _render(request, "reset_password.html", status_code=400, token="", notice=expired)
    except exceptions.InvalidPasswordException as exc:
        return _render(
            request, "reset_password.html", status_code=400, token=token,
            notice=_notice("error", exc.reason),
        )
    return _render(
        request,
        "notice.html",
        heading="Password updated",
        notice=_notice("success", "Your password has been updated. You can now sign in."),
        link_href="/auth/login",
        link_text="Go to sign in",
    )


@router.get("/auth/invite", response_class=HTMLResponse)
async def invite_page(request: Request, token: str = ""):
    claims = parse_invite(token, request.app.state.settings.auth_session_secret)
    if not claims:
        return _render(
            request, "invite.html", status_code=400, token="", email="",
            notice=_notice("error", "This invitation link is invalid or has expired."),
        )
    return _render(request, "invite.html", token=token, email=str(claims.get("email") or ""), notice=None)


@router.post("/auth/invite")
async def invite_submit(
    request: Request,
    token: str = Form(""),
    password: str = Form(""),
    confirm: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    expired = _notice("error", "This invitation link is invalid or has expired.")
    settings = request.app.state.settings
    claims = parse_invite(token, settings.auth_session_secret)
    if not claims:
        return _render(request, "invite.html", status_code=400, token="", email="", notice=expired)
    email = str(claims.get("email") or "")
    if password != confirm:
        return _render(
            request, "invite.html", status_code=400, token=token, email=email,
            notice=_notice("error", "The two passwords do not match."),
        )
    user = await _load_user_by_auth_id(user_manager, str(claims.get("sub") or ""))
    if user is None:
        return _render(request, "invite.html", status_code=400, token="", email="", notice=expired)
    _engine, factory = _bind(settings)
    async with factory() as session:
        if not await consume_invite(session, auth_user_id=str(user.id), token=token):
            return _render(request, "invite.html", status_code=400, token="", email="", notice=expired)
    try:
        await user_manager.update(
            UserUpdate(password=password, is_active=True, is_verified=True),
            user,
            safe=False,
            request=request,
        )
    except exceptions.InvalidPasswordException as exc:
        return _render(
            request, "invite.html", status_code=400, token=token, email=email,
            notice=_notice("error", exc.reason),
        )
    backend = auth_backend()
    response = RedirectResponse(url="/ui/", status_code=303)
    _copy_cookies(response, await backend.login(backend.get_strategy(), user))
    return response


async def _load_user_by_auth_id(user_manager, auth_user_id: str) -> AuthUser | None:
    from sqlalchemy import select

    stmt = select(AuthUser).where(AuthUser.id == auth_user_id)
    rows = (await user_manager.user_db.session.execute(stmt)).scalars().all()
    return rows[0] if rows else None


@router.get("/auth/admin/invites", response_class=HTMLResponse)
async def admin_invites_page(request: Request):
    _context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    return _render(
        request, "admin_invite.html", wide=True, notice=None, invite_link=None,
    )








@router.post("/auth/admin/invites")
async def admin_invites_submit(
    request: Request,
    user_id: str = Form(""),
    email: str = Form(""),
    display_name: str = Form(""),
    organization: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    _context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    settings = request.app.state.settings
    clean_id = str(user_id or "").strip()
    clean_email = str(email or "").strip()
    if not clean_id or not clean_email:
        return _render(
            request, "admin_invite.html", wide=True, status_code=400, invite_link=None,
            notice=_notice("error", "User id and e-mail are required."),
        )
    try:
        user = await user_manager.create(
            UserCreate(
                user_id=clean_id,
                email=clean_email,
                password=user_manager.password_helper.generate(),
                display_name=str(display_name or "").strip() or clean_id,
                organization=str(organization or "").strip(),
                is_active=False,
                is_verified=True,
            ),
            safe=False,
            request=request,
        )
    except exceptions.UserAlreadyExists:
        return _render(
            request, "admin_invite.html", wide=True, status_code=400, invite_link=None,
            notice=_notice("error", "A user with that e-mail already exists."),
        )
    except exceptions.InvalidPasswordException as exc:
        return _render(
            request, "admin_invite.html", wide=True, status_code=400, invite_link=None,
            notice=_notice("error", exc.reason),
        )
    token = mint_invite(
        auth_user_id=str(user.id),
        email=clean_email,
        secret=settings.auth_session_secret,
        lifetime_seconds=settings.auth_invite_ttl_seconds,
    )
    _engine, factory = _bind(settings)
    async with factory() as session:
        await register_invite(
            session,
            auth_user_id=str(user.id),
            email=clean_email,
            token=token,
            lifetime_seconds=settings.auth_invite_ttl_seconds,
        )
    return _render(
        request,
        "admin_invite.html",
        wide=True,
        invite_link=f"{_origin(request)}/auth/invite?token={token}",
        notice=_notice("success", f"Invitation created for {clean_email}. Share this link out of band."),
    )


def _user_form(user_id: str = "", email: str = "", display_name: str = "", organization: str = "") -> dict:
    return {
        "user_id": user_id,
        "email": email,
        "display_name": display_name,
        "organization": organization,
    }


def _user_row(user: AuthUser) -> dict:
    return {
        "id": str(user.id),
        "user_id": str(user.user_id),
        "email": str(user.email),
        "display_name": str(user.display_name or ""),
        "organization": str(user.organization or ""),
        "is_active": bool(user.is_active),
        "is_superuser": bool(user.is_superuser),
    }


async def _users_page(
    request: Request,
    user_manager,
    context,
    *,
    status_code: int = 200,
    notice: dict | None = None,
    form: dict | None = None,
    generated_password: str | None = None,
    open_modal: str = "",
    open_modal_user: dict | None = None,
):
    from sqlalchemy import select

    rows = (
        await user_manager.user_db.session.execute(select(AuthUser).order_by(AuthUser.user_id))
    ).scalars().all()
    return _render(
        request,
        "admin_users.html",
        status_code=status_code,
        full=True,
        subtitle=f"Signed in as {context.email or context.user_id}.",
        notice=notice,
        users=[_user_row(row) for row in rows],
        form=form or _user_form(),
        generated_password=generated_password,
        open_modal=open_modal,
        open_modal_user=open_modal_user,
    )


@router.get("/auth/admin/users", response_class=HTMLResponse)
async def admin_users_page(request: Request, user_manager=Depends(get_user_manager)):
    context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    return await _users_page(request, user_manager, context)


@router.post("/auth/admin/users")
async def admin_users_create(
    request: Request,
    user_id: str = Form(""),
    email: str = Form(""),
    display_name: str = Form(""),
    organization: str = Form(""),
    password: str = Form(""),
    is_superuser: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    form = _user_form(user_id, email, display_name, organization)
    clean_id = user_id.strip()
    clean_email = email.strip()
    if not clean_id or not clean_email:
        return await _users_page(
            request, user_manager, context, status_code=400, form=form, open_modal="create-user-modal",
            notice=_notice("error", "User id and e-mail are required."),
        )
    generated = password or user_manager.password_helper.generate()
    try:
        await user_manager.create(
            UserCreate(
                user_id=clean_id,
                email=clean_email,
                password=generated,
                display_name=display_name.strip() or clean_id,
                organization=organization.strip(),
                is_active=True,
                is_verified=True,
                is_superuser=bool(is_superuser),
            ),
            safe=False,
            request=request,
        )
    except exceptions.UserAlreadyExists:
        return await _users_page(
            request, user_manager, context, status_code=400, form=form, open_modal="create-user-modal",
            notice=_notice("error", "A user with that e-mail already exists."),
        )
    except exceptions.InvalidPasswordException as exc:
        return await _users_page(
            request, user_manager, context, status_code=400, form=form, open_modal="create-user-modal",
            notice=_notice("error", exc.reason),
        )
    return await _users_page(
        request,
        user_manager,
        context,
        notice=_notice("success", "User created." if password else "User created. Copy the one-time password below."),
        generated_password=None if password else generated,
    )


@router.post("/auth/admin/users/{auth_user_id}/password")
async def admin_users_set_password(
    request: Request,
    auth_user_id: str,
    password: str = Form(""),
    confirm: str = Form(""),
    user_manager=Depends(get_user_manager),
):
    context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    user = await _load_user_by_auth_id(user_manager, auth_user_id)
    if user is None:
        return await _users_page(request, user_manager, context, status_code=404,
                                notice=_notice("error", "User not found."))
    if not password:
        return await _users_page(
            request, user_manager, context, status_code=400,
            open_modal="set-password-modal", open_modal_user=_user_row(user),
            notice=_notice("error", "Enter a password."),
        )
    if password != confirm:
        return await _users_page(
            request, user_manager, context, status_code=400,
            open_modal="set-password-modal", open_modal_user=_user_row(user),
            notice=_notice("error", "The two passwords do not match."),
        )
    try:
        await user_manager.update(UserUpdate(password=password), user, safe=False, request=request)
    except exceptions.InvalidPasswordException as exc:
        return await _users_page(
            request, user_manager, context, status_code=400,
            open_modal="set-password-modal", open_modal_user=_user_row(user),
            notice=_notice("error", exc.reason),
        )
    return await _users_page(request, user_manager, context,
                            notice=_notice("success", f"Password updated for {user.user_id}."))


@router.post("/auth/admin/users/{auth_user_id}/active")
async def admin_users_toggle_active(
    request: Request,
    auth_user_id: str,
    user_manager=Depends(get_user_manager),
):
    context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    user = await _load_user_by_auth_id(user_manager, auth_user_id)
    if user is None:
        return await _users_page(request, user_manager, context, status_code=404,
                                notice=_notice("error", "User not found."))
    if user.user_id == context.user_id:
        return await _users_page(request, user_manager, context, status_code=400,
                                notice=_notice("error", "You cannot deactivate your own account."))
    await user_manager.update(UserUpdate(is_active=not user.is_active), user, safe=False, request=request)
    state = "activated" if user.is_active else "deactivated"
    return await _users_page(request, user_manager, context,
                            notice=_notice("success", f"{user.user_id} {state}."))


@router.post("/auth/admin/users/{auth_user_id}/delete")
async def admin_users_delete(
    request: Request,
    auth_user_id: str,
    user_manager=Depends(get_user_manager),
):
    context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    user = await _load_user_by_auth_id(user_manager, auth_user_id)
    if user is None:
        return await _users_page(request, user_manager, context, status_code=404,
                                notice=_notice("error", "User not found."))
    if user.user_id == context.user_id:
        return await _users_page(request, user_manager, context, status_code=400,
                                notice=_notice("error", "You cannot delete your own account."))
    removed = user.user_id
    _engine, factory = _bind(request.app.state.settings)
    async with factory() as session:
        from sqlalchemy import delete as sa_delete

        await session.execute(sa_delete(AuthInvite).where(AuthInvite.auth_user_id == str(user.id)))
        await session.commit()
    await user_manager.user_db.delete(user)
    return await _users_page(request, user_manager, context,
                            notice=_notice("success", f"{removed} deleted."))


@router.post("/auth/admin/users/{auth_user_id}/admin")
async def admin_users_toggle_admin(
    request: Request,
    auth_user_id: str,
    user_manager=Depends(get_user_manager),
):
    context, denied = await _guard_admin(request)
    if denied is not None:
        return denied
    user = await _load_user_by_auth_id(user_manager, auth_user_id)
    if user is None:
        return await _users_page(request, user_manager, context, status_code=404,
                                notice=_notice("error", "User not found."))
    if user.user_id == context.user_id:
        return await _users_page(request, user_manager, context, status_code=400,
                                notice=_notice("error", "You cannot change your own administrator role."))
    await user_manager.update(UserUpdate(is_superuser=not user.is_superuser), user, safe=False, request=request)
    state = "granted" if user.is_superuser else "revoked"
    return await _users_page(request, user_manager, context,
                            notice=_notice("success", f"{user.user_id}: administrator access {state}."))
