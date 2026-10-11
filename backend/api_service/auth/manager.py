from __future__ import annotations

import logging
from typing import Any, Union

from fastapi import Depends, Request
from fastapi_users import BaseUserManager, InvalidID, exceptions
from fastapi_users.db import SQLAlchemyUserDatabase

from ..settings import Settings
from .db import get_auth_session, get_user_db
from .models import AuthUser
from .schemas import UserCreate, UserUpdate

logger = logging.getLogger(__name__)


class StringIDMixin:
    def parse_id(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise InvalidID()
        return text


class EdimUserManager(StringIDMixin, BaseUserManager[AuthUser, str]):
    password_min_length: int = 10

    async def validate_password(self, password: str, user: Union[UserCreate, AuthUser]) -> None:
        if len(password) < self.password_min_length:
            raise exceptions.InvalidPasswordException(
                reason=f"Password must be at least {self.password_min_length} characters."
            )
        email = str(getattr(user, "email", "") or "").strip().lower()
        if email and email in password.lower():
            raise exceptions.InvalidPasswordException(
                reason="Password must not contain your e-mail address."
            )

    async def on_after_login(self, user: AuthUser, request: Request | None = None, response=None) -> None:
        logger.info("auth.login user_id=%s", user.user_id)

    async def on_after_register(self, user: AuthUser, request: Request | None = None) -> None:
        logger.info("auth.register user_id=%s", user.user_id)

    async def on_after_forgot_password(self, user: AuthUser, token: str, request: Request | None = None) -> None:
        logger.info("auth.forgot_password user_id=%s", user.user_id)
        _record_pending(request, user, "reset", token)
        await _maybe_send_reset_email(request, user, token)

    async def on_after_reset_password(self, user: AuthUser, request: Request | None = None) -> None:
        logger.info("auth.reset_password user_id=%s", user.user_id)

    async def on_after_update(self, user: AuthUser, update_dict: dict, request: Request | None = None) -> None:
        logger.info("auth.update user_id=%s fields=%s", user.user_id, sorted(update_dict))


def _record_pending(request: Request | None, user: AuthUser, kind: str, token: str) -> None:
    if request is None:
        return
    store = getattr(request.app.state, "auth_pending_tokens", None)
    if store is None:
        return
    store[str(user.id)] = {"kind": kind, "token": token, "user_id": user.user_id, "email": user.email}


async def _maybe_send_reset_email(request: Request | None, user: AuthUser, token: str) -> bool:
    if request is None:
        return False
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        return False
    from .email_sender import is_configured, send_reset_email

    if not is_configured(settings):
        return False
    base = str(getattr(settings, "auth_email_base_url", "") or "").strip().rstrip("/")
    if not base:
        base = str(request.base_url).rstrip("/")
    reset_link = f"{base}/auth/reset-password?token={token}"
    sent = await send_reset_email(settings, str(user.email or ""), reset_link)
    flags = getattr(request.app.state, "auth_email_last_sent", None)
    if flags is None:
        flags = {}
        request.app.state.auth_email_last_sent = flags
    flags[str(user.id)] = bool(sent)
    return sent


async def get_user_manager(
    user_db: SQLAlchemyUserDatabase = Depends(get_user_db),
    request: Request = None,
):
    settings: Settings = request.app.state.settings
    manager = EdimUserManager(user_db)
    manager.reset_password_token_secret = settings.auth_session_secret
    manager.verification_token_secret = settings.auth_session_secret
    manager.reset_password_token_lifetime_seconds = settings.auth_reset_ttl_seconds
    manager.password_min_length = settings.auth_password_min_length
    yield manager
