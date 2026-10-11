from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

try:
    from azure.communication.email import EmailClient

    _EMAIL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _EMAIL_AVAILABLE = False


def is_configured(settings) -> bool:
    return bool(
        _EMAIL_AVAILABLE
        and str(getattr(settings, "auth_email_connection_string", "") or "").strip()
        and str(getattr(settings, "auth_email_sender", "") or "").strip()
    )


def _send_blocking(connection_string: str, sender: str, recipient: str, subject: str, plain: str, html: str) -> None:
    with EmailClient.from_connection_string(connection_string) as client:
        message = {
            "content": {"subject": subject, "plainText": plain, "html": html},
            "recipients": {"to": [{"address": recipient}]},
            "senderAddress": sender,
        }
        poller = client.begin_send(message)
        poller.result()


async def send_reset_email(settings, recipient: str, reset_link: str) -> bool:
    if not is_configured(settings):
        return False
    subject = "Reset your EDIM password"
    plain = f"Reset your password with this link:\n\n{reset_link}\n\nThis link expires soon."
    html = f"<p>Reset your password with this link:</p><p><a href=\"{reset_link}\">{reset_link}</a></p><p>This link expires soon.</p>"
    try:
        await asyncio.to_thread(
            _send_blocking,
            str(settings.auth_email_connection_string),
            str(settings.auth_email_sender),
            recipient,
            subject,
            plain,
            html,
        )
    except Exception as exc:
        logger.warning("auth.email reset send failed recipient=%s (%s)", recipient, exc)
        return False
    logger.info("auth.email reset sent recipient=%s", recipient)
    return True
