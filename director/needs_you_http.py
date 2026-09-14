"""Receiver-owned loopback HTTPS-proxy action page for Needs you links."""
from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import html
import threading
from typing import Mapping
from urllib.parse import parse_qs, quote, urlencode, urlparse

from .needs_you import NeedsYouError, NeedsYouHome


class NeedsYouActionServer:
    """One receiver-lifecycle HTTP thread; a user-managed proxy supplies TLS."""

    def __init__(self, home: NeedsYouHome, web_client: object, *, client_id: str, client_secret: str, wake) -> None:
        if home.target is None or home.target.action_url is None:
            raise ValueError("Needs-you action server requires action_url")
        self.home, self.web, self.client_id, self.client_secret, self.wake = home, web_client, client_id, client_secret, wake
        self.target = home.target
        self._server = ThreadingHTTPServer((self.target.http_bind_host, self.target.http_port), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, name="director-needs-you-http", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    @property
    def redirect_uri(self) -> str:
        return self.target.action_url + "/needs-you/oauth/callback"

    def _handler(self):
        action_server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "SlackAgentBridgeNeedsYou/1"

            def log_message(self, _format, *_args):
                return

            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def end_headers(self):
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header("Content-Security-Policy", "frame-ancestors 'none'")
                self.send_header("Referrer-Policy", "no-referrer")
                super().end_headers()

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path.startswith("/needs-you/action/"):
                    token = parsed.path.removeprefix("/needs-you/action/")
                    operation = parse_qs(parsed.query).get("op", [""])[0]
                    try:
                        state, nonce = action_server.home.store.begin_oidc_flow(token, operation)
                    except NeedsYouError:
                        self._text(HTTPStatus.GONE, "This action link is no longer current. Return to Slack and open Needs you again.")
                        return
                    params = urlencode({
                        "response_type": "code", "scope": "openid", "client_id": action_server.client_id,
                        "redirect_uri": action_server.redirect_uri, "state": state, "nonce": nonce,
                    })
                    self.send_response(HTTPStatus.SEE_OTHER)
                    self.send_header("Location", "https://slack.com/openid/connect/authorize?" + params)
                    self.send_header("Set-Cookie", f"needs_you_oidc_state={state}; Path=/needs-you/oauth; Secure; HttpOnly; SameSite=Lax; Max-Age=600")
                    self.end_headers()
                    return
                if parsed.path == "/needs-you/oauth/callback":
                    values = parse_qs(parsed.query)
                    state, code = values.get("state", [None])[0], values.get("code", [None])[0]
                    if not isinstance(state, str) or not isinstance(code, str):
                        self._text(HTTPStatus.BAD_REQUEST, "Slack sign-in did not complete.")
                        return
                    if _cookie(self.headers.get("Cookie"), "needs_you_oidc_state") != state:
                        self._text(HTTPStatus.FORBIDDEN, "Slack sign-in state did not match this browser.")
                        return
                    try:
                        token = action_server.web.openid_connect_token(
                            client_id=action_server.client_id, client_secret=action_server.client_secret,
                            code=code, redirect_uri=action_server.redirect_uri,
                        )
                        claims = _verify_id_token(str(token["id_token"]), action_server.client_id, state, action_server.home.store)
                        session, csrf = action_server.home.store.complete_oidc_flow(
                            state, team_id=claims["team_id"], user_id=claims["user_id"], nonce=claims["nonce"],
                        )
                    except Exception:
                        self._text(HTTPStatus.FORBIDDEN, "Slack sign-in could not verify this owner.")
                        return
                    self.send_response(HTTPStatus.SEE_OTHER)
                    self.send_header("Location", "/needs-you/confirm")
                    self.send_header("Set-Cookie", f"needs_you_session={session}; Path=/needs-you; Secure; HttpOnly; SameSite=Lax; Max-Age=600")
                    self.send_header("Set-Cookie", f"needs_you_csrf={csrf}; Path=/needs-you; Secure; HttpOnly; SameSite=Lax; Max-Age=600")
                    self.send_header("Set-Cookie", "needs_you_oidc_state=; Path=/needs-you/oauth; Secure; HttpOnly; SameSite=Lax; Max-Age=0")
                    self.end_headers()
                    return
                if parsed.path == "/needs-you/confirm":
                    session = _cookie(self.headers.get("Cookie"), "needs_you_session")
                    resolved = action_server.home.store.web_session(session) if session else None
                    if resolved is None:
                        self._text(HTTPStatus.GONE, "This confirmation is no longer current. Return to Slack and open Needs you again.")
                        return
                    item, operation = resolved
                    csrf = _cookie(self.headers.get("Cookie"), "needs_you_csrf")
                    if csrf is None:
                        self._text(HTTPStatus.GONE, "This confirmation is no longer current.")
                        return
                    self._confirmation(item.title, operation, csrf)
                    return
                self._text(HTTPStatus.NOT_FOUND, "Not found")

            def do_POST(self):
                if urlparse(self.path).path != "/needs-you/confirm":
                    self._text(HTTPStatus.NOT_FOUND, "Not found")
                    return
                session = _cookie(self.headers.get("Cookie"), "needs_you_session")
                try:
                    size = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    self._text(HTTPStatus.BAD_REQUEST, "Invalid confirmation.")
                    return
                if size < 1 or size > 1024:
                    self._text(HTTPStatus.BAD_REQUEST, "Invalid confirmation.")
                    return
                values = parse_qs(self.rfile.read(size).decode("utf-8", "replace"))
                csrf = values.get("csrf", [""])[0]
                delay = values.get("delay", [None])[0]
                try:
                    action_server.home.store.apply_web_action(
                        session or "", csrf, snooze_seconds=int(delay) if delay is not None else None,
                    )
                    action_server.home.request_publish()
                    action_server.wake()
                except (NeedsYouError, ValueError):
                    self._text(HTTPStatus.CONFLICT, "This item changed. Return to Slack and open Needs you again.")
                    return
                self._text(HTTPStatus.OK, "Updated. You can return to Slack.")

            def _confirmation(self, title: str, operation: str, csrf: str):
                escaped = html.escape(title)
                if operation == "snooze":
                    controls = """<label>Bring it back <select name=\"delay\"><option value=\"86400\">In 1 day</option><option value=\"259200\">In 3 days</option><option value=\"604800\">In 1 week</option></select></label>"""
                    button = "Snooze"
                else:
                    controls, button = "", "Bring back"
                body = f"<!doctype html><title>Needs you</title><h1>{html.escape(button)}</h1><p>{escaped}</p><form method=\"post\"><input type=\"hidden\" name=\"csrf\" value=\"{html.escape(csrf)}\">{controls}<p><button>{html.escape(button)}</button></p></form>"
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(body.encode())

            def _redirect(self, location: str):
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Location", location)
                self.end_headers()

            def _text(self, status: HTTPStatus, value: str):
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(value.encode())

        return Handler


def _verify_id_token(token: str, client_id: str, state: str, store) -> dict[str, str]:
    """Verify Slack's signed ID token and recover the nonce from the bound flow."""
    import jwt
    from jwt import PyJWKClient
    with store._lock:
        row = store._connection.execute("SELECT nonce FROM needs_you_oidc_flows WHERE state_hash=?", (store_hash(state),)).fetchone()
    if row is None:
        raise NeedsYouError("unknown sign-in state")
    key = PyJWKClient("https://slack.com/openid/connect/keys").get_signing_key_from_jwt(token).key
    claims = jwt.decode(token, key, algorithms=["RS256"], audience=client_id, issuer="https://slack.com", options={"require": ["exp", "iat", "nonce"]})
    if claims.get("nonce") != row["nonce"]:
        raise NeedsYouError("sign-in nonce mismatch")
    team_id, user_id = claims.get("https://slack.com/team_id"), claims.get("https://slack.com/user_id")
    if not isinstance(team_id, str) or not isinstance(user_id, str):
        raise NeedsYouError("sign-in claims missing identity")
    return {"team_id": team_id, "user_id": user_id, "nonce": str(claims["nonce"])}


def store_hash(value: str) -> str:
    from .needs_you import _hash_token
    return _hash_token(value)


def _cookie(value: str | None, name: str) -> str | None:
    if not value:
        return None
    for part in value.split(";"):
        key, separator, item = part.strip().partition("=")
        if separator and key == name:
            return item
    return None
