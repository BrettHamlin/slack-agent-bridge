"""Real loopback HTTP acceptance with synthetic, cryptographically signed Slack IDs.

The JWT key fetch and OAuth code exchange are doubles. Requests, cookies, HTML,
JWT claim/signature verification, and durable state transitions use real code.
"""
from dataclasses import replace
import http.client
from http.cookies import SimpleCookie
from pathlib import Path
import re
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlparse

from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

from director.needs_you import NeedsYouHome, NeedsYouStore
from director.needs_you_http import NeedsYouActionServer


class NeedsYouHTTPReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.signing_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "state.sqlite3"
        self.config = {"team_id": "T-test", "owner_user_id": "U-owner", "channel_id": "C-test",
                       "workspace_domain": "example.slack.com", "needs_you": {
                           "enabled": True, "action_url": "https://bridge.example.test"}}
        self.web = SimpleNamespace(openid_connect_token=lambda **kwargs: {"id_token": self.id_token})
        self.home = NeedsYouHome(self.web, self.path, self.config)
        self.addCleanup(self.home.close)
        self.home.target = replace(self.home.target, http_port=0)
        self.wakes = []
        self.server = NeedsYouActionServer(self.home, self.web, client_id="synthetic-client",
                                          client_secret="synthetic-secret", wake=lambda: self.wakes.append(True))
        self.server.start()
        self.addCleanup(self.server.close)
        key_lookup = patch("jwt.PyJWKClient")
        lookup = key_lookup.start()
        self.addCleanup(key_lookup.stop)
        lookup.return_value.get_signing_key_from_jwt.return_value = SimpleNamespace(key=self.signing_key.public_key())
        self.cookies = {}
        self.item = self.home.store.record_completed_result(
            idempotency_key="synthetic-result", root="100.000001",
            conversation_url="https://example.slack.com/archives/C-test/p100000001",
            title="Review the prepared proposal", detail="Draft ready")

    def request(self, method, path, body=None, *, cookies=True, extra_headers=None):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if cookies:
            headers["Cookie"] = "; ".join(f"{key}={value}" for key, value in self.cookies.items())
        headers.update(extra_headers or {})
        connection = http.client.HTTPConnection("127.0.0.1", self.server._server.server_port, timeout=3)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            payload = response.read().decode()
            response_headers = response.getheaders()
            for key, value in response_headers:
                if key.lower() == "set-cookie":
                    parsed = SimpleCookie()
                    parsed.load(value)
                    self.cookies.update({name: morsel.value for name, morsel in parsed.items()})
            return response.status, dict(response_headers), payload
        finally:
            connection.close()

    def begin(self, operation="snooze", item=None):
        link = self.home.store.create_action_link(item or self.item, operation)
        parts = urlparse(link)
        status, headers, _ = self.request("GET", parts.path + "?" + parts.query)
        self.assertEqual(status, 303)
        return parse_qs(urlparse(headers["Location"]).query)

    def callback(self, flow, *, overrides=None, signing_key=None, cookies=True):
        claims = {"iss": "https://slack.com", "aud": "synthetic-client", "sub": "U-owner",
                  "iat": int(time.time()), "exp": int(time.time()) + 300, "nonce": flow["nonce"][0],
                  "https://slack.com/team_id": "T-test", "https://slack.com/user_id": "U-owner"}
        claims.update(overrides or {})
        self.id_token = jwt.encode(claims, signing_key or self.signing_key, algorithm="RS256")
        return self.request("GET", "/needs-you/oauth/callback?" + urlencode({
            "state": flow["state"][0], "code": "synthetic-code"}), cookies=cookies)

    def sign_in(self, operation="snooze", item=None):
        flow = self.begin(operation, item)
        status, _, _ = self.callback(flow)
        self.assertEqual(status, 303)
        status, _, body = self.request("GET", "/needs-you/confirm")
        self.assertEqual(status, 200)
        return re.search(r'name="csrf" value="([^"]+)"', body).group(1)

    def test_link_preview_and_confirmation_get_do_not_snooze(self):
        self.begin()
        self.begin()
        self.sign_in()
        self.assertEqual(self.home.store.get(self.item.id), self.item)
        self.assertEqual(self.wakes, [])

    def test_snooze_and_bring_back_through_http_are_single_use(self):
        csrf = self.sign_in()
        body = urlencode({"csrf": csrf, "delay": "86400"})
        status, _, _ = self.request("POST", "/needs-you/confirm", body)
        self.assertEqual(status, 200)
        snoozed = self.home.store.get(self.item.id)
        self.assertEqual(snoozed.state, "snoozed")
        self.assertAlmostEqual(snoozed.snoozed_until - time.time(), 86400, delta=5)
        status, _, _ = self.request("POST", "/needs-you/confirm", body)
        self.assertEqual(status, 409)
        self.assertEqual(self.home.store.get(self.item.id), snoozed)
        csrf = self.sign_in("bring_back", snoozed)
        status, _, _ = self.request("POST", "/needs-you/confirm", urlencode({"csrf": csrf}))
        self.assertEqual(status, 200)
        self.assertEqual(self.home.store.get(self.item.id).state, "active")
        self.assertEqual(len(self.wakes), 2)

    def test_post_without_session_or_with_bad_csrf_cannot_change_item(self):
        self.sign_in()
        for cookies in (False, True):
            status, _, _ = self.request("POST", "/needs-you/confirm",
                                       urlencode({"csrf": "wrong", "delay": "86400"}), cookies=cookies)
            self.assertIn(status, {403, 409})
            self.assertEqual(self.home.store.get(self.item.id), self.item)

    def test_checkbox_done_wins_over_old_web_confirmation(self):
        csrf = self.sign_in()
        done, _ = self.home.store.set_done(item_id=self.item.id, expected_version=self.item.version,
                                          done=True, action_ts="200.000001")
        status, _, _ = self.request("POST", "/needs-you/confirm", urlencode({"csrf": csrf, "delay": "86400"}))
        self.assertEqual(status, 409)
        self.assertEqual(self.home.store.get(self.item.id), done)

    def test_signed_wrong_identity_or_claims_are_rejected(self):
        for overrides in ({"https://slack.com/user_id": "U-other"},
                          {"https://slack.com/team_id": "T-other"}, {"nonce": "wrong"},
                          {"aud": "wrong-client"}, {"iss": "https://evil.example"}, {"exp": 1}):
            with self.subTest(overrides=overrides):
                status, _, _ = self.callback(self.begin(), overrides=overrides)
                self.assertEqual(status, 403)
                self.assertEqual(self.home.store.get(self.item.id), self.item)

    def test_wrong_signature_is_rejected(self):
        status, _, _ = self.callback(self.begin(), signing_key=self.other_key)
        self.assertEqual(status, 403)

    def test_callback_cannot_be_replayed(self):
        flow = self.begin()
        self.assertEqual(self.callback(flow)[0], 303)
        self.assertEqual(self.callback(flow)[0], 403)

    def test_callback_requires_the_browser_that_started_sign_in(self):
        status, _, _ = self.callback(self.begin(), cookies=False)
        self.assertEqual(status, 403)
        self.assertEqual(self.home.store.get(self.item.id), self.item)

    def test_confirmation_prevents_caching_framing_and_referrer_leaks(self):
        self.sign_in()
        status, headers, _ = self.request("GET", "/needs-you/confirm")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        self.assertIn("frame-ancestors 'none'", headers["Content-Security-Policy"])
        self.assertEqual(headers["Referrer-Policy"], "no-referrer")

    def test_expired_confirmation_cannot_snooze(self):
        csrf = self.sign_in()
        with patch("director.needs_you.time.time", return_value=time.time() + 601):
            status, _, _ = self.request("POST", "/needs-you/confirm",
                                       urlencode({"csrf": csrf, "delay": "86400"}))
        self.assertEqual(status, 409)
        self.assertEqual(self.home.store.get(self.item.id), self.item)

    def test_malformed_content_length_is_a_handled_request_error(self):
        status, _, _ = self.request("POST", "/needs-you/confirm", b"", extra_headers={"Content-Length": "bad"})
        self.assertEqual(status, 400)
        self.assertEqual(self.home.store.get(self.item.id), self.item)

    def test_web_session_cannot_read_old_source_after_reconfiguration(self):
        self.sign_in()
        other = NeedsYouStore(self.path, replace(self.home.target, source_channel_id="C-other"))
        self.addCleanup(other.close)
        self.assertIsNone(other.web_session(self.cookies["needs_you_session"]))


if __name__ == "__main__":
    unittest.main()
