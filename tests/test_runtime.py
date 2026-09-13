import unittest
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch
from director.runtime import CredentialUnavailable, credentials, mounted_credentials, provision_runtime_environment, verify_identity

class CredentialAvailabilityTests(unittest.TestCase):
    def test_missing_mount_values_raise_specific_safe_error(self):
        with patch('dotenv.dotenv_values', return_value={}):
            with self.assertRaises(CredentialUnavailable):
                mounted_credentials(Path('/unused'))

    def test_partial_mount_cannot_start_runtime(self):
        with patch('dotenv.dotenv_values', return_value={'SLACK_BOT_TOKEN':'synthetic'}):
            with self.assertRaises(CredentialUnavailable):
                mounted_credentials(Path('/unused'))

    def test_mount_consumption_disables_interpolation(self):
        values={'SLACK_BOT_TOKEN':'synthetic-bot','SLACK_APP_TOKEN':'synthetic-app'}
        with patch('dotenv.dotenv_values', return_value=values) as read:
            self.assertEqual(mounted_credentials(Path('/unused')), values)
            read.assert_called_once_with(Path('/unused/.env'), interpolate=False)

    def test_unresponsive_mount_is_bounded(self):
        with patch('director.runtime.CREDENTIAL_TIMEOUT_SECONDS', 0.02), patch('dotenv.dotenv_values', side_effect=lambda *a, **k: time.sleep(1)):
            with self.assertRaises(CredentialUnavailable):
                mounted_credentials(Path('/unused'))


class RuntimeIdentityTests(unittest.TestCase):
    def config(self, domain='director.test'):
        return {
            'team_id': 'T-allowed',
            'channel_id': 'C-allowed',
            'owner_user_id': 'U-owner',
            'bot_user_id': 'U-bot',
            'workspace_domain': domain,
        }

    def web(self, domain='director.test'):
        return type('Web', (), {
            'auth_test': lambda _self: {'team_id': 'T-allowed', 'user_id': 'U-bot', 'url': f'https://{domain}/'},
            'conversations_info': lambda _self, **_kwargs: {'channel': {'id': 'C-allowed', 'is_private': True}},
            'conversations_members': lambda _self, **_kwargs: {'members': ['U-owner', 'U-bot']},
        })()

    def test_identity_requires_the_configured_workspace_domain(self):
        verify_identity(self.web(), self.config())
        with self.assertRaises(RuntimeError):
            verify_identity(self.web('other.test'), self.config())


class UnattendedCredentialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name)
        self.env = {'SLACK_BOT_TOKEN': 'synthetic-bot', 'SLACK_APP_TOKEN': 'synthetic-app'}
        self.clean_env = patch.dict(os.environ, {}, clear=True)
        self.clean_env.start()

    def tearDown(self):
        self.clean_env.stop()
        self.temp.cleanup()

    def test_process_environment_never_opens_mount_or_runtime_file(self):
        with patch.dict(os.environ, self.env), patch('dotenv.dotenv_values', side_effect=AssertionError('must not open file')):
            self.assertEqual(credentials(self.project), self.env)

    def test_partial_process_environment_does_not_mix_credential_sources(self):
        with patch.dict(os.environ, {'SLACK_BOT_TOKEN':'synthetic'}), patch('dotenv.dotenv_values', side_effect=AssertionError('must not fallback')):
            with self.assertRaises(CredentialUnavailable): credentials(self.project)

    def test_missing_runtime_file_does_not_open_legacy_mount(self):
        os.mkfifo(self.project / '.env', 0o600)
        with patch('director.runtime.mounted_credentials', side_effect=AssertionError('must not open mount')):
            with self.assertRaises(CredentialUnavailable): credentials(self.project)

    def test_private_runtime_file_works_independently_of_mount(self):
        runtime = self.project / '.env.runtime'
        runtime.write_text('SLACK_BOT_TOKEN=synthetic-bot\nSLACK_APP_TOKEN=synthetic-app\n')
        runtime.chmod(0o600)
        os.mkfifo(self.project / '.env', 0o600)
        with patch('director.runtime.mounted_credentials', side_effect=AssertionError('must not open mount')):
            self.assertEqual(credentials(self.project), self.env)

    def test_rejects_insecure_file_fifo_symlink_and_wrong_owner(self):
        runtime = self.project / '.env.runtime'
        runtime.write_text('synthetic')
        runtime.chmod(0o644)
        with self.assertRaises(CredentialUnavailable): credentials(self.project)
        runtime.chmod(0o600)
        with patch('director.runtime.os.getuid', return_value=-1):
            with self.assertRaises(CredentialUnavailable): credentials(self.project)
        runtime.unlink()
        os.mkfifo(runtime, 0o600)
        with self.assertRaises(CredentialUnavailable): credentials(self.project)
        runtime.unlink()
        other=self.project/'other';other.write_text('synthetic');other.chmod(0o600)
        runtime.symlink_to(other)
        with self.assertRaises(CredentialUnavailable): credentials(self.project)

    def test_provision_verifies_identity_and_installs_only_selected_pair(self):
        with patch('director.runtime.mounted_credentials', return_value={**self.env,'UNRELATED':'not-copied'}), patch('director.runtime.verify_identity') as verify:
            result=provision_runtime_environment(self.project,{})
        verify.assert_called_once()
        self.assertEqual(result['variable_names'],list(self.env))
        self.assertEqual(credentials(self.project),self.env)
        self.assertEqual((self.project/'.env.runtime').stat().st_mode & 0o777,0o600)
        self.assertNotIn('UNRELATED',(self.project/'.env.runtime').read_text())
        with patch('director.runtime.mounted_credentials', side_effect=AssertionError('must not reread')):
            with self.assertRaises(FileExistsError): provision_runtime_environment(self.project,{})

    def test_identity_failure_does_not_materialize_credentials(self):
        with patch('director.runtime.mounted_credentials', return_value=self.env), patch('director.runtime.verify_identity',side_effect=RuntimeError('mismatch')):
            with self.assertRaises(RuntimeError): provision_runtime_environment(self.project,{})
        self.assertEqual(list(self.project.iterdir()),[])
