from unittest import mock
from unittest.mock import patch

from pandas import DataFrame

from bioos import bioos
from bioos.config import Config
from bioos.tests.base import BaseInit


class TestBioOs(BaseInit):

    def test_login_status(self):
        status = bioos.status()
        with patch.object(Config, "_ping_func") as success_ping:
            self.assertIsInstance(status, Config.LoginInfo)
            self.assertEqual(status.access_key, self.ak)
            self.assertEqual(status.secret_key, self.sk)
            self.assertEqual(status.endpoint, self.endpoint)
            self.assertEqual(status.login_status, "Already logged in")
        success_ping.assert_called_once()
        with patch.object(Config, "_ping_func", side_effect=Exception(b'foo')) as fail_ping:
            fake_ak = "aaa"
            fake_sk = "bbb"
            fake_endpoint = "http://fake.endpoint.com"
            fake_region = "region"
            bioos.login(fake_endpoint, fake_ak, fake_sk, fake_region)
            status = bioos.status()
            self.assertIsInstance(status, Config.LoginInfo)
            self.assertEqual(status.access_key, fake_ak)
            self.assertEqual(status.secret_key, fake_sk)
            self.assertEqual(status.endpoint, fake_endpoint)
            self.assertEqual(status.region, fake_region)
            self.assertEqual(status.login_status, "Not logged in")
        fail_ping.assert_has_calls([mock.call(), mock.call()])

    def test_list_workspaces(self):
        with patch.object(Config.service(), "list_workspaces",
                          return_value={"Items": [{}]}) as success_list:
            workspaces = bioos.list_workspaces()
            self.assertIsInstance(workspaces, DataFrame)
        success_list.assert_called_once()
        success_list.assert_called_with({"PageSize": 0})
