import unittest
from unittest.mock import patch

from volcengine.const.Const import REGION_CN_NORTH1

from bioos import bioos
from bioos.config import Config


class BaseInit(unittest.TestCase):
    ak = "ak"
    sk = "sk"
    endpoint = "http://endpoint"
    region = REGION_CN_NORTH1
    workspace_id = "wccxxxxxxxxxxxxxno80"
    really_login = bioos.login(endpoint, ak, sk, region)

    def __init__(self, *args, **kwargs):
        with patch.object(Config, "_ping_func"):
            bioos.login(self.endpoint, self.ak, self.sk, self.region)
            super(BaseInit, self).__init__(*args, **kwargs)
