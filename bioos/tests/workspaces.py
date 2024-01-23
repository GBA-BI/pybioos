from datetime import datetime
from unittest import mock
from unittest.mock import patch

import pandas.testing
from pandas import DataFrame

from bioos import bioos
from bioos.service.BioOsService import BioOsService
from bioos.tests.base import BaseInit


class TestWorkspaces(BaseInit):
    list_workspace_val = {
        'Items':
            [{'ID': BaseInit.workspace_id, 'Name': 'test-gqm',
              'Description': 'test-gqm', 'CreateTime': 1661326698,
              'UpdateTime': 1661326698, 'OwnerName': 'test',
              'CoverDownloadURL':
                  'https://fake.tos-s3-cn-beijing.volces.com/template-cover/pic7.png?XXXXXXXXXXXX',
              'Role': 'Admin', 'S3Bucket': 'bioos-dev-fake-workspace'}],
        'PageNumber': 1, 'PageSize': 10, 'TotalCount': 1}
    notebook_env_info = {
        'Items':
            [{'ClusterInfo':
                  {'ID': 'default',
                   'Name': 'Volcengine Container Cluster',
                   'Status': 'Running',
                   'StartTime': 1668596478,
                   'Description': 'Default Volcengine Container Cluster',
                   'Bound': True,
                   'Public': True,
                   'ExternalConfig': {
                       'WESEndpoint': 'http://localhost:8080/ga4gh/wes/v1',
                       'JupyterhubEndpoint': 'http://jupyterhub.fake.com/jupyterhub',
                       'JupyterhubJWTSecret': '',
                       'ResourceScheduler': 'Kubernetes',
                       'Filesystem': 'tos'}},
              'Type': 'notebook', 'BindTime': 1676534060}], 'TotalCount': 1
    }
    workflow_env_info = {
        'Items':
            [{'ClusterInfo':
                  {'ID': 'default',
                   'Name': 'Volcengine Container Cluster',
                   'Status': 'Running',
                   'StartTime': 1668596478,
                   'Description': 'Default Volcengine Container Cluster',
                   'Bound': True,
                   'Public': True,
                   'ExternalConfig': {
                       'WESEndpoint': 'http://localhost:8080/ga4gh/wes/v1',
                       'JupyterhubEndpoint': 'http://jupyterhub.fake.com/jupyterhub',
                       'JupyterhubJWTSecret': '',
                       'ResourceScheduler': 'Kubernetes',
                       'Filesystem': 'tos'}},
              'Type': 'workflow', 'BindTime': 1676534060}], 'TotalCount': 1
    }

    def list_cluster_side_effect(self, params):
        if params.get("Type") == "notebook":
            return self.notebook_env_info
        else:
            return self.workflow_env_info

    def __init__(self, *args, **kwargs):
        super(TestWorkspaces, self).__init__(*args, **kwargs)
        self.ws = bioos.workspace(self.workspace_id)

    def test_singleton(self):
        ws = bioos.workspace(self.workspace_id)
        self.assertEqual(self.ws, ws)

    def test_repr_info_and_cache_call(self):
        with patch.object(BioOsService, "list_workspaces",
                          return_value=self.list_workspace_val) as success_list_workspaces:
            with patch.object(BioOsService, "list_cluster",
                              side_effect=self.list_cluster_side_effect) as success_list_cluster:
                # makes no call
                self.ws.data_models
                # call get_workspace and list_cluster once
                repr(self.ws)
                basic_info = self.ws.basic_info
                env_info = self.ws.env_info
                self.assertEqual(basic_info["name"],
                                 self.list_workspace_val.get("Items")[0].get('Name'))
                self.assertEqual(basic_info["description"],
                                 self.list_workspace_val.get("Items")[0].get('Description'))
                self.assertEqual(basic_info["s3_bucket"],
                                 self.list_workspace_val.get("Items")[0].get("S3Bucket"))
                self.assertEqual(basic_info["owner"],
                                 self.list_workspace_val.get("Items")[0].get("OwnerName"))
                self.assertEqual(basic_info["create_time"],
                                 datetime.fromtimestamp(
                                     self.list_workspace_val.get("Items")[0].get("CreateTime"))
                                 )

                pandas.testing.assert_frame_equal(env_info, DataFrame.from_records([
                    {"cluster_id": "default", "name": "Volcengine Container Cluster",
                     "description": "Default Volcengine Container Cluster", "type": "notebook"},
                    {"cluster_id": "default", "name": "Volcengine Container Cluster",
                     "description": "Default Volcengine Container Cluster", "type": "workflow"},
                ]))

        success_list_workspaces.assert_called_once_with({"Filter": {"IDs": [self.workspace_id]}})
        success_list_cluster.assert_has_calls([
            mock.call(params={
                'Type': "notebook",
                "ID": self.workspace_id}
            ),
            mock.call(params={
                'Type': "workflow",
                "ID": self.workspace_id}
            )
        ])
