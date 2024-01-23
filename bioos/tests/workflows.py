import time
from unittest import mock
from unittest.mock import patch

import pandas
from pandas import DataFrame

from bioos import bioos
from bioos.errors import ConflictError
from bioos.service.BioOsService import BioOsService
from bioos.tests.base import BaseInit


class TestWorkflows(BaseInit):
    cluster_id = "default"

    workflow_name = "hello-test"
    workflow_id = "fake_workflow_id"

    data_model_name = "my_entity"
    data_model_id = "fake_data_model_id"

    submission_id = "fake_submission_id"
    run_id = "fake_run_id"

    list_workspace_val = {
        'Items':
            [{'ID': BaseInit.workspace_id, 'Name': 'test-gqm',
              'Description': 'test-gqm', 'CreateTime': 1661326698,
              'UpdateTime': 1661326698, 'OwnerName': 'test',
              'CoverDownloadURL':
                  'https://fake.tos-s3-cn-beijing.volces.com/template-cover/pic7.png?XXXXXXXXXXXX',
              'Role': 'Admin', 'S3Bucket': 'bioos-dev-fake-workspace'}],
        'PageNumber': 1, 'PageSize': 10, 'TotalCount': 1}

    list_workflows_val = {'Items': [
        {'ID': workflow_id, 'Name': workflow_name, 'Description': '[dockstore]',
         'CreateTime': 1670929870, 'UpdateTime': 1671032374, 'Language': 'WDL',
         'Source': 'https://github.com/fake/hello.git', 'Tag': 'main',
         'MainWorkflowPath': 'hello.wdl',
         'Status': {'Phase': 'Failed',
                    'Message': 'job failed: Reason=BackoffLimitExceeded, '
                               'Message=Job has reached the specified backoff limit; '
                               'failed to getJobLogs: invalid number of pods: 0'}}
    ], 'Total': 1}

    list_data_models_val = {'Total': 1, 'Items': [
        {'ID': data_model_id, 'Name': data_model_name, 'RowCount': 162, 'Type': 'normal'},
    ]}

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

    list_submission_val = {
        'Items': [{'ID': 'fake_submission_id', 'Name': 'hello-history-2022-12-21-16-54-29',
                   'Description': 'gqm-test-sdk', 'Status': 'Failed',
                   'RunStatus': {'Count': 3, 'Succeeded': 0, 'Failed': 3,
                                 'Running': 0}, 'StartTime': 1671612870,
                   'FinishTime': 1671613042, 'Duration': 172, 'WorkflowID':
                       'fcegt1vleig4f5465sofg', 'ClusterID': 'default',
                   'DataModelID': 'fake_data_model_id',
                   'Inputs': '{"testname.hello.name":"this.date"}',
                   'Outputs': '{"testname.hello.response":"this.response1"}',
                   'ExposedOptions': {'ReadFromCache': False,
                                      'ExecutionRootDir': 's3://bioos-dev-fake-workspace'},
                   'OwnerName': 'test'}], 'PageNumber': 1, 'PageSize': 10, 'TotalCount': 1}

    finished_list_runs_val = {'Total': 1, 'Items': [
        {'ID': run_id, 'DataEntityRowID': 'your-sample-3-id', 'Status': 'Failed',
         'StartTime': 1671612870, 'FinishTime': 1671613042, 'Duration': 172,
         'SubmissionID': 'fake_submission_id',
         'EngineRunID': 'd0da2939-8d12-4ba3-9872-529677a39da9',
         'Inputs': '{"testname.hello.name":"01/01/2022"}', 'Outputs': '',
         'TaskStatus': {'Succeeded': 0, 'Failed': 1, 'Running': 0},
         'Log': 's3://bioos-dev-fake-workspace/analysis/fake_submission_id'
                '/workflow.d0da2939-8d12-4ba3-9872-529677a39da9.log',
         'Message': 'workflow run failed: [{"causedBy":[{"causedBy":[],"message":"Task '
                    'testname.hello:NA:1 failed for unknown reason: FailedOrError"}],'
                    '"message":"Workflow failed"}]'
         }]}

    list_run_val = {'Total': 1, 'Items': [
        {'ID': run_id, 'DataEntityRowID': 'your-sample-3-id', 'Status': 'Running',
         'StartTime': 1671612870, 'Duration': 0, 'SubmissionID': 'fake_submission_id',
         'EngineRunID': None, 'Inputs': '{"testname.hello.name":"01/01/2022"}', 'Outputs': ''}]}

    list_tasks_val = {'Total': 0, 'Items': []}
    finished_list_tasks_val = {'Total': 1, 'Items': [
        {'Name': 'testname.hello', 'RunID': run_id, 'Status': 'Failed', 'StartTime': 1671612876,
         'FinishTime': 1671613037, 'Duration': 161,
         'Log': 's3://bioos-dev-fake-workspace/analysis/fake_submission_id/testname'
                '/d0da2939-8d12-4ba3-9872-529677a39da9/call-hello/execution/log',
         'Stdout': 's3://bioos-dev-fake-workspace/analysis/fake_submission_id/testname'
                   '/d0da2939-8d12-4ba3-9872-529677a39da9/call-hello/execution/stdout',
         'Stderr': 's3://bioos-dev-fake-workspace/analysis/fake_submission_id/testname'
                   '/d0da2939-8d12-4ba3-9872-529677a39da9/call-hello/execution/stderr'}]}

    def __init__(self, *args, **kwargs):
        super(TestWorkflows, self).__init__(*args, **kwargs)
        ws = bioos.workspace(self.workspace_id)
        with patch.object(BioOsService, "list_workspaces",
                          return_value=self.list_workspace_val):
            with patch.object(BioOsService, "list_workflows",
                              return_value=self.list_workflows_val):
                self.workflows = ws.workflows
                self.workflow = ws.workflow(self.workflow_name)
        # XXX: wait for a while to make cache overdue
        time.sleep(2)

    def test_workflows_repr(self):
        with patch.object(BioOsService, "list_workflows",
                          return_value=self.list_workflows_val) as success_list_workflows:
            with patch.object(BioOsService, "list_cluster",
                              return_value=self.workflow_env_info) as success_list_cluster:
                # makes no call
                self.workflows
                # call list_workflows once and list_cluster twice
                repr(self.workflows)
                # make another call of list_workflows
                list_res = self.workflows.list()
                pandas.testing.assert_frame_equal(list_res, DataFrame.from_records([
                    {'ID': self.workflow_id, 'Name': self.workflow_name,
                     'Description': '[dockstore]',
                     'CreateTime': pandas.to_datetime(1670929870,
                                                      unit='ms',
                                                      origin=pandas.Timestamp('2018-07-01')),
                     'UpdateTime': pandas.to_datetime(1671032374,
                                                      unit='ms',
                                                      origin=pandas.Timestamp('2018-07-01')),
                     'Language': 'WDL', 'Source': 'https://github.com/fake/hello.git',
                     'Tag': 'main', 'MainWorkflowPath': 'hello.wdl'}
                ]))
                success_list_workflows.assert_has_calls([
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                        'SortBy': 'CreateTime',
                        'PageSize': 0,
                    }),
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                        'SortBy': 'CreateTime',
                        'PageSize': 0,
                    }),
                ])
                success_list_cluster.assert_has_calls([
                    mock.call(params={
                        'Type': "workflow",
                        "ID": self.workspace_id
                    })
                ])

    def test_import_and_delete(self):
        with patch.object(BioOsService, "check_workflow",
                          return_value={'IsNameExist': False}) as success_check:
            with patch.object(BioOsService, "create_workflow",
                              return_value={'ID': self.workflow_id}) as success_create:
                workflow_id = self.workflows.import_workflow("https://github.com/fake/hello.git",
                                                             self.workflow_name, "WDL", "main",
                                                             "hello.wdl", "[dockstore]")
                self.assertEqual(workflow_id, self.workflow_id)
        success_check.assert_called_once_with({
            "WorkspaceID": self.workspace_id,
            "Name": self.workflow_name,
        })
        success_create.assert_called_once_with({
            "WorkspaceID": self.workspace_id,
            "Name": self.workflow_name,
            "Description": "[dockstore]",
            "Language": "WDL",
            "Source": "https://github.com/fake/hello.git",
            "Tag": "main",
            "MainWorkflowPath": "hello.wdl",
        })

        with patch.object(BioOsService, "check_workflow",
                          return_value={'IsNameExist': True}) as success_check:
            with patch.object(BioOsService, "create_workflow") as miss_create:
                try:
                    self.workflows.import_workflow("https://github.com/fake/hello.git",
                                                   self.workflow_name, "WDL", "main", "hello.wdl",
                                                   "[dockstore]")
                except ConflictError as e:
                    self.assertEqual(e.message, f"parameter 'name' conflicts: "
                                                f"{self.workflow_name} already exists")
        success_check.assert_called_once_with({
            "WorkspaceID": self.workspace_id,
            "Name": self.workflow_name,
        })
        miss_create.assert_not_called()

        with patch.object(BioOsService, "delete_workflow") as success_delete:
            with patch.object(BioOsService, "list_workflows",
                              return_value=self.list_workflows_val):
                with patch.object(BioOsService, "list_cluster",
                                  return_value=self.workflow_env_info):
                    self.workflows.delete(self.workflow_name)

        success_delete.assert_called_once_with({
            "WorkspaceID": self.workspace_id,
            "ID": self.workflow_id
        })

    def test_submit(self):
        with patch.object(BioOsService, "list_cluster",
                          return_value=self.workflow_env_info) as list_cluster:
            with patch.object(BioOsService, "list_workflows",
                              return_value=self.list_workflows_val) as list_workflows:
                with patch.object(BioOsService, "list_data_models",
                                  return_value=self.list_data_models_val) as list_data_models:
                    with patch.object(BioOsService, "create_submission",
                                      return_value={
                                          'ID': self.submission_id}) as create_submission:
                        with patch.object(BioOsService, "list_submissions",
                                          return_value=self.list_submission_val) as list_submissions:
                            with patch.object(BioOsService, "list_tasks",
                                              return_value=self.list_tasks_val) as list_tasks:
                                with patch.object(BioOsService, "list_runs",
                                                  return_value=self.list_run_val) as list_runs:
                                    runs = self.workflow.submit(
                                        data_model_name=self.data_model_name,
                                        row_ids=[
                                            "your-sample-3-id"
                                        ],
                                        inputs='{"testname.hello.name":"this.name1"}',
                                        outputs='{"testname.hello.response":"this.response1"}',
                                        submission_desc="gqm-test-sdk",
                                        call_caching=False)
                                    self.assertEqual(len(runs), 1)
                                    run = runs[0]
                                    self.assertEqual(run.submission, self.submission_id)
                                    self.assertEqual(run.id, self.run_id)
        list_cluster.assert_called_once()
        list_workflows.assert_called_once_with({
            'WorkspaceID': self.workspace_id,
            'SortBy': 'CreateTime',
            'PageSize': 0,
        })
        list_data_models.assert_has_calls([
            mock.call({
                'WorkspaceID': self.workspace_id
            }), mock.call({
                'WorkspaceID': self.workspace_id
            })
        ])
        # list_data_models.assert_called_once()
        create_submission.assert_called_once()
        # list_runs.assert_called_once()
        list_submissions.assert_called_once()
        list_tasks.assert_not_called()
        list_runs.assert_has_calls([
            mock.call({
                'WorkspaceID': self.workspace_id,
                'SubmissionID': self.submission_id,
                'PageSize': 0
            }), mock.call({
                'WorkspaceID': self.workspace_id,
                'Filter': {'IDs': ['fake_run_id']},
            }), mock.call({
                'WorkspaceID': self.workspace_id,
                'SubmissionID': self.submission_id,
            })
        ])
        # wait for a while to make cache overdue
        time.sleep(2)
        with patch.object(BioOsService, "list_runs",
                          return_value=self.finished_list_runs_val) as list_runs:
            with patch.object(BioOsService, "list_tasks",
                              return_value=self.finished_list_tasks_val) as list_tasks:
                repr(run)
        list_runs.assert_called_once()
        list_tasks.assert_called_once()
