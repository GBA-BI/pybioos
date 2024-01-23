import copy
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pandas.testing
from pandas import DataFrame

from bioos import bioos
from bioos.errors import ConflictError, NotFoundError
from bioos.service.BioOsService import BioOsService
from bioos.tests.base import BaseInit


class TestDataModel(BaseInit):
    list_data_models_val = {'TotalCount': 9, 'Items': [
        {'ID': 'dcblq1tteig44bng68od0', 'Name': 'jxc', 'RowCount': 162, 'Type': 'normal'},
        {'ID': 'dccc0ne5eig41ascop420', 'Name': 'run', 'RowCount': 499, 'Type': 'normal'},
        {'ID': 'dccc0nkleig41ascop42g', 'Name': 'sample', 'RowCount': 3, 'Type': 'normal'},
        {'ID': 'dccc0nmteig41ascop430', 'Name': 'sample6', 'RowCount': 1922, 'Type': 'normal'},
        {'ID': 'dccdaq2deig42s7rgs7j0', 'Name': 'sample_set', 'RowCount': 1, 'Type': 'set'},
        {'ID': 'dccc0o0teig41ascop43g', 'Name': 'test', 'RowCount': 3000, 'Type': 'normal'},
        {'ID': 'dccdaq6leig42s7rgs7jg', 'Name': 'test_set', 'RowCount': 1, 'Type': 'set'},
        {'ID': 'dccc0o2teig41ascop440', 'Name': 'testaa', 'RowCount': 3000, 'Type': 'normal'},
        {'ID': 'dcc6tbkleig4c9lddjt2g', 'Name': 'workspace_data', 'RowCount': 257788, 'Type': 'workspace'}]}
    list_data_model_rows_val = \
        {
            'TotalCount': 3,
            'Headers':
                ['sample_id', 'column-1-file-CRAM', 'date',
                 '123456789s_123456789s_123456789s_123456789s_', 'gg', 'hh', 'jj'],
            'Rows': [
                [
                    'your-sample-1-id', 'OK', '01/01/2022', 'we啊', 'abc',
                    's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/b6d2bf67-760c-4c56-b5b0-30a2e08dc180/call-step1/execution/resp.txt',
                    'abc'
                ],
                [
                    'your-sample-2-id', 'OK', '01/01/2022', "s'd", 'abc',
                    's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/cccd6f44-a53b-4f77-8b0a-b72768aba55a/call-step1/execution/resp.txt',
                    'abc'
                ],
                [
                    'your-sample-3-id', 'OK', '01/01/2022', "s'd", 'abc',
                    's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/c86ce783-6dd7-47e1-88ba-e282e5b0d2c4/call-step1/execution/resp.txt',
                    'abc'
                ]
            ]
        }
    list_data_model_rows_id = {
        "RowIDs": [
            "your-sample-1-id",
            "your-sample-2-id",
            "your-sample-changed"
        ]
    }

    sample_data = pd.DataFrame(
        {'sample_id': {0: 'your-sample-1-id', 1: 'your-sample-2-id', 2: 'your-sample-3-id'},
         'column-1-file-CRAM': {0: 'OK', 1: 'OK', 2: 'OK'},
         'date': {0: '01/01/2022', 1: '01/01/2022', 2: '01/01/2022'},
         '123456789s_123456789s_123456789s_123456789s_': {0: 'we啊', 1: "s'd", 2: "s'd"},
         'gg': {0: 'abc', 1: 'abc', 2: 'abc'}, 'hh': {
            0: 's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/b6d2bf67-760c-4c56-b5b0-30a2e08dc180/call-step1/execution/resp.txt',
            1: 's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/cccd6f44-a53b-4f77-8b0a-b72768aba55a/call-step1/execution/resp.txt',
            2: 's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/c86ce783-6dd7-47e1-88ba-e282e5b0d2c4/call-step1/execution/resp.txt'},
         'jj': {0: 'abc', 1: 'abc', 2: 'abc'}})
    sample_data_to_write = pd.DataFrame(
        {'sample_id': {0: 'your-sample-1-id', 1: 'your-sample-2-id', 2: 'your-sample-changed'},
         'column-1-file-CRAM': {0: 'OK', 1: 'OK', 2: 'OK'},
         'date': {0: '01/01/2022', 1: '01/01/2022', 2: '01/01/2022'},
         '123456789s_123456789s_123456789s_123456789s_': {0: 'we啊', 1: "s'd", 2: "s'd"},
         'gg': {0: 'abc', 1: 'abc', 2: 'abc'}, 'hh': {
            0: 's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/b6d2bf67-760c-4c56-b5b0-30a2e08dc180/call-step1/execution/resp.txt',
            1: 's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/cccd6f44-a53b-4f77-8b0a-b72768aba55a/call-step1/execution/resp.txt',
            2: 's3://bioos-transfertest-testfake_workspace/analysis/scc9oqc5eig42lnksf7qg/test/c86ce783-6dd7-47e1-88ba-e282e5b0d2c4/call-step1/execution/resp.txt'},
         'jj': {0: 'abc', 1: 'abc', 2: 'abc'}})

    def __init__(self, *args, **kwargs):
        super(TestDataModel, self).__init__(*args, **kwargs)
        self.data_models = bioos.workspace(self.workspace_id).data_models

    def test_repr(self):
        with patch.object(BioOsService, "list_data_models",
                          return_value=self.list_data_models_val) as success_list:
            # makes no call
            self.data_models
            # call list_data_models once
            repr(self.data_models)
            # makes another list_data_models call
            pandas.testing.assert_frame_equal(self.data_models.list(), DataFrame.from_records([
                {"ID": "dcblq1tteig44bng68od0", "Name": "jxc",
                 "RowCount": 162, "Type": "normal"},
                {"ID": "dccc0ne5eig41ascop420", "Name": "run",
                 "RowCount": 499, "Type": "normal"},
                {"ID": "dccc0nkleig41ascop42g", "Name": "sample",
                 "RowCount": 3, "Type": "normal"},
                {"ID": "dccc0nmteig41ascop430", "Name": "sample6",
                 "RowCount": 1922, "Type": "normal"},
                {"ID": "dccc0o0teig41ascop43g", "Name": "test",
                 "RowCount": 3000, "Type": "normal"},
                {"ID": "dccc0o2teig41ascop440", "Name": "testaa",
                 "RowCount": 3000, "Type": "normal"},
            ]))
        success_list.assert_has_calls([
            mock.call({
                'WorkspaceID': self.workspace_id
            }), mock.call({
                'WorkspaceID': self.workspace_id
            })
        ])

    def test_read_all(self):
        with patch.object(BioOsService, "list_data_models",
                          return_value=self.list_data_models_val) as success_list:
            with patch.object(BioOsService, "list_data_model_rows",
                              return_value={}) as success_rows_list:
                self.data_models.read()
        success_list.assert_called_once_with({
            'WorkspaceID': self.workspace_id,
        })
        calls = []
        for model in self.list_data_models_val.get("Items"):
            if model.get("Type") == "normal":
                calls.append(mock.call({
                    'WorkspaceID': self.workspace_id,
                    'ID': model["ID"],
                    'PageSize': 0,
                }))
        success_rows_list.assert_has_calls(
            calls
        )

    def test_crud(self):
        new_sample = "new_sample"
        new_sample_id = "acbq1tteig44bng68oa0"
        data_to_write = {"sample": self.sample_data_to_write,
                         new_sample: self.sample_data_to_write}
        with patch.object(BioOsService, "delete_data_model_rows_and_headers") as success_delete:
            with patch.object(BioOsService, "create_data_model",
                              return_value={
                                  "ID": new_sample_id
                              }) as success_create:

                with patch.object(BioOsService, "list_data_models",
                                  return_value=self.list_data_models_val) as success_list:
                    with patch.object(BioOsService, "list_data_model_rows",
                                      return_value=self.list_data_model_rows_val) as success_rows_list:
                        read_res = self.data_models.read("sample")
                        self.assertEqual(len(read_res), 1)
                        self.assertIsInstance(read_res, dict)
                        pandas.testing.assert_frame_equal(read_res["sample"], self.sample_data)

                        try:
                            self.data_models.write(data_to_write, force=False)
                        except ConflictError as e:
                            self.assertEqual(e.message,
                                             "parameter 'sources' conflicts: {'sample'} "
                                             "already exists, pls use force=True to overwrite")
                        success_create.assert_not_called()

                        self.data_models.write(data_to_write, force=True)
                        success_create.assert_has_calls([
                            mock.call({
                                'WorkspaceID': self.workspace_id,
                                'Name': "sample",
                                'Headers': list(self.sample_data_to_write.head()),
                                'Rows': self.sample_data_to_write.values.tolist(),
                            }),
                            mock.call({
                                'WorkspaceID': self.workspace_id,
                                'Name': new_sample,
                                'Headers': list(self.sample_data_to_write.head()),
                                'Rows': self.sample_data_to_write.values.tolist(),
                            })
                        ])

                success_list.assert_has_calls([
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                    }),
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                    }),
                ])
                success_rows_list.assert_called_once_with({
                    'WorkspaceID': self.workspace_id,
                    'ID': "dccc0nkleig41ascop42g",
                    'PageSize': 0,
                })

                new_list_data_models_val = copy.deepcopy(self.list_data_models_val)
                new_list_data_models_val["TotalCount"] = 10
                new_list_data_models_val['Items'].append(
                    {'ID': new_sample_id, 'Name': new_sample, 'RowCount': 3, 'Type': 'normal'})

                new_list_data_model_rows_val = copy.deepcopy(self.list_data_model_rows_val)
                new_list_data_model_rows_val['Rows'][2][0] = "your-sample-changed"
                with patch.object(BioOsService, "list_data_models",
                                  return_value=new_list_data_models_val) as success_list:
                    with patch.object(BioOsService, "list_data_model_rows",
                                      return_value=new_list_data_model_rows_val) as success_rows_list:
                        with patch.object(BioOsService, "list_data_model_row_ids",
                                          return_value=self.list_data_model_rows_id) as success_rows_id_list:
                            sample_res = self.data_models.read("sample")
                            self.assertEqual(len(sample_res), 1)
                            self.assertIsInstance(sample_res, dict)
                            pandas.testing.assert_frame_equal(sample_res["sample"],
                                                              self.sample_data_to_write)

                            new_sample_res = self.data_models.read(new_sample)
                            self.assertEqual(len(new_sample_res), 1)
                            self.assertIsInstance(new_sample_res, dict)
                            pandas.testing.assert_frame_equal(sample_res["sample"],
                                                              new_sample_res[new_sample])

                            self.data_models.delete(new_sample)

                success_list.assert_has_calls([
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                    }),
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                    }),
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                    })
                ])
                success_rows_list.assert_has_calls([
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                        'ID': "dccc0nkleig41ascop42g",
                        'PageSize': 0,
                    }),
                    mock.call({
                        'WorkspaceID': self.workspace_id,
                        'ID': new_sample_id,
                        'PageSize': 0,
                    }),
                ])
                success_rows_id_list.assert_called_once_with({
                    'WorkspaceID': self.workspace_id,
                    'ID': new_sample_id,
                })
                success_delete.assert_called_once_with({
                    'WorkspaceID': self.workspace_id,
                    'ID': new_sample_id,
                    'RowIDs': ['your-sample-1-id', 'your-sample-2-id', 'your-sample-changed']
                })

                new_list_data_models_val["TotalCount"] = 9
                new_list_data_models_val["Items"] = new_list_data_models_val["Items"][:-1]
                with patch.object(BioOsService, "list_data_models",
                                  return_value=new_list_data_models_val) as success_list:
                    try:
                        self.data_models.delete(new_sample)
                    except NotFoundError as e:
                        self.assertEqual(e.message, f"target '{new_sample}' not found")
