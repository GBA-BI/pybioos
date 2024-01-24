# coding:utf-8
import csv
import json
import os
import re
from datetime import datetime

from bioos.errors import NotFoundError, ParameterError
from bioos.service.config import BioOsServiceConfig as conf
from bioos.utils import workflows


def __set_env():
    conf.set_env()


def set_credential(accesskey, secret):
    conf.set_access_key(accesskey)
    conf.set_secret_key(secret)
    conf.set_env()


def upload_entity_table(csvfile, table_name=None):
    __set_env()
    if not table_name:
        table_name = re.sub('\.csv$', '', os.path.basename(csvfile))
    headers = None
    rows = []
    with open(csvfile, 'rt', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if headers is None:
                headers = row
            else:
                rows.append(row)
    params = {
        'WorkspaceID': conf.workspace_id(),
        'Name': table_name,
        'Headers': headers,
        'Rows': rows
    }
    conf.service().create_data_model(params)


def list_entity_tables():
    __set_env()
    params = {'WorkspaceID': conf.workspace_id()}
    result = conf.service().list_data_models(params)
    entities = []
    for table in result['Items']:
        if table['Type'] == 'normal':
            entities.append(table)
    return entities


def get_entity_table(table_name, page_number=1, page_size=10):
    l = list_entity_tables()
    res = None
    for table in l:
        if table['Name'] == table_name:
            res = table
    if not res:
        return None
    params = {
        'WorkspaceID': conf.workspace_id(),
        'ID': res['ID'],
        'PageNumber': page_number,
        'PageSize': page_size
    }
    content = conf.service().list_data_model_rows(params)
    res['Headers'] = content['Headers']
    res['Rows'] = content['Rows']
    res['RowTotalCount'] = content['TotalCount']
    return res


def delete_entity_table(table_name):
    __set_env()
    table = get_entity_table(table_name, page_size=1)
    if not table:
        raise NotFoundError('Table', table_name)
    params = {'WorkspaceID': conf.workspace_id(), 'ID': table['ID']}
    ids = conf.service().list_data_model_row_ids(params)
    params = {
        'WorkspaceID': conf.workspace_id(),
        'ID': table['ID'],
        'RowIDs': ids['RowIDs']
    }
    conf.service().delete_data_model_rows_and_headers(params)


def delete_entities(table_name, row_ids):
    if not row_ids:
        return
    __set_env()
    table = get_entity_table(table_name, page_size=1)
    if not table:
        raise NotFoundError('Table', table_name)
    params = {
        'WorkspaceID': conf.workspace_id(),
        'ID': table['ID'],
        'RowIDs': row_ids
    }
    conf.service().delete_data_model_rows_and_headers(params)


def delete_entity_table_headers(table_name, headers):
    if not headers:
        return
    __set_env()
    table = get_entity_table(table_name, page_size=1)
    if not table:
        raise NotFoundError('Table', table_name)
    params = {
        'WorkspaceID': conf.workspace_id(),
        'ID': table['ID'],
        'Headers': headers
    }
    conf.service().delete_data_model_rows_and_headers(params)


def list_workflows(search_keyword=None, page_number=1, page_size=10):
    __set_env()
    params = {
        'WorkspaceID': conf.workspace_id(),
        'SortBy': 'CreateTime',
        'PageNumber': page_number,
        'PageSize': page_size
    }
    if search_keyword:
        params['Filter'] = {'Keyword': search_keyword}
    return conf.service().list_workflows(params).get('Items')


def get_workflow(workflow_name):
    lis = list_workflows(search_keyword=workflow_name)
    if not lis:
        return None
    lis = [x for x in lis if x['Name'] == workflow_name]
    if not lis:
        return None
    params = {
        'WorkspaceID': conf.workspace_id(),
        'Filter': {
            'IDs': [lis[0].get('ID')]
        }
    }
    workflows = conf.service().list_workflows(params).get('Items')
    if len(workflows) != 1:
        return None
    detail = workflows[0]
    res = lis[0]
    for k, v in detail.items():
        if k != 'Item':
            res[k] = v
    return res


def add_submission(workflow_name,
                   table_name,
                   row_ids,
                   cluster_id,
                   inputs={},
                   outputs={},
                   submission_name_suffix=None,
                   submission_desc=None,
                   call_caching=True):
    if not row_ids or not isinstance(row_ids, list):
        raise ParameterError('row_ids')
    if not inputs and not isinstance(inputs, dict):
        raise ParameterError('inputs')
    if not outputs and not isinstance(outputs, dict):
        raise ParameterError('outputs')

    table = get_entity_table(table_name, page_size=1)
    if not table:
        raise NotFoundError('Table', table_name)
    workflow = get_workflow(workflow_name)
    if not workflow:
        raise NotFoundError('Workflow', workflow_name)
    if not submission_name_suffix:
        submission_name_suffix = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    params = {
        "ClusterID": cluster_id,
        'WorkspaceID': conf.workspace_id(),
        'WorkflowID': workflow['ID'],
        'Name': submission_name(workflow_name, submission_name_suffix),
        'Description': submission_desc,
        'DataModelID': table['ID'],
        'DataModelRowIDs': row_ids,
        'Inputs': json.dumps(inputs),
        'ExposedOptions': {
            "ReadFromCache": call_caching
        },
        'Outputs': json.dumps(outputs)
    }
    conf.service().create_submission(params)


def list_submissions(workflow_name=None,
                     search_keyword=None,
                     status=None,
                     cluster_id=None,
                     page_number=1,
                     page_size=10):
    __set_env()
    params = {
        'WorkspaceID': conf.workspace_id(),
        'PageNumber': page_number,
        'PageSize': page_size,
        'Filter': {}
    }
    if workflow_name:
        workflow = get_workflow(workflow_name)
        if not workflow:
            raise NotFoundError('Workflow', workflow_name)
        params['Filter']['WorkflowID'] = workflow['ID']
    if search_keyword:
        params['Filter']['Keyword'] = search_keyword
    if status:
        params['Filter']['Status'] = status
    if cluster_id:
        params['Filter']['ClusterID'] = cluster_id

    return conf.service().list_submissions(params).get('Items')


def get_submission(submission_name):
    lis = list_submissions(search_keyword=submission_name)
    if not lis:
        raise NotFoundError('Submission', submission_name)
    lis = [x for x in lis if x['Name'] == submission_name]
    if not lis or len(lis) < 1:
        return None
    submission = lis[0]
    # list data entity rows by call list runs
    runs = conf.service().list_runs({
        'WorkspaceID': submission.get("WorkflowID"),
        "SubmissionID": submission.get("ID"),
    }).get("Items")
    data_entity_row_ids = set()
    for run in runs:
        if run.get("DataEntityRowID") != "":
            data_entity_row_ids.add(run.get("DataEntityRowID"))
    # get data model name by call list data models
    models = conf.service().list_data_models({
        'WorkspaceID':
        submission.get("WorkflowID"),
    }).get("Items")
    data_model = ""
    for model in models:
        if model["ID"] == submission.get["DataModelID"]:
            data_model = model.get("Name")
            break
    submission["DataEntity"] = {
        "ID": submission.get["DataModelID"],
        "Name": data_model,
        "RowIDs": list(data_entity_row_ids)
    }

    return submission


def delete_submission(submission_name):
    submission = get_submission(submission_name)
    if not submission:
        raise NotFoundError('Submission', submission_name)
    params = {'WorkspaceID': conf.workspace_id(), 'ID': submission['ID']}
    conf.service().delete_submission(params)


def list_cluster(cluster_type):
    if cluster_type not in ("notebook", "workflow"):
        raise ParameterError("cluster_type")
    params = {'Type': cluster_type, 'ID': conf.workspace_id()}
    clusters = conf.service().list_cluster(params).get('Items')
    res = []
    for cluster in clusters:
        info = cluster["ClusterInfo"]
        if info['Status'] == "Running":
            res.append({
                "cluster_id": info['ID'],
                "name": info['Name'],
                "description": info['Description'],
                "type": cluster["Type"]
            })
    return res


def submission_name(workflow_name, submission_name_suffix):
    return workflows.submission_name(workflow_name, submission_name_suffix)
