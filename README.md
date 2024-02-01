# pybioos

Python SDK for Bio-OS.

# Installation
From PYPI.
```
$ pip install pybioos
```

From source code.
```
$ git clone https://github.com/GBA-BI/pybioos.git
$ cd pybioos
$ python setup.py install
```

# Example usage
See Example_usage.ipynb


Or use as CLI command:
```
$ bw -h
usage: bw [-h] [--endpoint ENDPOINT] [--ak AK] [--sk SK] [--workspace_name WORKSPACE_NAME] [--workflow_name WORKFLOW_NAME] [--input_json INPUT_JSON]
          [--data_model_name DATA_MODEL_NAME] [--call_caching] [--submission_desc SUBMISSION_DESC] [--force_reupload] [--monitor]
          [--monitor_interval MONITOR_INTERVAL] [--download_results]

Bio-OS instance platform workflow submitter program.

options:
  -h, --help            show this help message and exit
  --endpoint ENDPOINT   Bio-OS instance platform endpoint
  --ak AK               Access_key for your Bio-OS instance platform account.
  --sk SK               Secret_key for your Bio-OS instance platform account.
  --workspace_name WORKSPACE_NAME
                        Target workspace name.
  --workflow_name WORKFLOW_NAME
                        Target workflow name.
  --input_json INPUT_JSON
                        The input_json file in Cromwell Womtools format.
  --data_model_name DATA_MODEL_NAME
                        Intended name for the generated data_model on the Bio-OS instance platform workspace page.
  --call_caching        Call_caching for the submission run.
  --submission_desc SUBMISSION_DESC
                        Description for the submission run.
  --force_reupload      Force reupolad tos existed files.
  --monitor             Moniter the status of submission run until finishment.
  --monitor_interval MONITOR_INTERVAL
                        Time interval for query the status for the submission runs.
  --download_results    Download the submission run result files to local current path.
```
