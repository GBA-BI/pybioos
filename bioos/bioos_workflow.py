import argparse
import json
import logging
import os
import time

import pandas as pd

from bioos import bioos
from bioos.errors import NotFoundError


def recognize_files_from_input_json(workflow_input_json: dict) -> dict:
    putative_files = {}

    # this version only support absolute path

    for key, value in workflow_input_json.items():
        if str(value).startswith("s3"):
            continue

        if "registry-vpc" in str(value):
            continue

        if "/" in str(value):
            putative_files[key] = value

    return putative_files


def get_logger():
    global LOGGER

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)

    return LOGGER


# 引入langchain，利用dash起一个界面？
# 将启明的内容整合进来
# 与Workspace SPEC做映射
class Bioos_workflow:

    def __init__(self, workspace_name: str, workflow_name: str) -> None:
        # global LOGGER
        self.logger = get_logger()

        # get workspace id
        df = bioos.list_workspaces()
        ser = df[df.Name == workspace_name].ID
        if len(ser) != 1:
            raise NotFoundError("Workspace", workspace_name)
        workspace_id = ser.to_list()[0]

        self.ws = bioos.workspace(workspace_id)
        self.wf = self.ws.workflow(name=workflow_name)

    # 需要有推定上传目的地址的机制，由WES endpoint的配置来指定
    def input_provision(self, workflow_input_json: dict, force: bool = False):
        # need to support different source and target
        # 输入的是WDL的标准json，有两种形式，单例的{}和多例的[{}]，为简单表述，这里以单例形式处理

        # find files
        putative_files = recognize_files_from_input_json(workflow_input_json)

        # upload files
        update_dict = {}
        df = self.ws.files.list('input_provision')
        uploaded_files = [] if df.empty else df.key.to_list()

        # 这里可以改为数组上传
        for key, value in putative_files.items():
            target = f"input_provision/{os.path.basename(value)}"

            # skip existed file upload
            if not force and target in uploaded_files:
                self.logger.info(f"Skip tos existed file {value}")
                continue

            # 这里的target是prefix
            self.logger.info(f"Start upload {value}.")
            self.ws.files.upload(value,
                                 target="input_provision/",
                                 flatten=True)
            self.logger.info(f"Finish upload {value}.")

            s3_location = self.ws.files.s3_urls(target)[0]
            update_dict[key] = s3_location

        # update json
        workflow_input_json.update(update_dict)
        return workflow_input_json

    def output_provision(self):
        pass

    def preprocess(self,
                   input_json_file: str,
                   data_model_name: str = "dm",
                   submission_desc: str = "Submit by pybioos",
                   call_caching: bool = True,
                   force_reupload: bool = False):
        input_json = json.load(open(input_json_file))
        self.logger.info("Load json input successfully.")

        # 将单例的模式转换成向量形式
        if isinstance(input_json, list):
            inputs_list = input_json
        else:
            inputs_list = [
                input_json,
            ]

        # 处理provision，更新inputs_list
        inputs_list_update = []
        for input_dict in inputs_list:
            input_dict_update = self.input_provision(input_dict,
                                                     force_reupload)
            inputs_list_update.append(input_dict_update)

        # 生成datamodel并上传
        # 这里还需要处理id列的内容
        df = pd.DataFrame(inputs_list_update)
        id_col = f"{data_model_name}_id"
        columns = [
            id_col,
        ]
        columns.extend(df.columns)
        df[id_col] = [f"tmp_{x}" for x in list(range(len(df)))]
        df = df.reindex(columns=columns)
        columns = [key.split(".")[-1] for key in df.columns.to_list()]
        df.columns = pd.Index(columns)

        # 这里可能要对每次新上传的datamodel进行重命名
        # 这里经证实只支持全str类型的df
        self.ws.data_models.write({data_model_name: df.map(str)}, force=True)
        self.logger.info("Set data model successfully.")

        # 生成veapi需要的输入结构
        unupdate_dict = inputs_list[0]
        for key, value in unupdate_dict.items():
            unupdate_dict[key] = f'this.{key.split(".")[-1]}'

        self.params_submit = {
            "inputs": json.dumps(unupdate_dict),
            "outputs": "{}",
            "data_model_name": data_model_name,
            "row_ids": df[id_col].to_list(),
            "submission_desc": submission_desc,
            "call_caching": call_caching,
        }
        self.logger.info("Build submission params successfully.")

        return self.params_submit

    def postprocess(self, download=False):
        # 假设全部执行完毕
        #  对运行完成的目录进行下载
        # 证实bioos包只能对文件的list进行下载，不支持文件夹
        # ws.files.list方法不能指定起始路径，需要改进
        # 需要有一个地方执行定时任务，对run的status进行查询，并记录状态，对每次新完成的run进行后处理
        files = []
        for file in self.ws.files.list().key:
            for run in self.runs:
                if run.submission in file:
                    print(file)

                    files.append(file)

        if download:
            try:
                self.ws.files.download(files, ".", flatten=False)
            except Exception as e:
                print(f'Some file can not download. \n {e}')

            self.logger.info("Download finish.")

    def submit_workflow_bioosapi(self):
        self.runs = self.wf.submit(**self.params_submit)
        self.logger.info("Submit workflow run successfully.")
        return self.runs

    def monitor_workflow(self):
        # wf是否有对应的查询方法
        runs = []
        for run in self.runs:
            run.sync()
            runs.append(run)

        self.runs = runs
        return self.runs


def bioos_workflow():

    # argparse
    parser = argparse.ArgumentParser(
        description="Bio-OS instance platform workflow submitter program.")
    parser.add_argument("--endpoint",
                        type=str,
                        help="Bio-OS instance platform endpoint",
                        default="https://bio-top.miracle.ac.cn")
    parser.add_argument(
        "--ak",
        type=str,
        help="Access_key for your Bio-OS instance platform account.")
    parser.add_argument(
        "--sk",
        type=str,
        help="Secret_key for your Bio-OS instance platform account.")

    parser.add_argument("--workspace_name",
                        type=str,
                        help="Target workspace name.")
    parser.add_argument("--workflow_name",
                        type=str,
                        help="Target workflow name.")
    parser.add_argument(
        "--input_json",
        type=str,
        help="The input_json file in Cromwell Womtools format.")
    parser.add_argument(
        "--data_model_name",
        type=str,
        help=
        "Intended name for the generated data_model on the Bio-OS instance platform workspace page.",
        default="dm")
    parser.add_argument(
        "--call_caching",
        action='store_true',
        help="Call_caching for the submission run.",
    )
    parser.add_argument('--submission_desc',
                        type=str,
                        help="Description for the submission run.",
                        default="Submit by pybioos.")
    parser.add_argument('--force_reupload',
                        action='store_true',
                        help="Force reupolad tos existed files.")

    parser.add_argument(
        "--monitor",
        action='store_true',
        help="Moniter the status of submission run until finishment.")
    parser.add_argument(
        "--monitor_interval",
        type=int,
        default=600,
        help="Time interval for query the status for the submission runs.")
    parser.add_argument(
        "--download_results",
        action='store_true',
        help="Download the submission run result files to local current path.")

    parsed_args = parser.parse_args()

    # login and submit
    bioos.login(endpoint=parsed_args.endpoint,
                access_key=parsed_args.ak,
                secret_key=parsed_args.sk)
    bw = Bioos_workflow(workspace_name=parsed_args.workspace_name,
                        workflow_name=parsed_args.workflow_name)
    bw.preprocess(input_json_file=parsed_args.input_json,
                  data_model_name=parsed_args.data_model_name,
                  submission_desc=parsed_args.submission_desc,
                  call_caching=parsed_args.call_caching,
                  force_reupload=parsed_args.force_reupload)
    bw.submit_workflow_bioosapi()

    # moniter
    def all_runs_done() -> bool:

        statuses = []
        for run in bw.runs:
            statuses.append(True if run.status in ("Succeeded",
                                                   "Failed") else False)

        return all(statuses)

    if parsed_args.monitor or parsed_args.download_results:
        while not all_runs_done():
            bw.logger.info("Monitoring submission run.")
            print(bw.runs)
            time.sleep(parsed_args.monitor_interval)
            bw.monitor_workflow()

        time.sleep(60)
        bw.postprocess(download=parsed_args.download_results)
