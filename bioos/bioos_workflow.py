import json
import pandas as pd
from bioos import bioos
from bioos.errors import NotFoundError
import os
import logging



def recognize_files_from_input_json(workflow_input_json:dict) ->dict:
    putative_files={}

    # this version only support absolute path

    for key,value in workflow_input_json.items():
        if str(value).startswith('s3'):
            continue

        if 'registry-vpc' in str(value):
            continue

        if '/' in str(value):
            putative_files[key]=value

    return putative_files


def get_logger():
    global LOGGER

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")

    handler=logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)

    return LOGGER



# 引入langchain，利用dash起一个界面？
# 将启明的内容整合进来
# 与Workspace SPEC做映射
class Bioos_workflow():

    def __init__(self,workspace_name:str,workflow_name:str) -> None:
        #global LOGGER
        self.logger=get_logger()

        # get workspace id
        df=bioos.list_workspaces()
        ser=df[df.Name==workspace_name].ID
        if len(ser) != 1:
            raise NotFoundError('Workspace',workspace_name)
        workspace_id=ser[0]

        self.ws=bioos.workspace(workspace_id)
        self.wf=self.ws.workflow(name=workflow_name)


    # 需要有推定上传目的地址的机制，由WES endpoint的配置来指定
    def input_provision(self,workflow_input_json:dict):
        # need to support different source and target
        # 输入的是WDL的标准json，有两种形式，单例的{}和多例的[{}]，为简单表述，这里以单例形式处理

        # find files
        putative_files = recognize_files_from_input_json(workflow_input_json)

        # upload files
        update_dict={}
        for key,value in putative_files.items():
            target=f'input_provision/{os.path.basename(value)}'
            # 这里如果多行记录，即多个样本的run中有相同的文件，可能会触发多次上传。可能可以通过判断文件是否存在来判断
            # 需要对file的存在性进行检验
            # 这里的target是prefix
            self.ws.files.upload(value,target="input_provision/",flatten=True)

            s3_location=self.ws.files.s3_urls(target)[0]
            update_dict[key]=s3_location

        # update json 
        workflow_input_json.update(update_dict)
        return workflow_input_json
      

    def output_provision(self):

        pass
    
    def preprocess(self,input_json_file:str, data_model_name:str='dm',submission_desc:str='Submit by pybioos',call_caching:bool=True):
        input_json=json.load(open(input_json_file))
        self.logger.info('Load json input successfully.')

        # 将单例的模式转换成向量形式
        if type(input_json)==list:
            inputs_list=input_json
        else:
            inputs_list=[input_json,]

        # 处理provision，更新inputs_list
        inputs_list_update=[]
        for input_dict in inputs_list:
            input_dict_update=self.input_provision(input_dict)
            inputs_list_update.append(input_dict_update)

        # 生成datamodel并上传 
        # 这里还需要处理id列的内容
        df=pd.DataFrame(inputs_list_update)
        id_col=f'{data_model_name}_id'
        columns=[id_col,]
        columns.extend(df.columns)
        df[id_col]=[f'tmp_{x}' for x in list(range(len(df)))]
        df=df.reindex(columns=columns)
        columns=[key.split('.')[-1] for key in df.columns.to_list()]
        df.columns=pd.Index(columns)

        #这里可能要对每次新上传的datamodel进行重命名
        #这里经证实只支持全str类型的df
        self.ws.data_models.write({data_model_name:df.map(str)},force=True)
        self.logger.info('Set data model successfully.')

        # 生成veapi需要的输入结构
        unupdate_dict=inputs_list[0]
        for key,value in unupdate_dict.items():
            unupdate_dict[key]=f'this.{key.split('.')[-1]}'

        self.params_submit={
            'inputs':json.dumps(unupdate_dict),
            'outputs':'{}',
            'data_model_name':data_model_name,
            'row_ids':df[id_col].to_list(),
            'submission_desc':submission_desc,
            'call_caching':call_caching
        }
        self.logger.info('Build submission params successfully.')


        return self.params_submit
    

    def postprocess(self,download=False):
        # 假设全部执行完毕
        #  对运行完成的目录进行下载
        # 证实bioos包只能对文件的list进行下载，不支持文件夹
        # ws.files.list方法不能指定起始路径，需要改进
        # 需要有一个地方执行定时任务，对run的status进行查询，并记录状态，对每次新完成的run进行后处理
        files=[]
        for file in self.ws.files.list().key:
            for run in self.runs:
                if run.submission in file:
                    files.append(file)

        if download==True:
            self.ws.files.download(files,'.',flatten=False) 
            self.logger.info(f'Download finish.')


    def submit_workflow_bioosapi(self):
        self.runs=self.wf.submit(**self.params_submit)
        self.logger.info('Submit workflow run successfully.')
        return self.runs


    def monitor_workflow(self):
        # wf是否有对应的查询方法
        runs=[]
        for run in self.runs:
            run.sync()
            runs.append(run)

        self.runs=runs
        return self.runs

def main():
    bw=Bioos_workflow()
    bw.preprocess('/Users/liujilong/develop/bioos/test/test2.json')
    bw.submit_workflow_bioosapi()
    bw.monitor_workflow()
    bw.postprocess()

