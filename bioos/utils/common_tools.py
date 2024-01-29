import json
import threading

# helper_convert_pattern1 = re.compile(r'(.)([A-Z][a-z]+)')
# helper_convert_pattern2 = re.compile(r'([a-z0-9])([A-Z])')
#
#
# def camel_to_snake(case: str) -> str:
#     return helper_convert_pattern2.sub(
#         r'\1_\2',
#         helper_convert_pattern1.sub(
#             r'\1_\2',
#             case
#         )
#     ).lower()


def dict_str(dic: dict) -> str:  # 初始化dict的打印格式
    res_str = ""
    for k, v in dic.items():
        res_str += f"{k}: {v}\n"
    return res_str


def s3_endpoint_mapping(s3_endpoint: str) -> str:  #替换
    return s3_endpoint.replace("tos-s3", "tos")


def is_json(myjson: str):  # 通过loads函数来判断是否是json结构，捕获错误规避异常
    try:
        json.loads(myjson)
    except ValueError:
        return False
    return True


def submission_name(workflow_name, submission_name_suffix):
    return '{}-history-{}'.format(workflow_name, submission_name_suffix)


def instance_key(cls, *args, **kwargs):
    return cls.__name__ + "%" + str(args) + "%" + str(kwargs)


class SingletonType(type):
    _instance_lock = threading.RLock()

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = {}
        key = instance_key(cls, *args, **kwargs)
        if key not in getattr(cls, "_instance").keys():
            with SingletonType._instance_lock:
                if key not in getattr(cls, "_instance").keys():
                    cls._instance[key] = super(SingletonType,
                                               cls).__call__(*args, **kwargs)
        return cls._instance[key]
