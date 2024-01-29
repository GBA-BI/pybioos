import datetime
from typing import Iterable, List, Union

import pandas as pd
import tos
from cachetools import TTLCache, cached
from pandas import DataFrame
from tos.credential import FederationCredentials, FederationToken
from tos.models2 import ListedObject

from bioos.config import Config
from bioos.errors import ParameterError
from bioos.internal.tos import TOSHandler
from bioos.models.models import DisplayListedObject
from bioos.utils.common_tools import SingletonType, dict_str, s3_endpoint_mapping


class FileResource(metaclass=SingletonType):
    TOS_RETRY_TIMES = 5
    DEFAULT_PRE_SIGNED_TIME = 15 * 60

    def __init__(self, workspace_id=str, bucket=str):
        self.workspace_id = workspace_id
        self.bucket = bucket
        res = Config.service().get_tos_access({
            'WorkspaceID': self.workspace_id,
        })  # 这里触发的是后端的返回行为，返回值无定义python类型
        self.endpoint = s3_endpoint_mapping(res["Endpoint"])
        self.region = res["Region"]
        self.tos_handler = TOSHandler(  #需要注意
            tos.TosClientV2(  # 2.6.6中tos下还要有一层
                ak=None,
                sk=None,
                region=None,
                endpoint=self.endpoint,
                auth=tos.FederationAuth(  # 这里是使用构建的FeterationTokon为进行auth
                    FederationCredentials(
                        self._refresh_federation_credentials), self.region),
                max_retry_count=self.TOS_RETRY_TIMES),
            self.bucket)

    def __repr__(self) -> str:
        info_dict = dict_str({
            "name": self.bucket,
            "endpoint": self.endpoint,
            "region": self.region,
            "size": f"{self.size} bytes",
            "counts": self.counts,
        })
        return f"BucketInfo:\n{info_dict}"

    def _refresh_federation_credentials(self) -> FederationToken:
        res = Config.service().get_tos_access({
            'WorkspaceID': self.workspace_id,
        })

        tos_ak = res["AccessKey"]
        tos_sk = res["SecretKey"]
        sts_token = res["SessionToken"]

        return FederationToken(
            tos_ak, tos_sk, sts_token,
            (datetime.datetime.fromisoformat(res["ExpiredTime"]) -
             datetime.timedelta(minutes=5)).timestamp())

    @property
    @cached(cache=TTLCache(maxsize=10, ttl=1))
    def size(self) -> int:
        """Returns the size of all files.

        :return: size of all files calculated with bytes
        :rtype: int
        """
        size = 0
        for o in self._list_with_cache():
            size += o.size
        return size

    @property
    @cached(cache=TTLCache(maxsize=10, ttl=1))
    def counts(self) -> int:
        """Returns the number of all files .

        :return: number of all files
        :rtype: int
        """
        return len(self._list_with_cache())

    @cached(cache=TTLCache(maxsize=10, ttl=1))
    def _list_with_cache(self) -> List[ListedObject]:  # 需要一个list指定路径的方法
        return self.tos_handler.list_objects("", 0)

    def _build_s3_url(self, file_path) -> str:  #内部使用的相对路径，构建完整的s3
        return f"s3://{self.bucket}/{file_path}"

    def _build_https_url(self, file_path) -> str:  #生成有权限的http
        return self.tos_handler.presign_download_url(
            file_path, self.DEFAULT_PRE_SIGNED_TIME)

    def s3_urls(self, sources: Union[str, Iterable[str]]) -> List[str]:
        """Returns the S3 URLs for all the specified files .

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.files.s3_urls(sources = ['bar/baz.txt']) #output: ["s3://xxxxxx-foo/bar/baz.txt"]

        :param sources: The name of the file or a batch of files
        :type sources: Union[str, Iterable[str]]
        :return: S3 URLS of the file or a batch of file
        :rtype: List[str]
        """
        if isinstance(sources, str):  #为兼容Union必须的操作
            sources = [sources]

        if isinstance(sources, Iterable):
            return [self._build_s3_url(elem) for elem in sources]
        raise ParameterError("sources")

    def https_urls(self, sources: Union[str, Iterable[str]]) -> List[str]:
        """Returns the HTTPS URLs for all the specified files .

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.files.https_urls(sources = ['bar/baz.txt']) #output: ["https://xxxxxx-foo.tos-s3-xxxxxx.volces.com/bar/baz.txt"]

        :param sources: The name of the file or a batch of files
        :type sources: Union[str, Iterable[str]]
        :return: Pre-signed Downloading HTTPS URLS of the file or a batch of file
        :rtype: List[str]
        """
        if isinstance(sources, str):
            sources = [sources]

        if isinstance(sources, Iterable):
            return [self._build_https_url(elem) for elem in sources]
        raise ParameterError("sources")

    def list(self, prefix: str = '') -> DataFrame:  # 这里需要升级，支持list指定的路径，
        """Lists all files' information .

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.files.list()

        :return: files' information
        :rtype: DataFrame
        """
        files = self.tos_handler.list_objects(prefix, 0)
        return pd.DataFrame.from_records([
            DisplayListedObject(f, self._build_s3_url(f.key),
                                self._build_https_url(f.key)).__dict__
            for f in files
        ])

    # TODO support s3 url input in the future
    def download(self, sources: Union[str, Iterable[str]], target: str,
                 flatten: bool) -> bool:
        """Downloads all the specified file from internal tos bucket bound to workspace to
        local path.

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.files.download(sources = ['foo/bar.txt'], target="baz/", flatten=False)

        :param sources: The name of the file or a batch of files from internal tos bucket to download
        :type sources: Union[str, Iterable[str]]
        :param target: Local path
        :type target: str
        :param flatten: Whether to flatten the files locally
        :type flatten: bool
        :return: Downloading result
        :rtype: bool
        """

        if isinstance(sources, str):
            sources = [sources]

        return len(
            self.tos_handler.download_objects(  # 增加异常控制
                sources, target, flatten)) == 0

    def upload(
            self,
            sources: Union[str, Iterable[str]],
            target: str,  #这里需要增加已上传的校验，如已存在同名文件，有skip的选项
            flatten: bool) -> bool:
        """Uploads a local file or a batch of local files to internal tos bucket bound to workspace.

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.files.upload(sources = ['foo/bar.txt'], target="baz/", flatten=False)
        
        :param sources: The name of the local file or a batch of local files to upload
        :type sources: Union[str, Iterable[str]]
        :param target: The target internal path of the file or a batch of files
        :type target: str
        :param flatten: Whether to flatten the files at internal
        :type flatten: bool
        :return: Uploading result
        :rtype: bool
        """
        if isinstance(sources, str):
            sources = [sources]

        return len(self.tos_handler.upload_objects(sources, target,
                                                   flatten)) == 0

    def delete(self, sources: Union[str, Iterable[str]]) -> bool:
        """Deletes the given file from the tos bucket bound to workspace .

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.files.delete(sources = ['foo/bar.txt', "baz.csv"])

        :param sources: The name of the file to delete
        :type sources: Union[str, Iterable[str]]
        :return: Deleting result
        :rtype: bool
        """
        if isinstance(sources, str):
            sources = [sources]
        return len(self.tos_handler.delete_objects(sources)) == 0
