import hashlib
import math
import os
import re
from typing import List

import tos
from tos import DataTransferType, HttpMethodType
from tos.exceptions import TosClientError
from tos.models2 import DeleteError, ListedObject, ObjectTobeDeleted

from bioos.config import Config
from bioos.errors import ParameterError
from bioos.log import Logger

DEFAULT_THREAD = 10
LIST_OBJECT_MAX_KEYS = 1000
SIMPLE_UPLOAD_LIMITATION = 1024 * 1024 * 100
ONE_BATCH_WRITE_SIZE = 1024 * 1024 * 10
MAX_ALLOWED_PARTS = 10000
MIN_PART_SIZE = 1024 * 1024 * 5
ONE_BATCH_REQUEST = 50
ONE_BATCH_MAX_DELETE = 1000
REFRESH_TOKEN_TIME_BEFORE_EXPIRE = 20 * 60

CRC_CHECK_ERROR_PREFIX = "Check CRC failed"


def tos_percentage(
        consumed_bytes,
        total_bytes,
        rw_once_bytes,  # 类似进度条的报告
        type_: DataTransferType):
    if rw_once_bytes == 0:
        return
    parts_num = math.ceil(float(total_bytes) / float(rw_once_bytes))
    cur_part = math.ceil(float(consumed_bytes) / float(rw_once_bytes))
    notify_num = int(parts_num / 10)
    if total_bytes and notify_num and cur_part % notify_num == 0:
        rate = int(100 * float(consumed_bytes) / float(total_bytes))
        Config.Logger.info(
            "rate:{}, consumed_bytes:{},total_bytes:{}, rw_once_bytes:{}".
            format(rate, consumed_bytes, total_bytes, rw_once_bytes))


class TOSHandler:

    def __init__(
            self,
            client: tos.clientv2.TosClientV2,  # fixed
            bucket: str,
            logger: Logger = Config.Logger):
        # client should be with federation_credential
        self._client = client
        self._bucket = bucket

        self._debug_logging = logger.debug
        self._info_logging = logger.info
        self._warn_logging = logger.warn
        self._error_logging = logger.error

    def _is_crc_check_error(self, error: TosClientError) -> bool:
        if not isinstance(error, TosClientError):
            return False
        if not error.message:
            return False
        return error.message.startswith(CRC_CHECK_ERROR_PREFIX)

    def presign_download_url(self, file_path: str, duration: int) -> str:
        return self._client.pre_signed_url(HttpMethodType.Http_Method_Get,
                                           self._bucket, file_path,
                                           duration).signed_url

    def object_exists(self, file_path: str) -> bool:
        try:
            self._client.head_object(bucket=self._bucket, key=file_path)
            return True
        except tos.exceptions.TosServerError as err:
            if err.status_code == 404:
                return False
            raise

    def _resolve_upload_key(self,
                            file_path: str,
                            target_path: str,
                            flatten: bool) -> str:
        if flatten:
            to_upload_path = os.path.basename(file_path)
        else:
            to_upload_path = os.path.normpath(file_path)

        if os.path.isabs(to_upload_path):
            to_upload_path = to_upload_path.lstrip("/")

        return os.path.normpath(os.path.join(target_path, to_upload_path))

    def _build_upload_checkpoint_file(self,
                                      file_path: str,
                                      tos_target_path: str,
                                      checkpoint_dir: str) -> str:
        if not checkpoint_dir:
            return None

        resolved_checkpoint_dir = os.path.abspath(
            os.path.expanduser(checkpoint_dir))
        os.makedirs(resolved_checkpoint_dir, exist_ok=True)
        checkpoint_key = hashlib.sha256(
            f"{self._bucket}\0{tos_target_path}\0{os.path.abspath(file_path)}".
            encode("utf-8")).hexdigest()
        return os.path.join(resolved_checkpoint_dir,
                            f"{checkpoint_key}.upload.ckpt")

    def _upload_small_file(self, file_path: str, tos_target_path: str):
        self._client.put_object_from_file(
            bucket=self._bucket,
            key=tos_target_path,
            file_path=file_path,
            # don't show progress while uploading small file
            # data_transfer_listener=tos_percentage
        )

    def _upload_big_file(self,
                         file_path: str,
                         tos_target_path: str,
                         fsize: int,
                         checkpoint_dir: str,
                         task_num: int):
        part_size = max(int(fsize / MAX_ALLOWED_PARTS) + 1, MIN_PART_SIZE)
        checkpoint_file = self._build_upload_checkpoint_file(
            file_path, tos_target_path, checkpoint_dir)
        self._client.upload_file(
            bucket=self._bucket,
            key=tos_target_path,
            file_path=file_path,
            part_size=part_size,
            task_num=task_num,
            enable_checkpoint=checkpoint_file is not None,
            checkpoint_file=checkpoint_file,
            data_transfer_listener=tos_percentage)

    def _upload_with_retry(self,
                           file_path: str,
                           tos_target_path: str,
                           fsize: int,
                           checkpoint_dir: str,
                           max_retries: int,
                           task_num: int):
        total_attempts = max_retries + 1
        for attempt_index in range(total_attempts):
            try:
                if fsize <= SIMPLE_UPLOAD_LIMITATION:
                    self._upload_small_file(file_path, tos_target_path)
                else:
                    self._upload_big_file(file_path, tos_target_path, fsize,
                                          checkpoint_dir, task_num)
                return
            except Exception as err:
                if attempt_index == total_attempts - 1:
                    raise
                self._warn_logging(
                    f"upload {tos_target_path} failed on attempt "
                    f"{attempt_index + 1}/{total_attempts}: {err}. Retrying...")

    def list_objects(self, target_path: str, num: int) -> List[ListedObject]:
        object_list = []
        if num != 0:
            if num <= LIST_OBJECT_MAX_KEYS:
                resp = self._client.list_objects(bucket=self._bucket,
                                                 prefix=target_path,
                                                 max_keys=num)
                object_list = resp.contents
            else:
                remain = num
                cur_marker = None
                while True:
                    if remain <= LIST_OBJECT_MAX_KEYS:
                        object_list += self._client.list_objects(
                            bucket=self._bucket,
                            prefix=target_path,
                            marker=cur_marker,
                            max_keys=remain).contents
                        break
                    else:
                        resp = self._client.list_objects(
                            bucket=self._bucket,
                            prefix=target_path,
                            marker=cur_marker,
                            max_keys=LIST_OBJECT_MAX_KEYS)
                        object_list += resp.contents
                        if not resp.is_truncated:
                            break
                        cur_marker = resp.next_marker
                        remain = remain - LIST_OBJECT_MAX_KEYS

        else:
            cur_marker = None
            while True:
                resp = self._client.list_objects(bucket=self._bucket,
                                                 prefix=target_path,
                                                 marker=cur_marker,
                                                 max_keys=LIST_OBJECT_MAX_KEYS)
                object_list += resp.contents
                if not resp.is_truncated:
                    break
                cur_marker = resp.next_marker
        return object_list

    def upload_objects(
        self,
        files_to_upload: List[str],
        target_path: str,
        flatten: bool,
        ignore: str = "",
        include: str = "",
        checkpoint_dir: str = "",
        max_retries: int = 3,
        task_num: int = DEFAULT_THREAD,
    ) -> List[str]:

        def _upload_fail(error_list_: List[str], file_path_: str):
            error_list_.append(file_path_)

        files_to_upload = self.files_filter(files_to_upload, include, ignore)
        max_retries = max(int(max_retries) if max_retries is not None else 3, 0)
        task_num = max(int(task_num) if task_num is not None else DEFAULT_THREAD, 1)
        if len(files_to_upload) == 0:
            self._info_logging("no files to upload")
            return []

        error_list = []
        for file_path in files_to_upload:
            if not os.path.isfile(file_path):
                error_list.append(file_path)
                self._error_logging(f"'{file_path}' is not a file")
                continue
            fsize = os.path.getsize(file_path)
            tos_target_path = self._resolve_upload_key(file_path, target_path,
                                                       flatten)

            self._info_logging(
                f"[{file_path}] begins to upload to [{tos_target_path}]")

            try:
                if fsize == 0:
                    self._error_logging(
                        f"can not upload empty file {tos_target_path}")
                    _upload_fail(error_list, file_path)
                    continue
                self._upload_with_retry(file_path, tos_target_path, fsize,
                                        checkpoint_dir, max_retries,
                                        task_num)
            except Exception as err_:
                if self._is_crc_check_error(err_):
                    self._warn_logging(f"CRC check {tos_target_path} failed, "
                                       f"pls delete the uploaded file by hand")
                self._error_logging(f"upload {tos_target_path} failed: {err_}")
                _upload_fail(error_list, file_path)
                continue

            self._info_logging(f"{file_path} uploads succeed")

        if error_list:
            self._error_logging(
                f"{len(error_list)} uploaded failed, please upload them again: "
                f"\n{error_list}")

        return error_list

    def download_objects(self,
                         files_to_download: List[str],
                         local_path: str,
                         flatten: bool,
                         ignore: str = "",
                         include: str = "",
                         force: bool = True) -> List[str]:
        files_to_download = self.files_filter(files_to_download, include,
                                              ignore)

        files_failed = []
        if len(files_to_download) == 0:
            self._info_logging("no files to download")
            return

        for f in files_to_download:
            # handle the situation that the file on internal with the name formates "xxx/"
            if len(f) > 0 and f[-1] == "/":
                self._warn_logging(
                    "can't download the file with the name formats 'xxx/'")
                continue

            local_target_path = os.path.basename(
                f) if flatten else os.path.normpath(f)

            if not force:
                if os.path.isfile(local_target_path):
                    self._debug_logging(
                        f"skip downloading {local_target_path}")
                    continue

            try:
                resp = self._client.head_object(bucket=self._bucket, key=f)
                fsize_ = resp.content_length
                part_size = max(
                    int(fsize_ / MAX_ALLOWED_PARTS) + 1, MIN_PART_SIZE)

                # target文件名确定
                actual_file_path = os.path.join(local_path, local_target_path)
                self._info_logging(
                    f"[{f}] begins to download to [{actual_file_path}]")
                self._client.download_file(
                    bucket=self._bucket,
                    key=f,
                    file_path=actual_file_path,
                    part_size=part_size,
                    task_num=DEFAULT_THREAD,
                    data_transfer_listener=tos_percentage)
                self._info_logging(f"[{f}] download successfully.")
            except tos.exceptions.TosServerError as e:
                if e.status_code == 404:
                    self._warn_logging(f"'{f}' not found")
                    files_failed.append(f)
            except Exception as err_:
                raise err_
                if self._is_crc_check_error(err_):
                    self._warn_logging(
                        f"CRC check {actual_file_path} failed, file will be removed"
                    )
                    os.remove(actual_file_path)
                self._error_logging(f"download {f} failed: {err_}")
                files_failed.append(f)

        if len(files_failed) > 0:
            self._warn_logging(f"failed to download {files_failed}")
        return files_failed

    def delete_objects(self, files_to_delete: List[str], ignore: str = "", include: str = "") \
            -> List[DeleteError]:
        files_to_delete = self.files_filter(files_to_delete, include, ignore)

        if len(files_to_delete) == 0:
            self._info_logging("no files to delete")
            return

        cur = 0
        cur_end = min((cur + ONE_BATCH_MAX_DELETE), len(files_to_delete))
        error_list = []
        while cur < len(files_to_delete):
            # default quiet mode will only return error_list
            resp = self._client.delete_multi_objects(
                bucket=self._bucket,
                objects=[
                    ObjectTobeDeleted(f) for f in files_to_delete[cur:cur_end]
                ])
            cur = cur_end
            cur_end = min((cur + ONE_BATCH_MAX_DELETE), len(files_to_delete))
            if len(resp.error) != 0:
                error_list += resp.error_list
        if len(error_list) > 0:
            self._info_logging(
                f"{len(error_list)} files left undeleted: {[err.key for err in error_list]}."
            )
        return error_list

    def files_filter(self,
                     files: List[str],
                     include: str = "",
                     ignore: str = "") -> List[str]:
        file_lst = []
        for f in files:
            if f.endswith("/"):
                raise ParameterError("tos files path")
            basename = os.path.basename(os.path.normpath(f))
            if include != "":
                if not re.fullmatch(include, basename) or (
                        ignore != "" and re.fullmatch(ignore, basename)):
                    continue
            else:
                if ignore != "" and re.fullmatch(ignore, basename):
                    continue

            file_lst.append(f)
        return file_lst
