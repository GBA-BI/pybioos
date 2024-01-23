import copy
import hashlib
import os
import random
import shutil
import time
import unittest

from pandas import DataFrame

from bioos.bioos import workspace
from bioos.tests.base import BaseInit


class TestFiles(BaseInit):
    tos_target_dir = f"test_upload_files-{time.time()}"
    local_small_files_dir = "small_files/"
    local_downloads_files_dir = "downloads/"
    small_files = {}
    big_file = {}
    small_num = 5
    big_num = 1

    @classmethod
    def get_file_md5(cls, file_path):
        with open(file_path, 'rb') as f:
            data = f.read()
        return hashlib.md5(data).hexdigest()

    @classmethod
    def generate_random_str(cls, length=16):
        """
            生成一个指定长度的随机字符串
            """
        random_str = ''
        base_str = 'abcdefghigklmnopqrstuvwxyz'
        seed_len = len(base_str) - 1
        for i in range(length):
            random_str += base_str[random.randint(0, seed_len)]
        return random_str

    @classmethod
    def generate_random_file(cls, size, cur_dir="./"):
        """
            生成临时文件。
        """
        if not os.path.exists(cur_dir):
            os.mkdir(cur_dir)
        tmp_file = os.path.join(cur_dir, cls.generate_random_str(8))
        with open(tmp_file, 'wb') as f:
            while size > 0:
                sz = int(min(size, 5 * 1024 * 1024))
                data = os.urandom(sz)
                f.write(data)
                size -= sz
            f.close()
        return tmp_file

    def setUp(self):
        self.files = workspace(self.workspace_id).files
        if not self.really_login:
            return
        # create 5 small files and 1 big file
        for i in range(self.small_num):
            small_file = os.path.normpath(
                self.generate_random_file(5 * 1024 * 1024, self.local_small_files_dir))
            self.small_files[small_file] = self.get_file_md5(small_file)
        big_file = os.path.normpath(self.generate_random_file(200 * 1024 * 1024))
        self.big_file[big_file] = self.get_file_md5(big_file)

    def tearDown(self):
        for f in self.big_file:
            os.remove(f)
        if os.path.exists(self.local_small_files_dir):
            shutil.rmtree(self.local_small_files_dir)
        if os.path.exists(self.local_downloads_files_dir):
            shutil.rmtree(self.local_downloads_files_dir)
        TestFiles.small_files = {}
        TestFiles.big_file = {}

    @unittest.skipUnless(BaseInit.really_login, "need real ak,sk,endpoint,"
                                                "workspace_id")
    def test_1_crud_case(self):
        files = copy.deepcopy(self.small_files)
        files.update(self.big_file)
        self.files.upload(files, self.tos_target_dir, flatten=True)

        list_df = self.files.list()
        self.assertIsInstance(list_df, DataFrame)
        uploaded_files_in_tos = \
            list(list_df.query(f"key.str.startswith('{self.tos_target_dir}')")["key"])
        self.assertEqual(len(uploaded_files_in_tos), self.small_num + self.big_num)

        for f in uploaded_files_in_tos:
            # local_dir will not be mapped to tos
            self.assertNotIn(f"{self.tos_target_dir}/{self.local_small_files_dir}", f)

        self.files.download([os.path.join(self.tos_target_dir, os.path.basename(f)) for f in files],
                            self.local_downloads_files_dir,
                            flatten=True)
        md5_set = set()
        for root, dirs, files in os.walk(self.local_downloads_files_dir):
            self.assertEqual(len(files), self.big_num + self.small_num)
            self.assertEqual(len(dirs), 0)
            for name in files:
                md5_set.add(self.get_file_md5(os.path.join(root, name)))
        self.assertSetEqual(md5_set,
                            set(self.big_file.values()).union(set(self.small_files.values())))

        # clean tos files
        for f in uploaded_files_in_tos:
            self.files.delete(f)

        list_df = self.files.list()
        files_in_tos_after_delete = list_df.query(f"key.str.startswith('{self.tos_target_dir}')")[
            "key"]
        self.assertEqual(len(files_in_tos_after_delete), 0)

        # clean downloads files
        shutil.rmtree(self.local_downloads_files_dir)

    @unittest.skipUnless(BaseInit.really_login, "need real ak,sk,endpoint,"
                                                "workspace_id")
    def test_2_crud_case(self):
        files = copy.deepcopy(self.small_files)
        files.update(self.big_file)
        self.files.upload(files, self.tos_target_dir, flatten=False)

        list_df = self.files.list()
        self.assertIsInstance(list_df, DataFrame)
        uploaded_files_in_tos = \
            list(list_df.query(f"key.str.startswith('{self.tos_target_dir}')")["key"])
        self.assertEqual(len(uploaded_files_in_tos), self.big_num + self.small_num)

        for f in uploaded_files_in_tos:
            # make sure that all small files are in the directory
            if os.path.basename(f) not in files:
                self.assertIn(f"{self.tos_target_dir}/{self.local_small_files_dir}", f)
            # make sure that the only one big file matches
            else:
                self.assertEqual(f"{self.tos_target_dir}/{tuple(self.big_file.keys())[0]}", f)

        self.files.download([os.path.join(self.tos_target_dir, f) for f in files],
                            self.local_downloads_files_dir,
                            flatten=False)
        md5_set = set()
        for root, dirs, files in os.walk(self.local_downloads_files_dir):
            # dir for big_file
            if os.path.samefile(root,
                                os.path.join(self.local_downloads_files_dir, self.tos_target_dir)):
                self.assertEqual(len(files), self.big_num)
            # dir for small_files
            elif os.path.samefile(root, os.path.join(self.local_downloads_files_dir,
                                                     os.path.join(self.tos_target_dir,
                                                                  self.local_small_files_dir))):
                self.assertEqual(len(files), self.small_num)
            else:
                self.assertEqual(len(files), 0)
            for name in files:
                md5_set.add(self.get_file_md5(os.path.join(root, name)))
        self.assertSetEqual(md5_set,
                            set(self.big_file.values()).union(set(self.small_files.values())))

        # clean tos files
        for f in uploaded_files_in_tos:
            self.files.delete(f)

        list_df = self.files.list()
        files_in_tos_after_delete = list_df.query(f"key.str.startswith('{self.tos_target_dir}')")[
            "key"]
        self.assertEqual(len(files_in_tos_after_delete), 0)

        # clean downloads files
        shutil.rmtree(self.local_downloads_files_dir)
