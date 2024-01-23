import threading
import time
import unittest

from bioos.utils.common_tools import SingletonType


class Foo(metaclass=SingletonType):
    def __init__(self, name):
        time.sleep(1)
        self.name = name


class MyThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.func = func
        self.name = name
        self.args = args
        self.res = None

    def run(self):
        self.res = self.func(*self.args)

    def get_res(self):
        return self.res


class TestUtils(unittest.TestCase):
    @classmethod
    def init_foo(cls, arg):
        obj = Foo(str(arg))
        return obj

    def test_singletonType_diff_obj(self):

        threads = []
        for i in range(10):
            threads.append(MyThread(func=self.init_foo, args=(i,), name=f'diff_obj {i}'))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        res = set()
        for t in threads:
            res.add(t.get_res())
        self.assertEqual(len(res), 10)

    def test_singletonType_same_obj(self):

        threads = []
        for i in range(100):
            threads.append(MyThread(func=self.init_foo, args=(0,), name=f'same_obj {i}'))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        res = set()
        for t in threads:
            res.add(t.get_res())

        self.assertEqual(len(res), 1)
