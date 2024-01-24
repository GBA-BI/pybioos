from typing import Iterable, Union

from bioos.utils.common_tools import SingletonType


class UtilityResource(metaclass=SingletonType):
    """
    **[Deprecated] This resource will be implemented in the future**
    Common tool collection
    """

    @staticmethod
    def upload_wdl_image(source_image: str, target_image: str) -> bool:
        """Uploads an executable WDL image .

        *Example*:
        ::

            bioos.utility.upload_wdl_image(source_image="aaa/bbb:1.0.0", target_image:"AAA/BBB:miracle-1.0.0")

        :param source_image: A local executable WDL image name
        :type source_image: str
        :param target_image: Target image name
        :type target_image: str
        :return: Result of uploading
        :rtype: bool
        """
        pass

    @staticmethod
    def json_transfer(
            json_: Union[str, Iterable[str]]) -> Union[str, Iterable[str]]:
        """Convert the local JSON to meet the requirements of MiracleCloud

        *Example*:
        ::

            bioos.utility.json_transfer(json='{\"aaa\":\"bbb\"}')

        :param json_: JSON or JSON list to be converted
        :type json_: Union[str, Iterable[str]]
        :return: JSON or JSON list after converting
        :rtype: Union[str, Iterable[str]]
        """
        pass
