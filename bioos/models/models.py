from tos.models2 import ListedObject


class DisplayListedObject:

    def __init__(self, o: ListedObject, s3_url: str, https_url: str):
        self.key = o.key
        self.last_modified = o.last_modified
        self.size = o.size
        self.owner = o.owner.display_name
        # self.hash_crc64_ecma = o.hash_crc64_ecma
        self.s3_url = s3_url
        self.https_url = https_url
