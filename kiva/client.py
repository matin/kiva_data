import json
from json import JSONDecodeError
from typing import Union

import requests


BASE_URL = 'https://api.kivaws.org/v1'


class Resource:
    __resource__ = ''

    @classmethod
    def retrieve(cls, id_) -> Union[None, dict]:
        obj_list = cls.retrieve_ids([id_])
        if not obj_list:
            obj = None
        else:
            obj = obj_list[0]
        return obj

    @classmethod
    def retrieve_ids(cls, ids) -> list:
        ids = ','.join(str(id_) for id_ in ids)
        url = f'{BASE_URL}/{cls.__resource__}/{ids}.json'
        resp = requests.get(url)
        try:
            resp_dict = resp.json()
        except JSONDecodeError:
            resp_json = resp.text.replace('\\', '')
            resp_dict = json.loads(resp_json)
        try:
            obj_list = resp_dict[cls.__resource__]
        except KeyError:
            obj_list = []
        return obj_list


class Loan(Resource):
    __resource__ = 'loans'
