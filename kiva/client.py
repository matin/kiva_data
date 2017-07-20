from typing import List, Union

import requests


BASE_URL = 'https://api.kivaws.org/v1'


class Resource:
    __resource__ = ''

    @classmethod
    def retrieve(cls, id_: Union[int, str]) -> Union[None, dict]:
        url = f'{BASE_URL}/{cls.__resource__}/{id_}.json'
        resp = requests.get(url)
        obj_list = resp.json()[cls.__resource__]
        if not obj_list:
            obj = None
        else:
            obj = obj_list[0]
        return obj

    @classmethod
    def retrieve_ids(cls, ids: Union[List[int], List[str]]) -> list:
        ids = ','.join(str(id_) for id_ in ids)
        url = f'{BASE_URL}/{cls.__resource__}/{ids}.json'
        resp = requests.get(url)
        return resp.json()[cls.__resource__]


class Loan(Resource):
    __resource__ = 'loans'
