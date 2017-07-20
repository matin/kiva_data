from typing import Union

import requests


BASE_URL = 'https://api.kivaws.org/v1'


class Resource:
    __resource_name__ = ''

    @classmethod
    def retrieve(cls, id_: Union[int, str, list]) -> list:
        if isinstance(id_, list):
            id_ = ','.join(id_)
        url = f'{BASE_URL}/{cls.__resource_name__}/{id_}.json'
        resp = requests.get(url)
        return resp.json()[cls.__resource_name__]


class Loan(Resource):
    __resource_name__ = 'loans'
