from typing import List, Dict, Any
import requests
import os

AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'

def get_sales(date: str) -> List[Dict[str, Any]]:
  for page in range(1, 4):
    api_url2 = API_URL + 'sales?date=' + date + '&page=' + str(page)
    r = requests.get(api_url2,headers={'Authorization': AUTH_TOKEN})
    return r.json()


"""
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    # TODO: implement me

    # dummy return:
    # return [
    #     {
    #         "client": "Tara King",
    #         "purchase_date": "2022-08-09",
    #         "product": "Phone",
    #         "price": 1062
    #     },
    #     {
    #         "client": "Lauren Hawkins",
    #         "purchase_date": "2022-08-09",
    #         "product": "TV",
    #         "price": 1373
    #     },
    #     # ...
    # ]

