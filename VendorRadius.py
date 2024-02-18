# get API key

import json
import requests
import sys


def convert(list):
    return (*list,)


def get_keycloak_token():
    url = 'https://keycloak.home365.co/auth/realms/ProdRealm/protocol/openid-connect/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
    params = {
        'client_id': 'prod-client',
        'username': 'user-prod',
        'password': 'qowe34YY88',
        'grant_type': 'password'
    }
    r = requests.post(url, headers=headers, data=params).json()
    return r['access_token']


def return_related_vendors(category_id, incident_id, user_id='f9966362-6edb-4c87-805a-8ed66f70d54a'):
    print('[INFO]: Getting possible service providers using vendors-by-vategory API')
    url = 'https://balanceservice-prod.home365.co/finance/vendors/vendors-by-category?categoryId=%s&incidentId=%s&userId=%s' % (
    category_id, incident_id, user_id)
    token = get_keycloak_token()
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
               'Authorization': 'Bearer ' + token}
    r = requests.get(url, headers=headers).json()
    print(r)
    vendors_list = []
    try:
        if len(r) > 0:
            print('[INFO]: Listing %i vendors based on API' % len(r))
            for v in r:
                vendors_list.append(v['accountId'])
        else:
            print('[INFO]: No vendor has been found based on API')
    except Exception as e:
        print('[WARNING]: There has been an error: %s' % sys.exc_info()[1])
        return None

    return convert(vendors_list)

# https://balanceservice-prod.home365.co/finance/vendors/vendors-by-category?categoryId=%s&incidentId=%s&userId=%s
# https://balanceservice-prod.home365.co/finance/vendors/vendors-by-category?categoryId=a19ada2f-081c-e111-b6bd-001517d1792a&incidentId=c1755d18-380b-4f88-ba28-9fafaf02e8a1&userId=f9966362-6edb-4c87-805a-8ed66f70d54a