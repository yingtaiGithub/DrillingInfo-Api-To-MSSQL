import json

import requests

import config
def authToken(session):
    try:
        response = session.post(
            url="https://di-api.drillinginfo.com/v2/direct-access/tokens",
            headers={
                "X-API-KEY": config.apiKey,
                "Authorization": config.secret,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "client_credentials",
                },
            # verify=False
            )
        print('Response HTTP status Code: {status_code}'.format(
                status_code=response.status_code))
        r=response.content
        t = json.loads(r)
        access_token = t['access_token']

        return access_token

    except requests.exceptions.RequestException:
            print('HTTP Request failed')

# token = authToken()
# print (token)