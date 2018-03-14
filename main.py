import re
import json
import requests

import get_token
import config
import mssql

def get_entities(entity_url, session, token):
    headers={
        "X-API-KEY": config.apiKey,
        "Accept": "application/json",
        "Authorization": "bearer " + token,
    }

    r = session.get(entity_url,
                    headers=headers,
                    # verify=False
                    )

    entities = json.loads(r.content)
    if len(entities) > 0:
        for entity in entities:
            values = []
            for column in columns:
                try:
                    values.append(entity[column].encode('utf-8').replace('"', "'").replace("'", "''"))
                except:
                    values.append(entity[column])

            ms_client.add_row(config.table_name, values)
    else:
        return None

    try:
        next_link = "https://di-api.drillinginfo.com/v2/direct-access" + re.search("<(.+?)>", r.headers.get("Links").split(",")[-1]).group(1)
    except:
        return None

    return next_link
    # return None
if __name__ == "__main__":
    ms_client = mssql.Client(config.server, config.db_name, config.username, config.password)
    columns = ms_client.column_names(config.table_name)

    pagesize = 10000
    entity_url = "https://di-api.drillinginfo.com/v2/direct-access/producing-entities?pagesize=%d"%pagesize
    s = requests.session()
    token = get_token.authToken(s)

    while True:
        print (entity_url)
        next_link = get_entities(entity_url, s, token)
        if next_link:
            entity_url  = next_link
        else:
            break

    ms_client.end()
