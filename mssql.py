import sys
import json

import pyodbc

import config

class Client():
    def __init__(self, server, db, user, password):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=%s;"
                              "Database=%s;"
                              "username = %s;"
                              "password = %s;"
                              "Trusted_Connection=yes;" %(server, db, user, password))

        self.cursor = self.cnxn.cursor()

    def column_names(self, table):
        column_names = []
        for row in self.cursor.columns(table=table):
            column_names.append(row.column_name)

        return column_names

    def add_row(self, table, values):
        insert_query = "INSERT INTO %s(%s) VALUES (%s)"
        columns = self.column_names(table)
        query = insert_query %(config.table_name, ','.join(columns), str(values).replace("[", '').replace("]", '').replace('None', 'Null').replace("False", '0').replace("True", '1').replace('"', "'"))

        try:
            # Execute the SQL command
            self.cursor.execute(query)
            # Commit your changes in the database
            self.cnxn.commit()

        except Exception as e:
            # print ("ERROR:", e)
            # print (query)
            # print (values)
            # print (json.dumps(zip(columns, values), indent=2))
            # Rollback in case there is any error
            self.cnxn.rollback()
            # sys.exit()

    def end(self):
        self.cursor.close()
        self.cnxn.close()


def main():
    client = Client(config.server, config.db_name, config.username, config.password)
    values = [130036944, 130036944, 'GAS', 'WY', '49037296210000', 'MESAVERDE', 'SWEETWATER (WY)', 'WEXPRO DEVELOPMENT COMPANY', 'GREEN RIVER', 'D', None, '2017-12-02T00:00:00Z', '2017-09-13T00:00:00Z', '2018-03-08T18:48:23.917Z', '2018-03-08T18:48:23.917Z', None, '(N/A)', 'COM', 'USA', 'WHISKEY CANYON UNIT', '(N/A)', 'WC', '0', '0', '49037', '8', '2523', 'ACTIVE', '', '2419 FNL 2399 FWL', '13N 101W', '24', 'SE NW', '', '(N/A)', 'WEXPRO DEVELOPMENT COMPANY', '', 'P-530', '', '', '', '', '', '', '', '0', 'WEXPRO DEVELOPMENT COMPANY', 'MESAVERDE', '13N', '101W', 'X', '0101000020E61000009C08D110C42E5BC0F90A77D3D28B4440', 7139, 100374399, 7426, None, None, 57, None, 41.0923714, -108.7307169, None, None, None, None, None, None, 41.0923714, -108.7307169, 130036944, None, False]

    client.add_row(config.table_name, values)

    client.end()


if __name__ == "__main__":
    main()