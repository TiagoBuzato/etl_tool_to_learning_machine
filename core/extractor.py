#!/opt/anaconda3/envs/py360/bin/python
# -*- coding: utf-8 -*-

'''
    File name: extractor.py
    Python Version: 3.6.0

    ##########   DATAPROCESSOR   ##########
    Ca

'''

__author__ = "Tiago S. Buzato"
__version__ = "0.1"
__email__ = "tiago.buzato@climatempo.com.br"
__status__ = "Development"

# Tag das Mensagens:
# [I] -> Informacao
# [A] -> Aviso/Alerta
# [E] -> Erro

import pandas as pd
from pymongo import MongoClient, errors

class Extractor():
    def __init__(self, configure, verbose=False):
        self.verbose = verbose
        self.typeDB = configure['configBD']['typeDB']
        self.host = configure['configBD']['host']
        self.port = configure['configBD']['port']
        self.dbname= configure['configBD']['dbs']
        self.collection = configure['configBD']['collection']
        self.fields = configure['fields']

    def connect_mongod(self, dbname, collection, host, port, verbose):
        if verbose:
            print("[I] - Conneting in data base ", dbname, " in mongod.")
        try:
            connMongo = MongoClient(host=host, port=port)
            if verbose:
                print("[I] - Data base was connect with successful.")
            database = connMongo[dbname]
            collection = database[collection]

        except errors.ConnectionFailure as ecf:
            print("[E] - Error to connect to mongod: ", ecf)
            return False
        except errors.CollectionInvalid as eci:
            print("[E] - Error to validadte of colllection: ", eci)
            return False
        return collection

    def create_dataframe(self, fields, verbose):
        if verbose:
            print("[I] - Creating dataframe.")
        dfbasedata = pd.DataFrame()

        for i in range(0, len(fields)):
            dfbasedata[fields[i]] = 0

        return dfbasedata

    def extract(self, conn, verbose):
        if verbose:
            print("[I] - Getting data from database to dataframe.")
        listdata = []
        cursor = conn.find({})
        del conn

        for item in list(cursor):
            item['lat'], item['lon'] = item['location']['coordinates']
            for key, value in item['variables'].items():
                item[key] = value
            del item['variables']
            del item['location']
            listdata.append(item)

        dforigin = pd.DataFrame(listdata)

        del listdata

        return dforigin


    def transforme(self, dforigin, fields, verbose):
        if verbose:
            print("[I] - Getting data frame of base data.")
        dfbasedata = pd.DataFrame()
        for field in fields:
            dfbasedata[field] = dforigin[field]

        return dfbasedata


    def run(self):
        '''

        :return: list
        '''

        connmongo = self.connect_mongod(self.dbname, self.collection, self.host, self.port, self.verbose)
        dforigin = self.extract(connmongo, self.verbose)
        dfbasedata = self.transforme(dforigin, self.fields, self.verbose)

        del dforigin

        return dfbasedata