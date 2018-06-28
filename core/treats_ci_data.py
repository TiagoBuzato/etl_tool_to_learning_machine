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

import os
import pandas as pd
import pymongo
from multiprocessing import Process
from configinit.util import get_rootDir, get_dictionary

class Treats_ci_data():
    def __init__(self, inputdata, multiprocess, verbose=False):

        self.verbose = verbose
        self.pathinputdata = get_rootDir(verbose) + '/inputData/'
        self.MXPROCS = multiprocess
        self.inputdata = inputdata[0]
        self.typedictionary = inputdata[1]

        return

    def processing_data(self, inputdata, MXPROCS, verbose, typedictionary):
        if verbose:
            print("[I] -- Processing Data.")
            print("[I] MX Procs of insert in mongo: %s" % MXPROCS)

        listOcorrences = []
        procIsert = []
        count = 0
        dfdata = self.openfile(inputdata, verbose)
        dfdata = self.transform_knnbase(dfdata, verbose)

        for index, values in dfdata.iterrows():
            listOcorrences.append(self.make_dictionary(values, verbose, typedictionary))
            count = count +1
            if count == 500:
                proccessupsetmongo = Process(target=self.upsert_Mongo_Data, args=(listOcorrences, self.verbose))
                procIsert.append(proccessupsetmongo)
                proccessupsetmongo.start()

                while len(procIsert) >= MXPROCS:
                    for threadinsert in procIsert:
                        if not threadinsert.is_alive():
                            procIsert.remove(threadinsert)
                listOcorrences.clear()
                count = 0

        del dfdata

        return listOcorrences

    def upsert_Mongo_Data(self, data, verbose):
        if verbose:
            print("[I] - Upserting data in mongo.")

        try:
            # mongo_conn = connect_mongo.Database().get_connection()
            mongo_conn = pymongo.MongoClient("localhost", 27017)
            db = mongo_conn['test']
            collection = db['ci']
            collection.insert_many(data)

            if verbose:
                print("[I] - Insertion was successful.")
        except Exception as e:
            print('[E] - Connection with mongodb.')
            print('[E] - Message of error: ', e)
            return False

        return True

    def openfile(self,inputdata, verbose):
        if verbose:
            print("[I] - Opening file " + inputdata)
        dfdata = pd.DataFrame()

        try:
            dfdata = pd.read_csv(inputdata, sep=';', header=0, encoding='ISO-8859-1')
        except Exception as exOF:
            print("[E] - Exception to open inputdata "+inputdata)
            print("[E] - Message of error: ")
            print(exOF)

        return dfdata

    def transform_knnbase(self,dfdata,  verbose):
        if verbose:
            print("[I] - Modeling to KNN base")

        dfdata = dfdata.drop(['time'], axis=1)
        dfdata = dfdata.drop(['lat'], axis=1)
        dfdata = dfdata.drop(['lon'], axis=1)

        return dfdata

    def make_dictionary(self, values, verbose, typemodeling='standartdictionary'):
        if verbose:
            print("[I] - Building dictionary for occurrences.")
        location = values[1], values[2]

        if typemodeling == "knndictionary":
            dataDictionary = get_dictionary(typemodeling, verbose)

            print(values)
            print(dataDictionary['variables'])
            for index, variable in zip(range(0,len(dataDictionary['variables'])), dataDictionary['variables']):
                print(index, variable)
                dataDictionary.update()

        return dataDictionary

    def run(self):

        if not os.path.isdir(self.pathinputdata):
            os.mkdir(self.pathinputdata)

        # Para teste
        ## begin
        self.processing_data(self.inputdata, self.MXPROCS, self.verbose, self.typedictionary)
        ##end

        # dataprocess = Process(target=self.processing_data, args=(self.inputdata, self.MXPROCS, self.verbose))
        # dataprocess.start()

        return