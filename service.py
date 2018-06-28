#!/opt/anaconda3/envs/py360/bin/python
# -*- coding: utf-8 -*-

'''
    File name: service.py
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

import sys
import argparse
import json
from datetime import datetime
from configinit.util import get_rootDir
from core.treats_ci_data import Treats_ci_data
from core.extractor import Extractor


parser = argparse.ArgumentParser(description='''''', formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-v", "--verbose", action='store_true', dest='verbose', help="Verbose", default=False)

parser.add_argument("-td", "--treat-data", nargs='*', type=str, dest='treat_data',
                    help="Operation to treat the CI data to insert in mongodb.", default=None)

parser.add_argument("-ex", "--extractor", nargs='*', type=str, dest='extractor',
                    help="Operation to extract the CI data from database of training. It's need to pass the config "
                         "file.json ", default=None)

parser.add_argument("-mp", "--multiprocess", type=int, dest='multiprocess', help="Define the number of process tha will"
                                                                                 "run in the system.", default=20)

parser.add_argument("-mn", "--member-number", type=int, dest='member_number',
                    help="Definition of member number to training.", default=5)

args = parser.parse_args()

BASE_DIR = get_rootDir(args.verbose)

if __name__=="__main__":
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[I] Inicio - {} ({}).".format(now, sys.argv[0]))

    # To test
    ## begin
    args.treat_data = [BASE_DIR + '/inputData/201805191230.csv', "knndictionary"]
    inputdata = args.treat_data
    args.extractor = BASE_DIR + '/configinit/dbmongo_ci.json'
    configure = args.extractor
    ##end

    if args.treat_data:
        #inputdata = str(args.treat_data[0])
        Treats_ci_data(inputdata, args.multiprocess, args.verbose).run()
        exit()

    elif args.extractor:
        configure = str(args.extractor[0])
        configure = json.loads(open(configure, 'r').read())
        extractdata = Extractor(configure, verbose=args.verbose).run()

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[I] Fim - %s  (%s)." % (now, sys.argv[0]))
    exit(0)