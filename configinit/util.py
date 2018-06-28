# -*- coding: utf-8 -*-

'''
    File name: settings.py
    Python Version: 3.6
    Configurações padrão
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


def get_rootDir(verbose):
    if verbose:
        print("[I] - Get root path.")
    # Referência do diretorio do projeto
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    return BASE_DIR


def get_dictionary(typemodeling, verbose):
    if verbose:
        print("[I] - Finding model type and return dictionary.")

    DICTIONARY = {
        'knndictionary': {
            'variables': {
                '60min_starnet': None,
                '60min_earth': None,
                'vulnerability': None,
                'radarRR': None,
                '1h_radar': None,
                'hydroRR': None,
                '1h_hydro': None,
                '1h_basin_radar': None,
                '2h_basin_radar': None,
                '1h_basin_radar': None,
                '24h_basin_radar': None,
                '1h_basin_hydro': None,
                '2h_basin_hydro': None,
                '3h_basin_hydro': None,
                '24h_basin_hydro': None,
                'flood': None,
            }
        },
        'standartdictionary': {
            'datetime': None,
            'location': {
                'type': 'Point',
                'coordinates': None,
            },
            'variables': {
                'radar': None,
                'hydro': None,
                'starnet': None,
                'earth': None,
                'vulnerability': None,
                'basin_mean_radar': None,
                'basin_mean_hydro': None,
                '1h_radar': None,
                '1h_hydro': None,
                '1h_basin_radar': None,
                '1h_basin_hydro': None,
                'flood': None,
            },
            'source': 'ci'
        }
    }

    return DICTIONARY[typemodeling]
