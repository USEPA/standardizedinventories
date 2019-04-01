# -*- coding: utf-8 -*-
# Importing libraries

import yaml
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

__config = None

def config():

    global __config

    if not __config:

        with open(dir_path + '/config.yaml', mode = 'r') as f:

            __config = yaml.load(f)

    return __config
