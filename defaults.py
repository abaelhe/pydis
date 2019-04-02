#!/usr/bin/env python
#coding:utf-8

"""
@Author: Abael He<abaelhe@icloud.com>
@file: defaults.py
@time: 12/11/2018 22:31
@license: All Rights Reserved, Abael.com
"""

import sys

DATA_UNIT = 1024 * 1024
HOST = '127.0.0.1'
PORT = 6379
#ENCODING = sys.getdefaultencoding()
CONNECT_TIMEOUT = 20000
CHUNK_SIZE = 16 # unit: 1MB = 1024*1024
MAX_BUFFER_SIZE = 256 # unit 1MB = 1024 * 1024
READ_TIMEOUT = 0



