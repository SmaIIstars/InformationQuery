# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @version: v3.7
# @author: Small_stars
# @mailbox: smallstars.he@qq.com
# @site: 
# @software: PyCharm
# @file: __init__.py
# @time: 2021/2/9 20:47
HOST = 'localhost'
USER = 'root'
PASSWORD = '123456'
DATABASE = 'qq'
PORT = 3306


def sql_init():
    import pymysql
    db = pymysql.connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE, port=PORT)
    return db








