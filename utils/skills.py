# !/usr/bin/env python 
# -*- coding: utf-8 -*-
# @version: v3.7
# @author: Small_stars
# @mailbox: smallstars.he@qq.com
# @site: 
# @software: PyCharm
# @file: skills.py
# @time: 2021/2/9 15:47
def get_current_path(name):
    from os import path
    return path_format(path.dirname(name))


def path_format(path):
    return path.replace('\\', '/')
