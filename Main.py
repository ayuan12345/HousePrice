#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2021/1/28 16:51
@Author  : name
@File    : Main.py
"""
from SqlUtils import SQLiteWraper
import HtmlParser

if __name__ == "__main__":

    command = "create table if not exists xiaoqu (url TEXT primary key UNIQUE,name TEXT, regionb TEXT, regions TEXT, " \
              "year TEXT)"  #
    db_xq = SQLiteWraper('lianjia-xq.db', command)
    command = "create table if not exists zaishou (href TEXT primary key UNIQUE, detail TEXT,xiaoqu TEXT, style TEXT, " \
              "area TEXT, orientation TEXT, zhuangxiu TEXT, dianti TEXT, loucenniandai TEXT, quyu TEXT, guanzhu TEXT, " \
              "daikan TEXT, fabushijian TEXT,shuifei TEXT,subway TEXT,xianzhi TEXT, total_price TEXT, unit_price TEXT) "
    db_zs = SQLiteWraper('lianjia-zs.db', command)
    command = "create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, " \
              "orientation TEXT, zhuangxiu TEXT, dianti TEXT, sign_time TEXT, total_price TEXT, loucen TEXT, " \
              "year TEXT,source TEXT, unit_price TEXT,shuifei TEXT, subway TEXT, guapaiprice TEXT, cycletime TEXT) "
    db_cj = SQLiteWraper('lianjia-cj.db', command)

    HtmlParser.get_qu_url_spider()