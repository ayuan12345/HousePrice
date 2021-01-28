#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2021/1/28 16:43
@Author  : name
@File    : SqlUtils.py
"""
import sqlite3
import threading


class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """

    def __init__(self, path, command='', *args, **kwargs):
        self.lock = threading.RLock()  # 锁
        self.path = path  # 数据库连接参数
        if command != '':
            conn = self.get_conn()
            cu = conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)  # ,check_same_thread=False)
        conn.text_factory = str
        return conn

    def conn_close(self, conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self, *args, **kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self, *args, **kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs

        return connection

    @conn_trans
    def execute(self, command, method_flag=0, conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0], command[1])
            conn.commit()
        except sqlite3.IntegrityError as e:
            # print e
            return -1
        except Exception as e:
            print(e)
            return -2
        return 0

    @conn_trans
    def fetchall(self, command="select name from xiaoqu", conn=None):
        cu = conn.cursor()
        lists = []
        try:
            cu.execute(command)
            lists = cu.fetchall()
        except Exception as e:
            print(e)
            pass
        return lists


def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list = [u'url', u'小区名称', u'大区域', u'小区域', u'建造时间']
    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command = (r"insert into xiaoqu values(?,?,?,?,?)", t)  # ?,
    return command


def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list = [u'链接', u'小区名称', u'户型', u'面积', u'朝向', u'装修', u'电梯', u'签约时间', u'签约总价', u'楼层', u'年代楼型', u'来源', u'签约单价',
                 u'税费', u'地铁', u'挂牌价', u'成交周期']
    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command = (r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", t)
    return command


def gen_zaishou_insert_command(info_dict):
    """
    生成在售记录数据库插入命令
    """
    info_list = [u'链接', u'房子描述', u'小区名称', u'户型', u'面积', u'朝向', u'装修', u'电梯', u'楼层年代楼型', u'区域', u'关注', u'带看', u'发布时间',
                 u'税费', u'地铁', u'限制', u'挂牌价', u'挂牌单价']
    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command = (r"insert into zaishou values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", t)
    return command


def exception_write(fun_name, url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt', 'a')
    line = "%s %s\n" % (fun_name, url)
    f.write(line)
    f.close()
    lock.release()