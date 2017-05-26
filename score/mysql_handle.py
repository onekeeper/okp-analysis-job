#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Mysql handle Script
# Created on 2017-01-10
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################

###############################################################################
# define functions
###############################################################################
import pymysql
import time,datetime
import ConfigParser
import os
import sys

# 连接数据库
def ConnectMysql(dbname = None):
        # 读取配置信息
        config = ConfigParser.ConfigParser()
        curPath = os.path.dirname(__file__)
        # print curPath
        config.read(os.path.join(curPath, 'param.ini'))
        host_id = config.get("mysql_db", "host")
        port = config.getint("mysql_db", "port")
        username = config.get("mysql_db", "user")
        password = config.get("mysql_db", "passwd")

        if dbname == None:
            dbname = config.get("mysql_db", "db")

        # 打开数据库连接
        try:
            return pymysql.connect(host=host_id, user=username, passwd=password, port=port, db=dbname, charset='utf8')
        except Exception, e:
            print(e)

def CloseMysql(con):
    # 关闭数据库连接
    if con:
        con.close()

# 取单个返回值
def GetSingleValue(con, query_str):
    res = None
    try:
        # 使用cursor()方法获取操作游标
        cur = con.cursor()

        # 使用execute方法执行SQL语句
        cur.execute(query_str)

        # 使用fetchall获取数据集
        rows = cur.fetchall()
        for row in rows:
            res = row[0]

        return res
    except Exception, e:
        print(e)

# 获取mysql库查询结果集
def GetMultiValue(con, query_str):
    rows = None
    try:
        # 使用cursor()方法获取操作游标
        cur = con.cursor()

        # 使用execute方法执行SQL语句
        cur.execute(query_str)

        # 使用fetchall获取数据集
        rows = cur.fetchall()

        return rows
    except Exception, e:
        print(e)

def ExecuteSQL(con, query_str):
    res = None
    try:
        # 使用cursor()方法获取操作游标
        cur = con.cursor()

        # 使用execute方法执行SQL语句
        cur.execute(query_str)

        # 提交
        con.commit()

        return 1
    except Exception, e:
        print(e)
        con.rollback()

###############################################################################
# main
###############################################################################
'''
if __name__ == "__main__":
    # 建立mysql连接
    mysql_conn = ConnectMysql()
    query_str = "select value from aop_perf_stat where inst_id = 1 and name = 'parse count (total)' order by time desc limit 1;"

    value = GetSingleValue(mysql_conn, query_str)
    CloseMysql(mysql_conn)
    print value
'''
