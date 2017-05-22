#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Oracle handle Script
# Created on 2017-01-11
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import cx_Oracle
import time,datetime
import ConfigParser
import os
import sys

###############################################################################
# define functions
###############################################################################
# 连接数据库
def ConnectOracle():
    # 读取配置信息
    config = ConfigParser.ConfigParser()
    curPath = os.getcwd()
    #print curPath
    config.read(curPath + '/param.ini')
    host_id = config.get("oracle_db","host")
    port = config.get("oracle_db","port")
    username = config.get("oracle_db","user")
    password = config.get("oracle_db","passwd")
    dbname = config.get("oracle_db","db")

    # 打开数据库连接
    try:
        conn_str = username + '/' + password + '@' + host_id + ':' + port + '/' + dbname
        #print conn_str
        return cx_Oracle.connect(conn_str)
    except Exception, e:
        print(e)

def ConnectSpecialOracle(conn_str):
    # 打开数据库连接
    try:
        return cx_Oracle.connect(conn_str)
    except Exception, e:
        print(e)

def CloseOracle(con):
    # 关闭数据库连接
    if con:
        con.close()

def GetSingleValue(con,query_str):
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

def GetMultiValue(con, query_str):
    res = None

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

###############################################################################
# main test
###############################################################################
'''
if __name__ == "__main__":
    # 建立mysql连接
    oracle_conn = ConnectOracle()
    query_str = "select sysdate from dual"

    value = GetSingleValue(oracle_conn, query_str)
    CloseOracle(oracle_conn)
    print value
'''
