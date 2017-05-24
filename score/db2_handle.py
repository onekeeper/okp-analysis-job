#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Oracle handle Script
# Created on 2016-01-11
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import ibm_db
import time,datetime
import ConfigParser
import os
import sys

###############################################################################
# define functions
###############################################################################
# 连接数据库
def ConnectDB2():
    # 读取配置信息
    config = ConfigParser.ConfigParser()
    curPath = os.path.dirname(__file__)
    #print curPath
    config.read(os.path.join(curPath, 'param.ini'))
    host_id = config.get("tivoli_db","host")
    port = config.get("tivoli_db","port")
    username = config.get("tivoli_db","user")
    password = config.get("tivoli_db","passwd")
    dbname = config.get("tivoli_db","db")

    # 打开数据库连接
    try:
        conn_str = "DATABASE=" + dbname + ";HOSTNAME=" + host_id + ";PORT=" + port + ";PROTOCOL=TCPIP;UID=" + username + ";PWD=" + password
        # DATABASE=ITMUSER;HOSTNAME=192.168.129.82;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=db2inst1;", "", ""
        print conn_str
        return ibm_db.connect(conn_str, "", "")
    except Exception, e:
        print(e)


def ExecQuery(con, query_str):
    stmt = None

    try:
        # 使用execute方法执行SQL语句
        stmt = ibm_db.exec_immediate(con, query_str)
        return stmt
    except Exception, e:
        print(e)


def GetMultiValue(stmt):
    try:
        return ibm_db.fetch_both(stmt)
    except Exception, e:
        print(e)