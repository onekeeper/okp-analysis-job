#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Redo Score Script
# Created on 2017-03-02
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import time,datetime

import cx_Oracle
import mysql_handle
import traceback
import logging
import common
###############################################################################
# 计算tablespace 模块分值
###############################################################################
def GentablespaceScore(mysql_conn,inst_id,snap_id):
    try:
    # 建立mysql连接
        mysql_conn = mysql_handle.ConnectMysql()
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn,snap_id)

        table_space_str = """select min((free_sz/total_sz)*100) 
                         from aop_tablespace 
						 where snap_id=%d and type!='UNDO'""" % snap_id
        common.logger.debug(table_space_str)
        table_space = mysql_handle.GetSingleValue(mysql_conn, table_space_str)

    # 计算分值
        metric_name = "table_space"
    # 存储计算结果
        sql_str = "select count(1) from table_space_score where snap_id = %s" %snap_id
        has_record = mysql_handle.GetSingleValue(mysql_conn, sql_str)
        if has_record == 0:
            exec_str = "insert into table_space_score(inst_id,snap_id, start_time, end_time,  value, score) \
                    values(%s,%s, '%s', '%s', %s, %s)" %(1,snap_id, snap_start_time, snap_end_time, table_space,table_space)
        else:
            exec_str = "update table_space_score set value = %s, score = %s where snap_id = %s" %(table_space, table_space, snap_id)
        res = mysql_handle.ExecuteSQL(mysql_conn, exec_str)
        return 1
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
	
#mysql_conn = mysql_handle.ConnectMysql()	
#GentablespaceScore(mysql_conn,1,1370)
