#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate SQL Score Script
# Created on 2017-03-21
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import time,datetime
import traceback

import cx_Oracle
import mysql_handle

import common
###############################################################################
# 计算SQL 模块分值
###############################################################################
# 获取sqlstat中的最大snap_id
def GetLastSQLSnapID(mysql_conn, inst_id):

    last_sql_snap_str = """select ifnull(max(snap_id), -1) from aop_sqlstat where instance_number = %s""" %(inst_id)
    last_sql_snap_id = mysql_handle.GetSingleValue(mysql_conn, last_sql_snap_str)

    return last_sql_snap_id

# 计算SQL long execution 分值
def GenLongExecScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 如果最新的snap_id和sqlstat表中的最大snap_id不匹配，则直接返回
        last_sql_snap_id = GetLastSQLSnapID(mysql_conn, inst_id)
        if snap_id > last_sql_snap_id:
            return -1

        long_exec_str = """select count(distinct sql_id) long_num
                              from aop_sqlstat
                             where EXECUTIONS_DELTA/1000/1000/60 > 3
                               and snap_id = %s
                               and instance_number = %s""" % (snap_id, inst_id)
        common.logger.debug(long_exec_str)
        long_num = mysql_handle.GetSingleValue(mysql_conn, long_exec_str)

        # 计算分值
        metric_name = "sql_long"
        long_exec_score = common.GenMetricScore(mysql_conn, metric_name, long_num)
        if long_num == None or long_exec_score == None:
            common.logger.error("long_num or long_exec_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     long_num, long_exec_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算high cpu SQL分值
def GenSQLCPUScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 如果最新的snap_id和sqlstat表中的最大snap_id不匹配，则直接返回
        last_sql_snap_id = GetLastSQLSnapID(mysql_conn, inst_id)
        if snap_id > last_sql_snap_id:
            return -1

        sql_cpu_str = """select if(b.value=0,0,(100 * a.cpu_time_delta / b.value))  sql_cpu
                          from aop_sqlstat a, aop_total b
                         where a.snap_id = b.snap_id
                           and a.instance_number = b.instance_number
                           and a.ora_snap_id = b.last_snap_id
                           and a.snap_id = %s
                           and a.instance_number = %s
                           and b.stat_name = 'DB CPU'
                         order by cpu_time_delta desc limit 1""" % (snap_id, inst_id)
        common.logger.debug(sql_cpu_str)
        sql_cpu = mysql_handle.GetSingleValue(mysql_conn, sql_cpu_str)

        # 计算分值
        metric_name = "sql_cpu"
        sql_cpu_score = common.GenMetricScore(mysql_conn, metric_name, sql_cpu)
        if sql_cpu == None or sql_cpu_score == None:
            common.logger.error("sql_cpu or sql_cpu_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sql_cpu, sql_cpu_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算high io SQL分值
def GenSQLIOScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 如果最新的snap_id和sqlstat表中的最大snap_id不匹配，则直接返回
        last_sql_snap_id = GetLastSQLSnapID(mysql_conn, inst_id)
        if snap_id > last_sql_snap_id:
            return -1

        sql_io_str = """select if(b.value=0,0,(100 * a.disk_reads_delta / b.value))  sql_io
                          from aop_sqlstat a, aop_total b
                         where a.snap_id = b.snap_id
                           and a.instance_number = b.instance_number
                           and a.ora_snap_id = b.last_snap_id
                           and a.snap_id = %s
                           and a.instance_number = %s
                           and b.stat_name = 'physical reads'
                         order by disk_reads_delta desc limit 1""" % (snap_id, inst_id)
        common.logger.debug(sql_io_str)
        sql_io = mysql_handle.GetSingleValue(mysql_conn, sql_io_str)

        # 计算分值
        metric_name = "sql_io"
        sql_io_score = common.GenMetricScore(mysql_conn, metric_name, sql_io)
        if sql_io == None or sql_io_score == None:
            common.logger.error("sql_io or sql_io_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sql_io, sql_io_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0



