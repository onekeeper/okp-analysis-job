#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Parse Score Script
# Created on 2017-02-07
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
# 计算解析模块分值
###############################################################################

def GenParseCPUScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)
        #print snap_id, snap_start_time, snap_end_time

        total_count = 0
        parse_count_str = "select start_time, time end_time, diff_value from aop_perf_stat where inst_id = %s and name = 'parse count (total)' and snap_id = %s limit 1;" %(inst_id, snap_id)
        res = mysql_handle.GetMultiValue(mysql_conn,parse_count_str)
        for row in res:
            start_time = row[0]
            end_time = row[1]
            total_count = row[2]


        # 计算分值
        metric_name = "parse_cpu"
        count_per_second = total_count / (end_time - start_time).seconds
        parse_cpu_score = common.GenMetricScore(mysql_conn, metric_name, count_per_second)
        if count_per_second == None or parse_cpu_score == None:
            common.logger.error("count_per_second or parse_cpu_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time, count_per_second,parse_cpu_score)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

def GenParseWaitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        parse_wait_str = '''select sum(total_waits)
                              from aop_wait_event
                             where inst_id = %s
                               and snap_id = %s
                               and event in ('library cache lock',
                                             'library cache pin',
                                             'row cache lock',
                                             'latch: shared pool',
                                             'latch: row cache objects',
                                             'library cache: mutex S',
                                             'library cache: mutex X',
                                             'cursor: mutex S',
                                             'cursor: mutex X',
                                             'cursor: pin S',
                                             'cursor: pin S wait on X',
                                             'cursor: pin X');
                            ''' %(inst_id, snap_id)
        parse_wait = mysql_handle.GetSingleValue(mysql_conn,parse_wait_str)

        # 计算解析次数
        query_str = """select diff_value
                          from aop_perf_stat
                         where inst_id = %s
                           and snap_id = %s
                           and name = 'parse count (total)'"""%(inst_id, snap_id)
        parse_count = mysql_handle.GetSingleValue(mysql_conn, query_str)

        # 计算分值
        wait_per_parse = parse_wait / parse_count
        metric_name = "parse_wait"
        parse_wait_score = common.GenMetricScore(mysql_conn, metric_name, wait_per_parse)
        if wait_per_parse == None or parse_wait_score == None:
            common.logger.error("wait_per_parse or parse_wait_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time, wait_per_parse, parse_wait_score)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

def GenHardParseScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        total_count = 0
        start_time = None
        end_time = None
        parse_hard_str = "select start_time, time end_time, diff_value from aop_perf_stat where inst_id = %s and name = 'parse count (hard)' and snap_id = %s limit 1;" %(inst_id, snap_id)
        res = mysql_handle.GetMultiValue(mysql_conn,parse_hard_str)
        for row in res:
            start_time = row[0]
            end_time = row[1]
            total_count = row[2]

        common.logger.debug("start_time: " + str(start_time) + "; " + "end_time: " + str(end_time) + "; " + "total_count: " + str(total_count))
        hard_per_second = total_count / (end_time - start_time).seconds
        # 计算分值
        metric_name = "parse_hard"
        parse_hard_score = common.GenMetricScore(mysql_conn, metric_name, hard_per_second)
        if hard_per_second == None or parse_hard_score == None:
            common.logger.error("hard_per_second or parse_hard_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time, hard_per_second, parse_hard_score)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0



