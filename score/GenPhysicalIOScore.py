#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate physical IO Score Script
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
# 计算物理IO模块分值
###############################################################################
# 计算物理读响应时间分值
def GenPIOReadTimeScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        #计算db file sequential read 和 db file scattered read的时间，转换为毫秒（ms）
        pio_rtime_str = """select if(sum(total_waits)=0, 0, (sum(time_waited_micro) / sum(total_waits) /1000)) pio_rtime
                              from aop_wait_event
                             where event in ('db file sequential read','db file scattered read')
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(pio_rtime_str)
        pio_rtime = mysql_handle.GetSingleValue(mysql_conn, pio_rtime_str)

        # 计算分值
        metric_name = "pio_rtime"
        pio_rtime_score = common.GenMetricScore(mysql_conn, metric_name, pio_rtime)
        if pio_rtime == None or pio_rtime_score == None:
            common.logger.error("pio_rtime or pio_rtime_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     pio_rtime, pio_rtime_score)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算物理写响应时间分值
def GenPIOWriteTimeScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 计算物理写的平均时间，转换为毫秒（ms）
        pio_wtime_str = """select if(total_waits=0, 0, (time_waited_micro / total_waits /1000)) pio_wtime
                              from aop_wait_event
                             where event in ('db file parallel write')
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(pio_wtime_str)
        pio_wtime = mysql_handle.GetSingleValue(mysql_conn, pio_wtime_str)

        # 计算分值
        metric_name = "pio_wtime"
        pio_wtime_score = common.GenMetricScore(mysql_conn, metric_name, pio_wtime)
        if pio_wtime == None or pio_wtime_score == None:
            pio_wtime = 0
            pio_wtime_score = 100
        #    common.logger.error("pio_wtime or pio_wtime_score is None!!!")
        #     return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     pio_wtime, pio_wtime_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算直接路径读响应时间分值
def GenPIODirectReadTimeScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 计算直接路径读响应时间，转换为毫秒（ms）
        pio_d_rtime_str = """select if(sum(total_waits)=0, 0, (sum(time_waited_micro) / sum(total_waits) /1000)) pio_d_rtime
                              from aop_wait_event
                             where event in ('direct path read','direct path read temp')
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(pio_d_rtime_str)
        pio_d_rtime = mysql_handle.GetSingleValue(mysql_conn, pio_d_rtime_str)

        # 计算分值
        metric_name = "pio_d_rtime"
        pio_d_rtime_score = common.GenMetricScore(mysql_conn, metric_name, pio_d_rtime)
        if pio_d_rtime == None or pio_d_rtime_score == None:
            common.logger.error("pio_d_rtime or pio_d_rtime_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     pio_d_rtime, pio_d_rtime_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

# 计算直接路径写响应时间分值
def GenPIODirectWriteTimeScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 计算直接路径写响应时间，转换为毫秒（ms）
        pio_d_wtime_str = """select if(total_waits=0, 0, (time_waited_micro / total_waits /1000)) pio_d_wtime
                              from aop_wait_event
                             where event in ('direct path write','direct path write temp')
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(pio_d_wtime_str)
        pio_d_wtime = mysql_handle.GetSingleValue(mysql_conn, pio_d_wtime_str)

        # 计算分值
        metric_name = "pio_d_wtime"
        pio_d_wtime_score = common.GenMetricScore(mysql_conn, metric_name, pio_d_wtime)
        if pio_d_wtime == None or pio_d_wtime_score == None:
            common.logger.error("pio_d_wtime or pio_d_wtime_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     pio_d_wtime, pio_d_wtime_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算每次物理写所需要的请求次数分值
def GenPIOReqPerWriteScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        pio_req_write_str = """select if(a.value=0, 0, (b.value * 10 / a.value))  pio_req_write
                                  from (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name in ('physical write IO requests')
                                           and snap_id = %s
                                           and inst_id = %s) a,
                                       (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name = 'physical writes from cache'
                                           and snap_id = %s
                                           and inst_id = %s) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(pio_req_write_str)
        pio_req_write = mysql_handle.GetSingleValue(mysql_conn, pio_req_write_str)

        # 计算分值
        metric_name = "pio_req_write"
        pio_req_write_score = common.GenMetricScore(mysql_conn, metric_name, pio_req_write, 'N')
        if pio_req_write == None or pio_req_write_score == None:
            common.logger.error("pio_req_write or pio_req_write_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     pio_req_write, pio_req_write_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
