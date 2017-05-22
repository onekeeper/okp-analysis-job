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
import traceback

import cx_Oracle
import mysql_handle

import common
###############################################################################
# 计算Redo 模块分值
###############################################################################
# 计算redo 等待次数分值
def GenRedoWaitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        redo_wait_str = """select if(b.value = 0, 0, (a.t_ws / b.value))  redo_wait
                                      from (select inst_id, snap_id, sum(total_waits) t_ws
                                              from aop_wait_event
                                             where event in ('log buffer space', 'latch: redo writing', 'latch: redo allocation', 'LGWR wait for redo copy')
                                               and snap_id = %s
                                               and inst_id = %s
                                          group by inst_id, snap_id) a,
                                           (select inst_id, snap_id, diff_value as value
                                              from aop_perf_stat
                                             where name = 'redo entries'
                                               and snap_id = %s
                                               and inst_id = %s) b
                                     where a.snap_id = b.snap_id
                                       and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(redo_wait_str)
        redo_wait = mysql_handle.GetSingleValue(mysql_conn, redo_wait_str)

        # 计算分值
        metric_name = "redo_wait"
        redo_wait_score = common.GenMetricScore(mysql_conn, metric_name, redo_wait)
        if redo_wait == None or redo_wait_score == None:
            common.logger.error("redo_wait or redo_wait_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     redo_wait, redo_wait_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算log file sync分值
def GenLgSyncScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        lgsync_str = """select if(total_waits = 0, 0, (time_waited_micro/total_waits/1000))
                              from aop_wait_event
                             where event = 'log file sync'
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(lgsync_str)
        lgsync = mysql_handle.GetSingleValue(mysql_conn, lgsync_str)

        # 计算分值
        metric_name = "redo_lgsync"
        lgsync_score = common.GenMetricScore(mysql_conn, metric_name, lgsync)
        if lgsync == None or lgsync_score == None:
            common.logger.error("lgsync or lgsync_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     lgsync, lgsync_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算log file parallel write分值
def GenLGWRScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        lgwr_str = """select if(total_waits = 0, 0, (time_waited_micro/total_waits/1000))
                              from aop_wait_event
                             where event = 'log file parallel write'
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(lgwr_str)
        lgwr = mysql_handle.GetSingleValue(mysql_conn, lgwr_str)

        # 计算分值
        metric_name = "redo_lgwr"
        lgwr_score = common.GenMetricScore(mysql_conn, metric_name, lgwr)
        if lgwr == None or lgwr_score == None:
            common.logger.error("lgwr or lgwr_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     lgwr, lgwr_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

