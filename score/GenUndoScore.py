#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Undo Score Script
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
# 计算Undo 模块分值
###############################################################################
# 计算expired undo blocks stolen from other undo segments分值
def GenUndoExpiredScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        undo_expired_str = """select if(undoblks = 0, 0, (expblkrelcnt * 1000/undoblks))
                              from aop_undostat
                             where snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(undo_expired_str)
        undo_expired = mysql_handle.GetSingleValue(mysql_conn, undo_expired_str)

        # 计算分值
        metric_name = "undo_expired"
        expired_score = common.GenMetricScore(mysql_conn, metric_name, undo_expired)
        if undo_expired == None or expired_score == None:
            common.logger.error("undo_expired or expired_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     undo_expired, expired_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算undo 等待次数分值
def GenUedoWaitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        undo_wait_str = """select if(total_waits = 0, 0, (time_waited_micro/total_waits/1000)) undo_wait
                              from aop_wait_event
                             where event = 'latch: undo global data'
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(undo_wait_str)
        undo_wait = mysql_handle.GetSingleValue(mysql_conn, undo_wait_str)

        # 计算分值
        metric_name = "undo_wait"
        undo_wait_score = -1
        if undo_wait == None:
            undo_wait = 0
            undo_wait_score = 100
        else:
            undo_wait_score = common.GenMetricScore(mysql_conn, metric_name, undo_wait)

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     undo_wait, undo_wait_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0



# 计算rollback分值
def GenRollbackScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        rollback_str = """select if(diftime <= 0, 0, (value*10/diftime))
                             from (select UNIX_TIMESTAMP(time)-UNIX_TIMESTAMP(start_time) diftime, diff_value as value
                                      from aop_perf_stat
                                     where name = 'user rollbacks'
                                       and snap_id = %s
                                       and inst_id = %s) a""" % (snap_id, inst_id)
        common.logger.debug(rollback_str)
        rollback = mysql_handle.GetSingleValue(mysql_conn, rollback_str)

        # 计算分值
        metric_name = "undo_rollback"
        common.logger.debug("rollback: " + str(rollback))
        rollback_score = common.GenMetricScore(mysql_conn, metric_name, rollback)
        if rollback == None or rollback_score == None:
            common.logger.error("rollback or rollback_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     rollback, rollback_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

