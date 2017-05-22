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
# 计算执行模块分值
###############################################################################
# 计算执行时间分值
def GenExecTimeScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        total_count = 0
        start_time = None
        end_time = None
        exec_time_str = "select start_time, time end_time, value from aop_time_model where inst_id = %s and stat_name = 'sql execute elapsed time' and snap_id = %s limit 1;" %(inst_id, snap_id)
        res = mysql_handle.GetMultiValue(mysql_conn,exec_time_str)
        for row in res:
            start_time = row[0]
            end_time = row[1]
            total_time = row[2]

        common.logger.debug("start_time: " + str(start_time) + "; " + "end_time: " + str(end_time) + "; " + "total_time: " + str(total_time))
        exec_per_second = total_count / (end_time - start_time).seconds

        # 计算分值
        metric_name = "exec_time"
        exec_time_score = common.GenMetricScore(mysql_conn, metric_name, exec_per_second)
        if exec_per_second == None or exec_time_score == None:
            common.logger.error("exec_per_second or exec_time_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time, exec_per_second, exec_time_score)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算执行等待分值
def GenExecWaitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        exec_wait_str = """select (b.t_ws *1000 / a.diff_value) wait_per_count
                              from (select *
                                      from aop_perf_stat
                                     where name = 'execute count'
                                       and snap_id = %s
                                       and inst_id = %s) a,
                                   (select snap_id, inst_id, sum(t_ws) t_ws
                                      from (select snap_id,
                                                   inst_id,
                                                   sum(total_waits) t_ws
                                              from aop_wait_event
                                             where event in ('buffer busy waits')
                                               and snap_id = %s
                                               and inst_id = %s
                                             group by snap_id, inst_id
                                            union all
                                            select snap_id,
                                                   inst_id,
                                                   sum(total_waits) t_ws
                                              from aop_wait_event
                                             where event like 'log %%'
                                               and snap_id = %s
                                               and inst_id = %s
                                             group by snap_id, inst_id
                                            union all
                                            select snap_id,
                                                   inst_id,
                                                   sum(total_waits) t_ws
                                              from aop_wait_event
                                             where event like 'latch%%'
                                               and snap_id = %s
                                               and inst_id = %s
                                             group by snap_id, inst_id
                                            union all
                                            select snap_id,
                                                   inst_id,
                                                   sum(total_waits) t_ws
                                              from aop_wait_event
                                             where event like 'enq%%'
                                               and snap_id = %s
                                               and inst_id = %s
                                             group by snap_id, inst_id) ina
                                     group by ina.snap_id, ina.inst_id) b
                             where a.snap_id = b.snap_id
                               and a.inst_id = b.inst_id""" %(snap_id, inst_id, snap_id, inst_id,snap_id, inst_id,snap_id, inst_id,snap_id, inst_id)
        common.logger.debug(exec_wait_str)
        wait_per_exec = mysql_handle.GetSingleValue(mysql_conn, exec_wait_str)

        # 计算分值
        metric_name = "exec_wait"
        exec_wait_score = common.GenMetricScore(mysql_conn, metric_name, wait_per_exec)
        if wait_per_exec == None or exec_wait_score == None:
            common.logger.error("wait_per_exec or exec_wait_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time, wait_per_exec, exec_wait_score)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


