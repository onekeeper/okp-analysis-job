#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate logical IO Score Script
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
# 计算逻辑IO模块分值
###############################################################################
# 计算逻辑IO等待次数分值
def GenLIOWaitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        lread_wait_str = """select if(a.value=0,0,(b.t_ws *100000 / a.value))  waits_per_lread
                                  from (select inst_id, snap_id, sum(diff_value) value
                                              from aop_perf_stat
                                             where name in ('consistent gets from cache', 'db block gets from cache')
                                               and snap_id = %s
                                               and inst_id = %s
                                          group by inst_id, snap_id) a,
                                       (select snap_id, inst_id, sum(total_waits) t_ws
                                          from aop_wait_event
                                         where event in ('latch: cache buffers chains','buffer busy waits','free buffer waits','read by other session','latch: cache buffers lru chain')
                                           and snap_id = %s
                                           and inst_id = %s
                                         group by snap_id, inst_id) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(lread_wait_str)
        waits_per_lread = mysql_handle.GetSingleValue(mysql_conn, lread_wait_str)

        # 计算分值
        metric_name = "lio_wait"
        lread_wait_score = common.GenMetricScore(mysql_conn, metric_name, waits_per_lread)
        if waits_per_lread == None or lread_wait_score == None:
            common.logger.error("waits_per_lread or lread_wait_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     waits_per_lread, lread_wait_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算逻辑读命中率分值
def GenLIORatioScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        lread_ratio_str = """select if(a.value=0,100,(100 - 100 * b.value / a.value))  lread_ratio
                                  from (select inst_id, snap_id, sum(diff_value) value
                                          from aop_perf_stat
                                         where name in ('consistent gets from cache', 'db block gets from cache')
                                           and snap_id = %s
                                           and inst_id = %s
                                      group by inst_id, snap_id) a,
                                       (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name = 'physical reads cache'
                                           and snap_id = %s
                                           and inst_id = %s) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(lread_ratio_str)
        lread_ratio = mysql_handle.GetSingleValue(mysql_conn, lread_ratio_str)

        # 计算分值
        metric_name = "lio_ratio"
        lread_ratio_score = common.GenMetricScore(mysql_conn, metric_name, lread_ratio, 'N')
        if lread_ratio == None or lread_ratio_score == None:
            common.logger.error("lread_ratio or lread_ratio_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     lread_ratio, lread_ratio_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算每次逻辑读需要创建多少次cr块数分值
def GenLIOCRScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        lread_cr_str = """select if(b.value=0,0,(a.value * 10000 / b.value))  cr_per_lread
                                  from (select inst_id, snap_id, sum(diff_value) value
                                          from aop_perf_stat
                                         where name in ('CR blocks created', 'current blocks converted for CR')
                                           and snap_id = %s
                                           and inst_id = %s
                                      group by inst_id, snap_id) a,
                                       (select inst_id, snap_id, sum(diff_value) value
                                              from aop_perf_stat
                                             where name in ('consistent gets from cache', 'db block gets from cache')
                                               and snap_id = %s
                                               and inst_id = %s
                                          group by inst_id, snap_id) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(lread_cr_str)
        cr_per_lread = mysql_handle.GetSingleValue(mysql_conn, lread_cr_str)

        # 计算分值
        metric_name = "lio_cr"
        lread_cr_score = common.GenMetricScore(mysql_conn, metric_name, cr_per_lread)
        if cr_per_lread == None or lread_cr_score == None:
            common.logger.error("cr_per_lread or lread_cr_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     cr_per_lread, lread_cr_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0