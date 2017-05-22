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
from mysql_handle import *
###############################################################################
# rac_score
###############################################################################
def GenRacScore(mysql_conn,inst_id,snap_id):
    # 建立mysql连接
    try:
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        rac_str = "select a.ksxp_time/b.ksxp*1000 from \
                (select inst_id,snap_id,value ksxp_time from aop_dlm_misc \
                where NAME='msgs sent queue time on ksxp (ms)' ) a,\
                (select snap_id,inst_id,value ksxp from aop_dlm_misc \
                where NAME='msgs sent queued on ksxp') b \
                where a.inst_id=b.inst_id and a.snap_id=b.snap_id and a.snap_id=%s and a.inst_id=%s"%(snap_id,inst_id)
        common.logger.debug(rac_str)
        rac_value = mysql_handle.GetSingleValue(mysql_conn, rac_str)
	if rac_value == None:
	    rac_value=0
	else:
	    pass
    # 计算分值
        metric_name = "rac_ksxp"
        rac_score = common.GenMetricScore(mysql_conn, metric_name, rac_value)
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                    rac_value, rac_score)
        print res
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
	
def GenRacBusy(mysql_conn,inst_id,snap_id):
    try:
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)
	busy_str="select (busy.gcb/rec.gcr)*1000 from \
		(select snap_id,inst_id,sum(TOTAL_WAITS) gcb from aop_wait_event \
		where event  in ('gc cr block busy','gc current block busy','gc current grant busy') \
		group by snap_id,inst_id  ) busy, \
		(select snap_id,inst_id,sum(diff_value) gcr \
		from aop_perf_stat where name in ('gc cr blocks received','gc current blocks received') \
		group by snap_id,inst_id ) rec where busy.inst_id=rec.inst_id \
		and busy.snap_id=rec.snap_id and rec.snap_id=%s and rec.inst_id=%s"%(snap_id,inst_id)
 	common.logger.debug(busy_str)
	busy_value = mysql_handle.GetSingleValue(mysql_conn, busy_str)
	if busy_value == None:
	    busy_value=0
	else:
	    pass
	metric_name = "rac_busy"
	busy_score = common.GenMetricScore(mysql_conn, metric_name, busy_value)
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     busy_value, busy_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
#mysql_conn = ConnectMysql()
#x=GenRacBusy(mysql_conn,2,2)
#print x
