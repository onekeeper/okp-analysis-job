#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Buffer Cache Score Script
# Created on 2017-02-15
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
# 计算Buffer Cache模块分值
###############################################################################
# 计算索引分裂分值
def GenIndexSplitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        idx_split_str = """select sum(diff_value)
                              from aop_perf_stat
                             where name in ('leaf node splits','leaf node 90-10 splits','branch node splits','root node splits')
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(idx_split_str)
        idx_split = mysql_handle.GetSingleValue(mysql_conn, idx_split_str)

        # 计算上个月最大的split值
        max_split_str = """select sum(diff_value)
                              from aop_perf_stat
                             where name in ('leaf node splits','leaf node 90-10 splits','branch node splits','root node splits')
                               and inst_id = %s
                               and time >= date_sub(date_sub(date_format(now(), '%%y-%%m-%%d 00:00:00'),interval extract(day from now()) - 1 day),interval 1 month)
                               and time < date_sub(date_sub(date_format(now(), '%%y-%%m-%%d 00:00:00'),interval extract(day from now()) - 1 day),interval 0 month)
                             group by snap_id
                             order by sum(value) desc
                             limit 1""" %(inst_id)
        max_split = mysql_handle.GetSingleValue(mysql_conn, idx_split_str)

        # 计算分值
        metric_name = "bc_idxsplit"
        common.logger.debug("max_split: " + str(max_split))
        common.logger.debug("idx_split: " + str(idx_split))
        idx_split_score = 0
        if max_split == None or max_split == 0:
            idx_split_score = 100
        else:
            split_increase = 100 * (idx_split - max_split) / max_split
            idx_split_score = common.GenMetricScore(mysql_conn, metric_name, split_increase)

        if idx_split == None or idx_split_score == None:
            common.logger.error("idx_split or idx_split_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     idx_split, idx_split_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算检查点分值
def GenCKPTScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        bc_ckpt_str = """select if((b.value - a.value)=0,100,(a.value/ (b.value - a.value)))  bc_ckpt
                                  from (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name = ('DBWR checkpoint buffers written')
                                           and snap_id = %s
                                           and inst_id = %s) a,
                                       (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name = 'physical writes from cache'
                                           and snap_id = %s
                                           and inst_id = %s) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(bc_ckpt_str)
        bc_ckpt = mysql_handle.GetSingleValue(mysql_conn, bc_ckpt_str)

        # 计算上个月最大的check point值
        max_ckpt_str = """select ifnull(max_ckpt, 0) from
                        (select a.snap_id, if((b.value - a.value) = 0, 100, (a.value / (b.value - a.value))) max_ckpt
                          from (select snap_id, diff_value as value
                                  from aop_perf_stat
                                 where name = ('DBWR checkpoint buffers written')
                                   and time >= date_sub(date_sub(date_format(now(), '%%y-%%m-%%d 00:00:00'),interval extract(day from now()) - 1 day),interval 1 month)
                                   and time < date_sub(date_sub(date_format(now(), '%%y-%%m-%%d 00:00:00'),interval extract(day from now()) - 1 day),interval 0 month)
                                   and inst_id = %s) a,
                               (select snap_id, diff_value as value
                                  from aop_perf_stat
                                 where name = 'physical writes from cache'
                                   and time >= date_sub(date_sub(date_format(now(), '%%y-%%m-%%d 00:00:00'),interval extract(day from now()) - 1 day),interval 1 month)
                                   and time < date_sub(date_sub(date_format(now(), '%%y-%%m-%%d 00:00:00'),interval extract(day from now()) - 1 day),interval 0 month)
                                   and inst_id = %s) b
                         where a.snap_id = b.snap_id) c
                         order by max_ckpt DESC
                         limit 1"""% (inst_id, inst_id)
        common.logger.debug(max_ckpt_str)
        max_ckpt = mysql_handle.GetSingleValue(mysql_conn, max_ckpt_str)

        # 计算分值
        metric_name = "bc_ckpt"
        bc_ckpt_score = 0
        if max_ckpt == None or max_ckpt == 0:
            bc_ckpt_score = 100
        else:
            ckpt_increase = 100 * (bc_ckpt - max_ckpt) / max_ckpt
            common.logger.debug("ckpt_increase: " + str(ckpt_increase))
            bc_ckpt_score = common.GenMetricScore(mysql_conn, metric_name, ckpt_increase)

        if bc_ckpt == None or bc_ckpt_score == None:
            common.logger.error("bc_ckpt or bc_ckpt_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     bc_ckpt, bc_ckpt_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算LRU分值
def GenLRUScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        bc_lru_str = """select if(a.value=0,0,(10 * a.value / b.value))  bc_lru
                                  from (select inst_id, snap_id, sum(diff_value) value
                                          from aop_perf_stat
                                         where name in ('free buffer inspected', 'dirty buffers inspected')
                                           and snap_id = %s
                                           and inst_id = %s
                                      group by inst_id, snap_id) a,
                                       (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name = 'free buffer requested'
                                           and snap_id = %s
                                           and inst_id = %s) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(bc_lru_str)
        bc_lru = mysql_handle.GetSingleValue(mysql_conn, bc_lru_str)

        # 计算分值
        metric_name = "bc_lru"
        common.logger.debug("bc_lru: " + str(bc_lru))
        bc_lru_score = common.GenMetricScore(mysql_conn, metric_name, bc_lru)
        if bc_lru == None or bc_lru_score == None:
            common.logger.error("bc_lru or bc_lru_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     bc_lru, bc_lru_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算index failed probes分值
def GenIndexFailProbeScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        bc_idxfp_str = """select 10 * diff_value
                              from aop_perf_stat
                             where name = 'failed probes on index block reclamation'
                               and snap_id = %s
                               and inst_id = %s""" % (snap_id, inst_id)
        common.logger.debug(bc_idxfp_str)
        bc_idxfp = mysql_handle.GetSingleValue(mysql_conn, bc_idxfp_str)

        # 计算分值
        metric_name = "bc_idxfp"
        common.logger.debug("bc_idxfp: " + str(bc_idxfp))
        bc_idxfp_score = common.GenMetricScore(mysql_conn, metric_name, bc_idxfp)
        if bc_idxfp == None or bc_idxfp_score == None:
            common.logger.error("bc_idxfp or bc_idxfp_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     bc_idxfp, bc_idxfp_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

# 计算buffer nowait分值
def GenBFNoWaitScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        bc_nowait_str = """select if(a.value=0,100,(100 - 100 * b.t_ws / a.value))  bc_nowait
                                  from (select inst_id, snap_id, sum(diff_value) value
                                          from aop_perf_stat
                                         where name in ('consistent gets from cache', 'db block gets from cache')
                                           and snap_id = %s
                                           and inst_id = %s
                                      group by inst_id, snap_id) a,
                                       (select inst_id, snap_id, sum(total_waits) t_ws
                                          from aop_wait_event
                                         where snap_id = %s
                                           and inst_id = %s
					   and event='buffer busy waits') b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(bc_nowait_str)
        bc_nowait = mysql_handle.GetSingleValue(mysql_conn, bc_nowait_str)

        # 计算分值
        metric_name = "bc_nowait"
        bc_nowait_score = common.GenMetricScore(mysql_conn, metric_name, bc_nowait, 'N')
        if bc_nowait == None or bc_nowait_score == None:
            common.logger.error("bc_nowait or bc_nowait_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     bc_nowait, bc_nowait_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
