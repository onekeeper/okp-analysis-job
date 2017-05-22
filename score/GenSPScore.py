#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Shared Pool Score Script
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
# 计算Shared Pool模块分值
###############################################################################
# 计算open cursor分值
def GenCursorScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        sp_cursor_str = """select if(a.value=0,100,(100 - 100 * a.value / b.value))  sp_cursor
                                  from (select inst_id, max(session_count) as value
                                          from aop_open_cursor
                                         where snap_id = %s
                                           and inst_id = %s) a,
                                       (select inst_id, value
                                          from aop_parameter
                                         where name = 'open_cursors'
                                           and inst_id = %s
                                        order by snap_id desc
                                        limit 1) b
                                 where a.inst_id = b.inst_id""" % (snap_id, inst_id, inst_id)
        common.logger.debug(sp_cursor_str)
        sp_cursor = mysql_handle.GetSingleValue(mysql_conn, sp_cursor_str)

        # 计算分值
        metric_name = "sp_cursor"
        sp_cursor_score = common.GenMetricScore(mysql_conn, metric_name, sp_cursor)
        if sp_cursor == None or sp_cursor_score == None:
            common.logger.error("sp_cursor or sp_cursor_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sp_cursor, sp_cursor_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

# 计算Dictionary Cache Stats分值
def GenDictScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        sp_dict_str = """select ifnull(max(misspct),0)
                          from (select if(a.gets = 0, 0, (a.getmisses / a.gets)) misspct
                                  from (select parameter, sum(getmiss) getmisses, sum(gets) gets
                                          from aop_rowcache_misspct
                                         where snap_id = %s
                                           and inst_id = %s
                                         group by parameter) a
                                  where gets > 500) b""" % (snap_id, inst_id)
        common.logger.debug(sp_dict_str)
        sp_dict = mysql_handle.GetSingleValue(mysql_conn, sp_dict_str)

        # 计算分值
        metric_name = "sp_dict"
        sp_dict_score = common.GenMetricScore(mysql_conn, metric_name, sp_dict)
        if sp_dict == None or sp_dict_score == None:
            common.logger.error("sp_dict or sp_dict_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sp_dict, sp_dict_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

# 计算Library Cache Activity分值
def GenLCAScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        sp_lca_str = """select ifnull(max(misspct),0)
                          from (select if(a.gets = 0, 0, (100 - 100 * a.gethits / a.gets)) misspct
                                  from (select namespace, sum(gethits) gethits, sum(gets) gets
                                          from aop_lc_misspct
                                         where snap_id = %s
                                           and inst_id = %s
                                         group by namespace) a
                                  where gets > 500) b""" % (snap_id, inst_id)
        common.logger.debug(sp_lca_str)
        sp_lca = mysql_handle.GetSingleValue(mysql_conn, sp_lca_str)

        # 计算分值
        metric_name = "sp_lca"
        common.logger.debug("sp_lca:" + str(sp_lca))
        sp_lca_score = common.GenMetricScore(mysql_conn, metric_name, sp_lca)
        if sp_lca == None or sp_lca_score == None:
            common.logger.error("sp_lca or sp_lca_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sp_lca, sp_lca_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

# 计算Library Cache Hit分值
def GenLCRatioScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        sp_lcratio_str = """select if(a.value=0,100,(100 - 100 * b.value / a.value))  sp_lcratio
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
        common.logger.debug(sp_lcratio_str)
        sp_lcratio = mysql_handle.GetSingleValue(mysql_conn, sp_lcratio_str)

        # 计算分值
        metric_name = "sp_lcratio"
        sp_lcratio_score = common.GenMetricScore(mysql_conn, metric_name, sp_lcratio, 'N')
        if sp_lcratio == None or sp_lcratio_score == None:
            common.logger.error("sp_lcratio or sp_lcratio_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sp_lcratio, sp_lcratio_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算软软解析比率的分值
def GenSSParseScore(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        sp_ssparse_str = """select if(b.value=0,100,(100 * a.value / b.value))  sp_ssparse
                                  from (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name in ('session cursor cache hits')
                                           and snap_id = %s
                                           and inst_id = %s) a,
                                       (select inst_id, snap_id, diff_value as value
                                          from aop_perf_stat
                                         where name = 'parse count (total)'
                                           and snap_id = %s
                                           and inst_id = %s) b
                                 where a.snap_id = b.snap_id
                                   and a.inst_id = b.inst_id""" % (snap_id, inst_id, snap_id, inst_id)
        common.logger.debug(sp_ssparse_str)
        sp_ssparse = mysql_handle.GetSingleValue(mysql_conn, sp_ssparse_str)

        # 计算分值
        metric_name = "sp_ssparse"
        sp_ssparse_score = common.GenMetricScore(mysql_conn, metric_name, sp_ssparse, 'N')
        if sp_ssparse == None or sp_ssparse_score == None:
            common.logger.error("sp_ssparse or sp_ssparse_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time,
                                     sp_ssparse, sp_ssparse_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0