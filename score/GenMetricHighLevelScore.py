#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Metric High Level Score Script
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
# 计算上层分值
###############################################################################
# 计算第二层分值
def GenLevel2Score(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        # 当前的时间段
        curhour = str(snap_end_time)[11:13] + ':00:00'

        # 获取当前库的score_dept表名和score_gen_rule的表名
        score_dept_tab = common.GetScoreDeptTabName(mysql_conn)
        score_gen_rule_tab = common.GetScoreGenRuleTabName(mysql_conn)

        # 获取所有的第2层指标相关表
        level2_tab_name = None
        query_str = "select tab_name, metric_name from %s where tab_level = 2" %(score_dept_tab)
        common.logger.debug(query_str)
        res = mysql_handle.GetMultiValue(mysql_conn, query_str)

        # 循环计算每一个level为2的得分
        result = 1
        for row in res:
            level2_tab_name = row[0]
            level2_metric_name = row[1]
            common.logger.debug("level2_tab_name: " + level2_tab_name)

            score_total = 0
            score_count = 0         #如果存在子项不为空，则加1
            query_str = "select tab_name,metric_name from %s where tab_level = 3 and up_table = '%s'" % (score_dept_tab, level2_tab_name)
            common.logger.debug(query_str)
            lever3res = mysql_handle.GetMultiValue(mysql_conn, query_str)
            #print res
            for row in lever3res:
                level3_tab_name = row[0]
                metric_name = row[1]
                common.logger.debug("level3_tab_name: " + level3_tab_name)
                common.logger.debug("metric_name: " + metric_name)

                query_str = "select score from %s where inst_id = %s and snap_id = %s" %(level3_tab_name, inst_id, snap_id)
                common.logger.debug(query_str)
                score = mysql_handle.GetSingleValue(mysql_conn, query_str)
                common.logging.debug(str(score))
                # 如果为空，则赋一个大的负值以做区分
                if score == None:
                    continue

                score_count = score_count + 1       # 计算子项分值的次数

                # 获取计算规则
                query_str = """select score_rule, value
                                  from %s
                                 where inst_id = %s
                                   and stat_name = '%s'
                                   and start_time = '%s' """ % (score_gen_rule_tab, inst_id, metric_name, curhour)
                common.logger.debug(query_str)
                res = mysql_handle.GetMultiValue(mysql_conn, query_str)
                score_rule = None
                for row in res:
                    score_rule = row[0]
                    value = row[1]

                if score_rule == "avg":
                    score_total = score_total + score * value / 100

            #print score_total
            if score_count == 0:
                common.logger.error("score_total is None!!!")
                continue

            # 存储计算结果
            res = common.SaveMetricScore(mysql_conn, level2_metric_name, inst_id, snap_id, snap_start_time,
                                         snap_end_time, 'NULL', score_total)
            if res == None or res != 1:
                result = 0

        return result
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0



###############################################################################
# 计算第一层分值
def GenLevel1Score(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)
        # 当前的时间段
        curhour = str(snap_end_time)[11:13] + ':00:00'
        common.logger.debug('curhour:' + curhour)

        # 获取当前库的score_dept表名和score_gen_rule的表名
        score_dept_tab = common.GetScoreDeptTabName(mysql_conn)
        score_gen_rule_tab = common.GetScoreGenRuleTabName(mysql_conn)

        # 获取所有的第1层指标相关表
        score_total = 0
        query_str = "select tab_name, metric_name from %s where tab_level = 2 " %(score_dept_tab)
        common.logging.debug(query_str)
        res = mysql_handle.GetMultiValue(mysql_conn, query_str)
        for row in res:
            level2_tab_name = row[0]
            metric_name = row[1]

            if metric_name == 'score_sql':
                query_str = "select score from %s where inst_id = %s order by snap_id desc limit 1" % (level2_tab_name, inst_id)
            else:
                query_str = "select score from %s where inst_id = %s and snap_id = %s" % (level2_tab_name, inst_id, snap_id)
            common.logging.debug(query_str)
            score = mysql_handle.GetSingleValue(mysql_conn, query_str)

            # 如果为空，则赋一个大的负值以做区分
            if score == None:
                continue

            # 获取计算规则
            query_str = '''select score_rule, value
                              from %s
                             where inst_id = %s
                               and stat_name = '%s'
                               and start_time = '%s';''' % (score_gen_rule_tab, inst_id, metric_name, curhour)
            common.logging.debug(query_str)
            res = mysql_handle.GetMultiValue(mysql_conn, query_str)
            common.logging.debug(res)
            if res <> ():
                score_rule = ""
                for row in res:
                    score_rule = row[0]
                    value = row[1]

                if score_rule == "avg":
                    score_total = score_total + score * value / 100
            else:
		print query_str
                common.logger.error("Have not get the rule!!!")

        # 存储计算结果
        metric_name = 'score_db'
        if score_total == None:
            common.logger.error("score_total is None!!!")
            return 0

        res = common.SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, snap_start_time,
                                     snap_end_time, '', score_total)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
