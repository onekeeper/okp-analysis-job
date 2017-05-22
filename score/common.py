# -*- coding: UTF-8 -*-
###############################################################################
# Common function Scripts
# Created on 2017-01-14
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import ConfigParser
import os
import sys
import time,datetime

import cx_Oracle
import mysql_handle

import logging
import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('GenMetric')
###############################################################################
# define functions
###############################################################################
# 判断是否为周末, 如果是，则返回1，不是，返回0
def is_week_day(date):
    day = date.weekday()
    val=0
    if day==5 or day==6 :
        val=1
    return val

def GetLastSnapId(mysql_conn):
    # 获取最新的snap_id
    snap_id = None

    query_snap = "select snap_id from aop_snap order by snap_id desc limit 1"
    snap_id = mysql_handle.GetSingleValue(mysql_conn,query_snap)

    return snap_id

def GetTimeBySnapId(mysql_conn, snap_id):
    # 根据snap_id获取时间
    start_time = None
    end_time = None

    query_snap = "select start_time, end_time from aop_snap where snap_id = %s" %(snap_id)
    res = mysql_handle.GetMultiValue(mysql_conn,query_snap)

    for row in res:
        start_time = row[0]
        end_time = row[1]

    return start_time, end_time

# 根据metric_name获取分值表名
def GetTabNameByMetric(mysql_conn, metric_name):
    query_str = "select tab_name from table_map where metric_name = '%s' and tab_type = 'score' " %(metric_name)
    tab_name = mysql_handle.GetSingleValue(mysql_conn,query_str)
    return tab_name


# 根据metric_name获取分值表名
def GetForecastTabNameByMetric(mysql_conn, metric_name):
    query_str = "select tab_name from table_map where metric_name = '%s' and tab_type = 'forecast' " %(metric_name)
    tab_name = mysql_handle.GetSingleValue(mysql_conn,query_str)

    return tab_name


# 获取当前库中存储分值表层级的表名
def GetScoreDeptTabName(mysql_conn):
    query_str = "select tab_name from table_map where tab_type = 'score_dept' "
    tab_name = mysql_handle.GetSingleValue(mysql_conn,query_str)

    return tab_name

# 获取当前库中存储分值计算规则的表名
def GetScoreGenRuleTabName(mysql_conn):
    query_str = "select tab_name from table_map where tab_type = 'score_gen_rule' "
    tab_name = mysql_handle.GetSingleValue(mysql_conn,query_str)

    return tab_name


# 获取metric_name 获取 metric_level
def GetMetricLevelByName(mysql_conn, metric_name):
    query_snap = "select metric_level from table_map where metric_name = '%s' limit 1 "%(metric_name)
    metric_level = mysql_handle.GetSingleValue(mysql_conn,query_snap)

    return metric_level

# 存储计算的分值
def SaveMetricScore(mysql_conn, metric_name, inst_id, snap_id, start_time, end_time,  value, score):
    # 根据metric_name获取表名
    tab_name = GetTabNameByMetric(mysql_conn, metric_name)
    metric_level = GetMetricLevelByName(mysql_conn, metric_name)

    # 查看是否已经计算过分值
    sql_str = "select count(1) from %s where inst_id = %s and snap_id = %s" % (tab_name, inst_id, snap_id)
    logger.debug(sql_str)

    has_record = mysql_handle.GetSingleValue(mysql_conn, sql_str)
    if metric_level == 3:
        if has_record == 0:
            exec_str = "insert into %s(inst_id, snap_id, start_time, end_time,  value, score) \
                                values(%s, %s, '%s', '%s', %s, %s)" % (
            tab_name, inst_id, snap_id, start_time, end_time, value, score)
        else:
            exec_str = "update %s set value = %s, score = %s where inst_id = %s and snap_id = %s" % (
            tab_name, value, score, inst_id, snap_id)
    elif metric_level == 1 or metric_level == 2:
        if has_record == 0:
            exec_str = "insert into %s(inst_id, snap_id, start_time, end_time, score) \
                                values(%s, %s, '%s', '%s', %s)" % (
            tab_name, inst_id, snap_id, start_time, end_time, score)
        else:
            exec_str = "update %s set score = %s where inst_id = %s and snap_id = %s" % (
            tab_name, score, inst_id, snap_id)

    logger.debug(exec_str)
    result = mysql_handle.ExecuteSQL(mysql_conn, exec_str)

    return result

# 存储计算的预测分值
def SaveMetricScoreForecast(mysql_conn, metric_name, inst_id, snap_id, start_time, end_time,  value, score):
    # 根据metric_name获取预测表名
    forecast_tab_name = GetForecastTabNameByMetric(mysql_conn, metric_name)
    metric_level = GetMetricLevelByName(mysql_conn, metric_name)

    # 查看是否已经计算过分值
    sql_str = "select count(1) from %s where inst_id = %s and snap_id = %s" % (forecast_tab_name, inst_id, snap_id)

    has_record = mysql_handle.GetSingleValue(mysql_conn, sql_str)
    if metric_level == 3:
        if has_record == 0:
            exec_str = "insert into %s(inst_id, snap_id, start_time, end_time,  value, score) \
                                values(%s, %s, '%s', '%s', %s, %s)" % (
                forecast_tab_name, inst_id, snap_id, start_time, end_time, value, score)
        else:
            exec_str = "update %s set value = %s, score = %s where inst_id = %s and snap_id = %s" % (
                forecast_tab_name, value, score, inst_id, snap_id)
    elif metric_level == 1 or metric_level == 2:
        if has_record == 0:
            exec_str = "insert into %s(inst_id, snap_id, start_time, end_time, score) \
                                values(%s, %s, '%s', '%s', %s)" % (
                forecast_tab_name, inst_id, snap_id, start_time, end_time, score)
        else:
            exec_str = "update %s set score = %s where inst_id = %s and snap_id = %s" % (
                forecast_tab_name, score, inst_id, snap_id)

    result = mysql_handle.ExecuteSQL(mysql_conn, exec_str)

    return result


def GenMetricScore(mysql_conn, metric_name, metric_value, is_inverted='Y'):
    # 获取评分表
    score_1 = None
    score_2 = None
    score_3 = None
    score_4 = None

    scorelist_str = "select score_1, score_2, score_3, score_4 from metric_threshold where inst_id = 1 and stat_name = '%s'"%(metric_name)
    res = mysql_handle.GetMultiValue(mysql_conn, scorelist_str)
    for row in res:
        score_1 = row[0]
        score_2 = row[1]
        score_3 = row[2]
        score_4 = row[3]

    logger.debug("score_1: " + str(score_1) + ";" + "score_2: " + str(score_2) + ";" + "score_3: " + str(score_3) + ";" + "score_4: " + str(score_4))

    # 计算当前得分
    metric_score = 0

    # is_inverted 为 "Y"时，value越大，分值越低
    if is_inverted == 'Y':
        if metric_value >= score_4:                  # 超过0分阈值，得0分
            metric_score = 0
        elif metric_value >= score_3:                # 在 0 到 60分之间
            metric_score = 60 - (metric_value - score_3) * 60 / (score_4 - score_3)
        elif metric_value >= score_2:                # 在 60 到 90分之间
            metric_score = 90 - (metric_value - score_2) * 30 / (score_3 - score_2)
        elif metric_value >= score_1:                                     # 在 90 到 100分之间
            metric_score = 100 - (metric_value - score_1) * 10 / (score_2 - score_1)
        else:
            metric_score = 100
    else:
        if metric_value >= score_1:                  # 超过100分值，得100分
            metric_score = 100
        elif metric_value >= score_2:                # 在 100 到 90分之间
            metric_score = 100 - (score_1 - metric_value) * 10 / (score_1 - score_2)
        elif metric_value >= score_3:                # 在 90 到 60分之间
            metric_score = 90 - (score_2 - metric_value) * 30 / (score_2 - score_3)
        elif metric_value >= score_4:                 # 在 60 到 0分之间
            metric_score = 60 - (score_3 - metric_value) * 60 / (score_3 - score_4)
        else:
            metric_score = 0

    return metric_score


