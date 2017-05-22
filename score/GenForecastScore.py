#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Forecast Score Script
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
# 根据历史值预测同一时间段的分值
###############################################################################
# 计算第3层指标预测分值
def GenLevel3forecast(mysql_conn, inst_id, snap_id, metric_name):
    try:
        tab_name_score = common.GetTabNameByMetric(mysql_conn, metric_name)
        tab_name_forecast = common.GetForecastTabNameByMetric(mysql_conn, metric_name)

        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)
        #print snap_id, snap_start_time, snap_end_time

        nextday_snap_id = snap_id + 144         #此处按10分钟一个snap计算后一天同一时间段的预测值
        nextday_start_time = snap_start_time + datetime.timedelta(hours=24)
        nextday_end_time = snap_end_time + datetime.timedelta(hours=24)
        #print nextday_snap_id, nextday_start_time, nextday_end_time

        is_nextday_weekend = common.is_week_day(nextday_end_time)  # 下一天是否为周末
        score_total = 0
        value_total = 0
        score_count = 0
        forecast_value = -1
        forecast_score = -1

        hist_str = '''select end_time, value, score
                      from %s
                     where inst_id = %s
                     and mod(snap_id, 144) = mod(%s, 144)
                     and end_time > date_add(sysdate(), interval -2 month)
                     order by snap_id;'''%(tab_name_score, inst_id, snap_id)
        res = mysql_handle.GetMultiValue(mysql_conn, hist_str)
        for row in res:
            row_end_time = row[0]
            row_value = row[1]
            row_score = row[2]
            if common.is_week_day(row_end_time) == is_nextday_weekend:
                value_total = value_total + row_value
                score_total = score_total + row_score
                score_count = score_count + 1

        #print score_total, score_count

        if score_count > 0:
            forecast_value = value_total / score_count
            forecast_score = score_total / score_count

        if forecast_value == None or forecast_score == None:
            common.logger.error("forecast_value or forecast_score is None!!!")
            return 0

        # 存储计算结果
        res = common.SaveMetricScoreForecast(mysql_conn, metric_name, inst_id, nextday_snap_id, nextday_start_time, nextday_end_time, forecast_value, forecast_score)
        return res

    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0

# 计算第二层预测分值
def GenLevel2forecast(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        nextday_snap_id = snap_id + 144  # 此处按10分钟一个snap计算后一天同一时间段的预测值
        nextday_start_time = snap_start_time + datetime.timedelta(hours=24)
        nextday_end_time = snap_end_time + datetime.timedelta(hours=24)
        # print nextday_snap_id, nextday_start_time, nextday_end_time

        is_nextday_weekend = common.is_week_day(nextday_end_time)  # 下一天是否为周末

        #获取score_dept表名
        score_dept_tab = common.GetScoreDeptTabName(mysql_conn)

        # 获取所有的第2层指标相关表
        level2_tab_name = None
        query_str = "select tab_name, metric_name from %s where tab_level = 2"%(score_dept_tab)
        res = mysql_handle.GetMultiValue(mysql_conn, query_str)

        # 循环计算每一个2层level的得分
        result = 1
        for row in res:
            level2_tab_name = row[0]
            metric_name = row[1]
            #print level2_tab_name
            tab_forecast_name = level2_tab_name + "_forecast"

            score_total = 0
            score_count = 0
            forecast_score = -1

            hist_str = '''select end_time, score
                          from %s
                         where inst_id = %s
                         and mod(snap_id, 144) = mod(%s, 144)
                         and end_time > date_add(sysdate(), interval -2 month)
                         order by snap_id;''' % (level2_tab_name, inst_id, snap_id)
            common.logger.debug(hist_str)

            score_res = mysql_handle.GetMultiValue(mysql_conn, hist_str)
            common.logger.debug(score_res)

            for row in score_res:
                row_end_time = row[0]
                row_score = row[1]
                if common.is_week_day(row_end_time) == is_nextday_weekend:
                    score_total = score_total + row_score
                    score_count = score_count + 1

            # print score_total, score_count
            if score_count > 0:
                forecast_score = score_total / score_count

            if forecast_score == None:
                common.logger.error("forecast_score is None!!!")
                continue

            # 存储计算结果
            res = common.SaveMetricScoreForecast(mysql_conn, metric_name, inst_id, nextday_snap_id, nextday_start_time,
                                                 nextday_end_time, '', forecast_score)
            if res == None or res != 1:
                result = 0

        return result
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


# 计算Level 1预测分值
def GenLevel1forecast(mysql_conn, inst_id, snap_id):
    try:
        # 获取snap_id的时间
        [snap_start_time, snap_end_time] = common.GetTimeBySnapId(mysql_conn, snap_id)

        nextday_snap_id = snap_id + 144  # 此处按10分钟一个snap计算后一天同一时间段的预测值
        nextday_start_time = snap_start_time + datetime.timedelta(hours=24)
        nextday_end_time = snap_end_time + datetime.timedelta(hours=24)
        common.logger.info("nextday_snap_id: " + str(nextday_snap_id))

        is_nextday_weekend = common.is_week_day(nextday_end_time)  # 下一天是否为周末

        #通过score_dept获取level 1的表名
        score_dept_tab = common.GetScoreDeptTabName(mysql_conn)
        query_str = "select tab_name from %s where tab_level = 1" % (score_dept_tab)
        level1_tab_name = mysql_handle.GetSingleValue(mysql_conn, query_str)

        score_total = 0
        score_count = 0
        forecast_score = -1

        hist_str = '''select end_time, score
                          from %s
                         where inst_id = %s
                         and mod(snap_id, 144) = mod(%s, 144)
                         and end_time > date_add(sysdate(), interval -2 month)
                         order by snap_id;''' % (level1_tab_name, inst_id, snap_id)
        common.logging.debug(hist_str)

        res = mysql_handle.GetMultiValue(mysql_conn, hist_str)
        for row in res:
            row_end_time = row[0]
            row_score = row[1]
            if common.is_week_day(row_end_time) == is_nextday_weekend:
                score_total = score_total + row_score
                score_count = score_count + 1

        common.logging.debug("score_total: " + str(score_total) + "; " + "score_count: " + str(score_count))
        if score_count > 0:
            forecast_score = score_total / score_count

        if forecast_score == None:
            common.logger.error("forecast_score is None!!!")

        # 存储计算结果
        metric_name = 'score_db'
        res = common.SaveMetricScoreForecast(mysql_conn, metric_name, inst_id, snap_id, snap_start_time, snap_end_time, '', forecast_score)
        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0


###############################################################################
# main function
###############################################################################

