#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Metric Score Script
# Created on 2017-03-31
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import time,datetime
import traceback
import logging
import logging.config

import common
import cx_Oracle
import mysql_handle

###############################################################################
# 回写最新的分值到hzmc_data库aop_model_score表
###############################################################################
def ReWriteScore(mysql_conn, object_id, inst_id, snap_id):
    try:
        # 建立总览库mysql连接
        mysql_hzmcdata = mysql_handle.ConnectMysql()

        # 查询需要更新的 model score
        query_str = """select sys_id,table_name from aop_model_score where object_id = '%s' """ % (object_id)
        common.logging.debug(query_str)
        res = mysql_handle.GetMultiValue(mysql_hzmcdata, query_str)
        for row in res:
	    just_table_name=row[1]
            tab_name = row[0]+'_'+row[1]
            common.logging.debug("tab_name: " + tab_name)

            score_str = """select ifnull(score, 0) from %s where inst_id = %s and snap_id = %s""" % (tab_name, inst_id, snap_id)
            common.logging.debug(score_str)
            score = mysql_handle.GetSingleValue(mysql_conn, score_str)

            if score == None:
                common.logger.error("score is None!!!")
                continue

            #更新到aop_model_score表
            update_str = """update aop_model_score set score = %s, update_time = NOW() where object_id = '%s' and table_name = '%s'""" % (score, object_id, just_table_name)
            common.logging.debug(update_str)
            res = mysql_handle.ExecuteSQL(mysql_hzmcdata, update_str)


        # 更新到aop_object_score表
        score_db_tab = common.GetTabNameByMetric(mysql_conn, 'score_db')
        score_str = """select ifnull(score, 0) from %s where inst_id = %s and snap_id = %s""" % (score_db_tab, inst_id, snap_id)
        object_score = mysql_handle.GetSingleValue(mysql_conn, score_str)

        update_str = """update aop_object_score set score = %s, update_time = NOW() where object_id = '%s' """ % (object_score, object_id)
        res = mysql_handle.ExecuteSQL(mysql_hzmcdata, update_str)

        # 更新到aop_system表
        update_str = """update aop_system set score = %s, update_time = NOW() where sys_id in (select sys_id from aop_object_score where object_id = '%s') """ % (object_score, object_id)
        res = mysql_handle.ExecuteSQL(mysql_hzmcdata, update_str)

        mysql_handle.CloseMysql(mysql_hzmcdata)

        return res
    except Exception, e:
        common.logger.error(traceback.format_exc())
        return 0
