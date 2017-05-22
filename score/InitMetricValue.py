#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Init Metric Values Script
# Created on 2017-01-12
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import pymysql
import cx_Oracle
import time,datetime
import logging
import logging.config

import oracle_handle
import mysql_handle
import ErlangC

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('init')

###############################################################################
# define functions
###############################################################################
def Update_Metric_Threshold(mysql_conn, inst_id, metric_name, score_1, score_2, score_3, score_4):
    update_str = '''UPDATE metric_threshold
                    SET score_1 = %s, score_2 = %s, score_3 = %s, score_4 = %s
                    WHERE inst_id = %s
                    AND stat_name = '%s';''' % (score_1, score_2, score_3, score_4, inst_id, metric_name)

    logger.debug(update_str)
    res = mysql_handle.ExecuteSQL(mysql_conn, update_str)

def GetMaxExecSnapId(ora_conn, inst_id):
    exec_count = 0
    snap_id = 0

    # 获取执行次数最高的snap_id
    query_str = '''select snap_id, val
                                  from (select /*+NO_MERGE(pre)*/ /*+NO_MERGE(aft)*/
                                         aft.snap_id, (aft.value - pre.value) val
                                          from (select a.snap_id, a.instance_number, b.value
                                                  from sys.wrm$_snapshot  a,
                                                       sys.wrh$_sysstat   b,
                                                       sys.wrh$_stat_name n
                                                 where b.stat_id = n.stat_id
                                                   and n.stat_name = 'execute count'
                                                   and a.END_INTERVAL_TIME >= sysdate - 7
                                                   and b.snap_id = a.snap_id
                                                   and a.instance_number = b.instance_number) pre,
                                               (select a.snap_id, a.instance_number, b.value
                                                  from sys.wrm$_snapshot  a,
                                                       sys.wrh$_sysstat   b,
                                                       sys.wrh$_stat_name n
                                                 where b.stat_id = n.stat_id
                                                   and n.stat_name = 'execute count'
                                                   and a.END_INTERVAL_TIME >= sysdate - 7
                                                   and b.snap_id = a.snap_id
                                                   and a.instance_number = b.instance_number) aft
                                         where pre.snap_id = aft.snap_id - 1
                                           and pre.instance_number = aft.instance_number
                                           and pre.instance_number = %s
                                         order by val desc)
                                 where rownum = 1''' % inst_id
    logger.debug(query_str)
    res = oracle_handle.GetMultiValue(ora_conn, query_str)
    for row in res:
        snap_id = row[0]
        exec_count = row[1]

    logger.debug("snap_id: " + str(snap_id) + "; " + "exec_count: " + str(exec_count))

    return snap_id

############################################################
def InitParseThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'parse_cpu':

    snap_id = GetMaxExecSnapId(ora_conn, inst_id)

    # 获取parse cpu时间
    stat_name = 'parse time cpu'
    query_str = '''select (t.value - o.value) value
                              from sys.wrh$_sysstat   t,
                                   sys.wrh$_stat_name n,
                                   sys.wrh$_sysstat   o
                             where t.stat_id = o.stat_id
                               and t.stat_id = n.stat_id
                               and t.snap_id = o.snap_id + 1
                               and t.instance_number = o.instance_number
                               and t.instance_number = %s
                               and n.stat_name = '%s'
                               and t.snap_id = %s''' % (inst_id, stat_name, snap_id)
    logger.debug(query_str)
    parse_cpu = oracle_handle.GetSingleValue(ora_conn, query_str)

    # 获取parse 次数
    stat_name = 'parse count (total)'
    query_str = '''select (t.value - o.value) value
                              from sys.wrh$_sysstat   t,
                                   sys.wrh$_stat_name n,
                                   sys.wrh$_sysstat   o
                             where t.stat_id = o.stat_id
                               and t.stat_id = n.stat_id
                               and t.snap_id = o.snap_id + 1
                               and t.instance_number = o.instance_number
                               and t.instance_number = %s
                               and n.stat_name = '%s'
                               and t.snap_id = %s''' % (inst_id, stat_name, snap_id)
    logger.debug(query_str)
    parse_count = oracle_handle.GetSingleValue(ora_conn, query_str)

    logger.debug("parse_cpu: " + str(parse_cpu) + "; " + "parse count: " + str(parse_count))

    if parse_cpu or parse_count == None:
	parse_cpu=1
	parse_count=1000
    # 计算解析cpu的erlangC的到达率
    cpu_per_count = float('%0.3f' % (float(parse_cpu) / float(parse_count)))
    logger.debug("cpu_per_count: " + str(cpu_per_count))

    score_60 = ErlangC.GetInflectionPoint(1, cpu_per_count) * 100  # 60分点的值
    score_90 = score_60 * 0.95  # 90分点的值

    logger.debug("score_60: " + str(score_60) + "; " + "score_90: " + str(score_90))
    score_100 = 0  # 100分点的值
    score_0 = score_60 * 1.1  # 0分点的值

    # 将计算结果存入mysql库
    Update_Metric_Threshold(mysql_conn, inst_id, 'parse_cpu', score_100, score_90, score_60, score_0)


    #metric_name == 'parse_wait':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 100, 300, 600
    Update_Metric_Threshold(mysql_conn, inst_id, 'parse_wait', score_100, score_90, score_60, score_0)


    #metric_name == 'parse_hard':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 5, 40, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'parse_hard', score_100, score_90, score_60, score_0)



############################################################
def InitExecThreshold(ora_conn, mysql_conn, inst_id):
    # metric_name == 'exec_time'
    snap_id = GetMaxExecSnapId(ora_conn, inst_id)

    # 获取exec cpu时间
    stat_name = 'sql execute elapsed time'
    query_str = '''select (t.value - o.value) value
                              from dba_hist_sys_time_model   t,
                                   dba_hist_sys_time_model   o
                             where t.stat_id = o.stat_id
                               and t.snap_id = o.snap_id + 1
                               and t.instance_number = o.instance_number
                               and t.instance_number = %s
                               and t.stat_name = '%s'
                               and t.snap_id = %s''' % (inst_id, stat_name, snap_id)
    logger.debug(query_str)
    exec_time = oracle_handle.GetSingleValue(ora_conn, query_str)

    # 获取execute 次数
    stat_name = 'execute count'
    query_str = '''select (t.value - o.value) value
                              from sys.wrh$_sysstat   t,
                                   sys.wrh$_stat_name n,
                                   sys.wrh$_sysstat   o
                             where t.stat_id = o.stat_id
                               and t.stat_id = n.stat_id
                               and t.snap_id = o.snap_id + 1
                               and t.instance_number = o.instance_number
                               and t.instance_number = %s
                               and n.stat_name = '%s'
                               and t.snap_id = %s''' % (inst_id, stat_name, snap_id)
    logger.debug(query_str)
    exec_count = oracle_handle.GetSingleValue(ora_conn, query_str)

    logger.debug("execution time: " + str(exec_time) + "; " + "execution count: " + str(exec_count))

    # 取cpu个数
    query_str = "select value from v$parameter t where t.name = 'cpu_count'"
    cpu_count = oracle_handle.GetSingleValue(ora_conn, query_str)

    # 计算解析cpu的erlangC的到达率
    if  exec_time or exec_count == None:
        exec_time=1
        exec_count=1000 
    time_per_count = float('%0.3f' % (float(exec_time) / float(exec_count) / 1000)) 
    logger.debug("time_per_count: " + str(time_per_count))

    #Temporary solved set time_per_count=0.9 if its value >= 1
    if time_per_count >= 1 :
        time_per_count = 0.9

    score_60 = ErlangC.GetInflectionPoint(int(cpu_count), time_per_count) * 1000  # 60分点的值
    score_90 = score_60 * 0.95  # 90分点的值

    logger.debug("score_60: " + str(score_60) + "; " + "score_90: " + str(score_90))
    score_100 = 0  # 100分点的值
    score_0 = score_60 * 1.1  # 0分点的值

    # 将计算结果存入mysql库
    Update_Metric_Threshold(mysql_conn, inst_id, 'exec_time', score_100, score_90, score_60, score_0)

    #######################################
    #metric_name == 'exec_wait':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 50, 200, 1000
    Update_Metric_Threshold(mysql_conn, inst_id, 'exec_wait', score_100, score_90, score_60, score_0)

############################################################
def InitPIOThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'pio_rtime':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 10, 30, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'pio_rtime', score_100, score_90, score_60, score_0)

    # metric_name == 'pio_wtime':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 10, 30, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'pio_wtime', score_100, score_90, score_60, score_0)

    #metric_name == 'pio_d_rtime':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 15, 45, 150
    Update_Metric_Threshold(mysql_conn, inst_id, 'pio_d_rtime', score_100, score_90, score_60, score_0)

    #metric_name == 'pio_d_wtime':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 15, 45, 150
    Update_Metric_Threshold(mysql_conn, inst_id, 'pio_d_wtime', score_100, score_90, score_60, score_0)

    # metric_name == 'pio_req_write':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 50, 30, 15, 10
    Update_Metric_Threshold(mysql_conn, inst_id, 'pio_req_write', score_100, score_90, score_60, score_0)


############################################################
def InitLIOThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'lio_wait':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 50, 125, 100000
    Update_Metric_Threshold(mysql_conn, inst_id, 'lio_wait', score_100, score_90, score_60, score_0)

    # metric_name == 'lio_ratio':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 100, 95, 90, 70
    Update_Metric_Threshold(mysql_conn, inst_id, 'lio_ratio', score_100, score_90, score_60, score_0)

    #metric_name == 'lio_cr':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 2, 50, 1000
    Update_Metric_Threshold(mysql_conn, inst_id, 'lio_cr', score_100, score_90, score_60, score_0)

############################################################
def InitBCThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'bc_idxsplit':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'bc_idxsplit', score_100, score_90, score_60, score_0)

    # metric_name == 'bc_ckpt':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id,'bc_ckpt', score_100, score_90, score_60, score_0)

    #metric_name == 'bc_lru':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 5, 10, 15, 50
    Update_Metric_Threshold(mysql_conn, inst_id, 'bc_lru', score_100, score_90, score_60, score_0)

    #metric_name == 'bc_idxfp':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'bc_idxfp', score_100, score_90, score_60, score_0)

    # metric_name == 'bc_nowait':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 100, 95, 90, 70
    Update_Metric_Threshold(mysql_conn, inst_id, 'bc_nowait', score_100, score_90, score_60, score_0)

############################################################
def InitSPThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'sp_cursor':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 50, 80, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'sp_cursor', score_100, score_90, score_60, score_0)

    # metric_name == 'sp_dict':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 10, 40, 100
    Update_Metric_Threshold(mysql_conn, inst_id,'sp_dict', score_100, score_90, score_60, score_0)

    #metric_name == 'sp_lca':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 10, 40, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'sp_lca', score_100, score_90, score_60, score_0)

    #metric_name == 'sp_lcratio':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 100, 95, 90, 70
    Update_Metric_Threshold(mysql_conn, inst_id, 'sp_lcratio', score_100, score_90, score_60, score_0)

    # metric_name == 'sp_ssparse':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 100, 95, 60, 0
    Update_Metric_Threshold(mysql_conn, inst_id,'sp_ssparse', score_100, score_90, score_60, score_0)



############################################################
def InitRedoThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'redo_wait':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 50, 1000, 10000
    Update_Metric_Threshold(mysql_conn, inst_id, 'redo_wait', score_100, score_90, score_60, score_0)

    # metric_name == 'redo_lgsync':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id,'redo_lgsync', score_100, score_90, score_60, score_0)

    #metric_name == 'redo_lgwr':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'redo_lgwr', score_100, score_90, score_60, score_0)


############################################################
def InitUndoThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'undo_expired':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 1, 10, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'undo_expired', score_100, score_90, score_60, score_0)

    # metric_name == 'undo_wait':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 50, 500, 3600
    Update_Metric_Threshold(mysql_conn, inst_id, 'undo_wait', score_100, score_90, score_60, score_0)

    #metric_name == 'undo_rollback':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 1, 50, 200
    Update_Metric_Threshold(mysql_conn, inst_id, 'undo_rollback', score_100, score_90, score_60, score_0)


############################################################
def InitSQLThreshold(ora_conn, mysql_conn, inst_id):
    #metric_name == 'sql_long':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 0, 10, 40, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'sql_long', score_100, score_90, score_60, score_0)

    # metric_name == 'sql_cpu':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 2, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'sql_cpu', score_100, score_90, score_60, score_0)

    #metric_name == 'sql_io':
    # 以下源自经验值
    score_100,score_90,score_60,score_0 = 2, 5, 20, 100
    Update_Metric_Threshold(mysql_conn, inst_id, 'sql_io', score_100, score_90, score_60, score_0)



###############################################################################
def InitTargetThreshold(ora_conn, mysql_conn, inst_id):
    #初始化数据
    InitParseThreshold(ora_conn, mysql_conn, inst_id)
    InitExecThreshold(ora_conn, mysql_conn, inst_id)
    InitPIOThreshold(ora_conn, mysql_conn, inst_id)
    InitLIOThreshold(ora_conn, mysql_conn, inst_id)
    InitBCThreshold(ora_conn, mysql_conn, inst_id)
    InitSPThreshold(ora_conn, mysql_conn, inst_id)
    InitRedoThreshold(ora_conn, mysql_conn, inst_id)
    InitUndoThreshold(ora_conn, mysql_conn, inst_id)
    InitSQLThreshold(ora_conn, mysql_conn, inst_id)

###############################################################################
# main function
###############################################################################
if __name__=="__main__":
    # 建立mysql连接
    mysql_conn = mysql_handle.ConnectMysql('rac1')
    ora_conn = oracle_handle.ConnectOracle()

    InitTargetThreshold(ora_conn, mysql_conn, 1)




