#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Metric Score Script
# Created on 2017-01-12
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import time,datetime
import multiprocessing
import traceback
import os
import logging
import logging.config

import oracle_handle
import mysql_handle

import common
import GenParseScore    as GPS
import GenExecScore     as GES
import GenPhysicalIOScore as GPIOS
import GenLogicalIOScore as GLIOS
import GenBCScore as GBC
import GenSPScore as GSP
import GenRedoScore as GRS
import GenUndoScore as GUS
import GenSQLScore as GSS
import GenRacScore as GRAC

import GenForecastScore   as GFS
import GenMetricHighLevelScore  as GMHLS
import InitMetricValue as IMV

import ReWriteScore as RWS

logging.config.fileConfig(os.path.join(os.path.dirname(__file__), 'logging.conf'))
logger = logging.getLogger('GenMetric')
###############################################################################
# functions
###############################################################################
def GenScore(ora_conn_str, mysql_dbname, target_name, inst_id, snap_id):
    # 建立mysql连接
    mysql_conn = mysql_handle.ConnectMysql(mysql_dbname)
    ora_conn = oracle_handle.ConnectSpecialOracle(ora_conn_str)

    # Init score thresholds
    is_init = IsScoreThresholdInit(mysql_conn, inst_id)
    if is_init == 0:
        IMV.InitTargetThreshold(ora_conn, mysql_conn,inst_id)

    # 计算解析模块分值
    res_parse = GenParseScore(mysql_conn, inst_id, snap_id)

    # 计算执行模块分值
    res_exec = GenExecScore(mysql_conn, inst_id, snap_id)

    # 计算物理IO模块分值
    res_pio = GenPIOScore(mysql_conn, inst_id, snap_id)

    # 计算逻辑IO模块分值
    res_lio = GenLIOScore(mysql_conn, inst_id, snap_id)

    # 计算Buffer cache模块分值
    res_bc = GenBCScore(mysql_conn, inst_id, snap_id)

    # 计算shared pool模块分值
    res_sp = GenSPScore(mysql_conn, inst_id, snap_id)

    # 计算Redo模块分值
    res_redo = GenRedoScore(mysql_conn, inst_id, snap_id)

    # 计算undo模块分值
    res_undo = GenUndoScore(mysql_conn, inst_id, snap_id)

    # 计算sql模块分值
    res_sql = GenSQLScore(mysql_conn, inst_id, snap_id)

    # 计算RAC模块分值
    res_rac = GenRacScore(mysql_conn, inst_id, snap_id)

    # 生成上层分值和预测分值
    res_level2 = GenLevel2Score(mysql_conn, inst_id, snap_id)
    res_level1 = GenLevel1Score(mysql_conn, inst_id, snap_id)

    # 回写最新的分值到hzmc_data库aop_model_score表
    res_rewrite = RWS.ReWriteScore(mysql_conn, target_name, inst_id, snap_id)

    # 关闭mysql连接
    mysql_handle.CloseMysql(mysql_conn)

    #logger.info('res_sp: ' + str(res_sp))
    if res_parse == 1 and res_exec == 1 and res_pio == 1 and res_lio == 1 and res_bc == 1 and res_sp ==1 and res_redo == 1 \
            and res_undo == 1 and res_sql == 1 and res_level2 == 1 and res_level1 == 1 and res_rewrite == 1:
        logger.info('Generate DB Score succss.')
        return 1
    else:
        logger.info('Generate DB Score failed.')
        return 0


###############################################################################
# 判断计算分值表阈值是否已经初始化
def IsScoreThresholdInit(mysql_conn, inst_id):
    if mysql_conn:
        query_str = "select score_1 from metric_threshold where stat_name = 'sql_io' and inst_id = %s" %(inst_id)

        res = mysql_handle.GetSingleValue(mysql_conn, query_str)

        if res == None:
            return 0
        else:
            return 1


###############################################################################
# 计算解析模块分值
def GenParseScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Parse Score starting...')
    res_cpu_1 = GPS.GenParseCPUScore(mysql_conn, inst_id, snap_id)
    res_cpu_2 = GPS.GenParseWaitScore(mysql_conn, inst_id, snap_id)
    res_cpu_3 = GPS.GenHardParseScore(mysql_conn, inst_id, snap_id)
    #print res_cpu_1, res_cpu_2, res_cpu_3

    res_cpu_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'parse_cpu')
    res_cpu_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'parse_wait')
    res_cpu_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'parse_hard')
    #print res_cpu_f1, res_cpu_f2, res_cpu_f3

    if res_cpu_1 == 1 and res_cpu_2 == 1 and res_cpu_3 == 1 and res_cpu_f1 == 1 and res_cpu_f2 == 1 and res_cpu_f3 ==1:
        logger.info('Generate Parse Score succss.')
        return 1
    else:
        logger.info('Generate Parse Score failed.')
        return 0

###############################################################################
# 计算执行模块分值
def GenExecScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Exec Score starting...')
    res_exec_1 = GES.GenExecTimeScore(mysql_conn, inst_id, snap_id)
    res_exec_2 = GES.GenExecWaitScore(mysql_conn, inst_id, snap_id)
    #print res_exec_1, res_exec_2

    res_exec_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'exec_time')
    res_exec_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'exec_wait')
    #print res_exec_f1, res_exec_f2

    if res_exec_1 == 1 and res_exec_2 == 1 and res_exec_f1 == 1 and res_exec_f2 == 1:
        logger.info('Generate Exec Score succss.')
        return 1
    else:
        logger.info('Generate Exec Score failed.')
        return 0

###############################################################################
# 计算物理IO模块分值
def GenPIOScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Physical IO Score starting...')
    res_pio_1 = GPIOS.GenPIOReadTimeScore(mysql_conn, inst_id, snap_id)
    res_pio_2 = GPIOS.GenPIOWriteTimeScore(mysql_conn, inst_id, snap_id)
    res_pio_3 = GPIOS.GenPIODirectReadTimeScore(mysql_conn, inst_id, snap_id)
    res_pio_4 = GPIOS.GenPIODirectWriteTimeScore(mysql_conn, inst_id, snap_id)
    res_pio_5 = GPIOS.GenPIOReqPerWriteScore(mysql_conn, inst_id, snap_id)
    #print res_pio_1, res_pio_2, res_pio_3, res_pio_4, res_pio_5

    res_pio_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'pio_rtime')
    res_pio_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'pio_wtime')
    res_pio_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'pio_d_rtime')
    res_pio_f4 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'pio_d_wtime')
    res_pio_f5 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'pio_req_write')
    #print res_pio_f1, res_pio_f2, res_pio_f3, res_pio_f4, res_pio_f5

    if res_pio_1 == 1 and res_pio_2 == 1 and res_pio_3 == 1 and res_pio_4 == 1 and res_pio_5 == 1 and \
        res_pio_f1 == 1 and res_pio_f2 == 1 and res_pio_f3 == 1 and res_pio_f4 == 1 and res_pio_f5 == 1:
        logger.info('Generate Physical IO Score succss.')
        return 1
    else:
        logger.info('Generate Physical IO Score failed.')
        return 0


###############################################################################
# 计算逻辑IO模块分值
def GenLIOScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Logical IO Score starting...')
    res_lio_1 = GLIOS.GenLIOWaitScore(mysql_conn, inst_id, snap_id)
    res_lio_2 = GLIOS.GenLIORatioScore(mysql_conn, inst_id, snap_id)
    res_lio_3 = GLIOS.GenLIOCRScore(mysql_conn, inst_id, snap_id)
    #print res_lio_1, res_lio_2, res_lio_3

    res_lio_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'lio_wait')
    res_lio_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'lio_ratio')
    res_lio_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'lio_cr')
    #print res_lio_f1, res_lio_f2, res_lio_f3

    if res_lio_1 == 1 and res_lio_2 == 1 and res_lio_3 == 1 and res_lio_f1 == 1 and res_lio_f2 == 1 and res_lio_f3 == 1:
        logger.info('Generate Logical IO Score succss.')
        return 1
    else:
        logger.info('Generate Logical IO Score failed.')
        return 0

###############################################################################
# 计算 Buffer cache 模块分值
def GenBCScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Buffer cache Score starting...')
    res_bc_1 = GBC.GenIndexSplitScore(mysql_conn, inst_id, snap_id)
    res_bc_2 = GBC.GenCKPTScore(mysql_conn, inst_id, snap_id)
    res_bc_3 = GBC.GenLRUScore(mysql_conn, inst_id, snap_id)
    res_bc_4 = GBC.GenIndexFailProbeScore(mysql_conn, inst_id, snap_id)
    res_bc_5 = GBC.GenBFNoWaitScore(mysql_conn, inst_id, snap_id)
    #print res_bc_1, res_bc_2, res_bc_3, res_bc_4, res_bc_5

    res_bc_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'bc_idxsplit')
    res_bc_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'bc_ckpt')
    res_bc_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'bc_lru')
    res_bc_f4 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'bc_idxfp')
    res_bc_f5 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'bc_nowait')
    #print res_bc_f1, res_bc_f2, res_bc_f3, res_bc_f4, res_bc_f5

    if res_bc_1 == 1 and res_bc_2 == 1 and res_bc_3 == 1 and res_bc_4 == 1 and res_bc_5 == 1 and \
                    res_bc_f1 == 1 and res_bc_f2 == 1 and res_bc_f3 == 1 and res_bc_f4 == 1 and res_bc_f5 == 1:
        logger.info('Generate Buffer cache Score succss.')
        return 1
    else:
        logger.info('Generate Buffer cache Score failed.')
        return 0

###############################################################################
# 计算 Shared Pool 模块分值
def GenSPScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Shared Pool Score starting...')
    res_sp_1 = GSP.GenCursorScore(mysql_conn, inst_id, snap_id)
    res_sp_2 = GSP.GenDictScore(mysql_conn, inst_id, snap_id)
    res_sp_3 = GSP.GenLCAScore(mysql_conn, inst_id, snap_id)
    res_sp_4 = GSP.GenLCRatioScore(mysql_conn, inst_id, snap_id)
    res_sp_5 = GSP.GenSSParseScore(mysql_conn, inst_id, snap_id)
    #print res_sp_1, res_sp_2, res_sp_3, res_sp_4, res_sp_5

    res_sp_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sp_cursor')
    res_sp_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sp_dict')
    res_sp_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sp_lca')
    res_sp_f4 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sp_lcratio')
    res_sp_f5 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sp_ssparse')
    #print res_sp_f1, res_sp_f2, res_sp_f3, res_sp_f4, res_sp_f5

    if res_sp_1 == 1 and res_sp_2 == 1 and res_sp_3 == 1 and res_sp_4 == 1 and res_sp_5 == 1 and \
                    res_sp_f1 == 1 and res_sp_f2 == 1 and res_sp_f3 == 1 and res_sp_f4 == 1 and res_sp_f5 == 1:
        logger.info('Generate Shared Pool Score succss.')
        return 1
    else:
        logger.info('Generate Shared Pool Score failed.')
        return 0

###############################################################################
# 计算 Redo 模块分值
def GenRedoScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Redo Score starting...')
    res_redo_1 = GRS.GenRedoWaitScore(mysql_conn, inst_id, snap_id)
    res_redo_2 = GRS.GenLgSyncScore(mysql_conn, inst_id, snap_id)
    res_redo_3 = GRS.GenLGWRScore(mysql_conn, inst_id, snap_id)
    #print res_redo_1, res_redo_2, res_redo_3

    res_redo_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'redo_wait')
    res_redo_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'redo_lgsync')
    res_redo_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'redo_lgwr')
    #print res_redo_f1, res_redo_f2, res_redo_f3

    if res_redo_1 == 1 and res_redo_2 == 1 and res_redo_3 == 1 and \
                    res_redo_f1 == 1 and res_redo_f2 == 1 and res_redo_f3 == 1:
        logger.info('Generate Redo Score succss.')
        return 1
    else:
        logger.info('Generate Redo Score failed.')
        return 0

###############################################################################
# 计算 Undo 模块分值
def GenUndoScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate Undo Score starting...')
    res_undo_1 = GUS.GenUndoExpiredScore(mysql_conn, inst_id, snap_id)
    res_undo_2 = GUS.GenUedoWaitScore(mysql_conn, inst_id, snap_id)
    res_undo_3 = GUS.GenRollbackScore(mysql_conn, inst_id, snap_id)
    #print res_undo_1, res_undo_2, res_undo_3

    res_undo_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'undo_expired')
    res_undo_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'undo_wait')
    res_undo_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'undo_rollback')
    #print res_undo_f1, res_undo_f2, res_undo_f3

    if res_undo_1 == 1 and res_undo_2 == 1 and res_undo_3 == 1 and \
                    res_undo_f1 == 1 and res_undo_f2 == 1 and res_undo_f3 == 1:
        logger.info('Generate Undo Score succss.')
        return 1
    else:
        logger.info('Generate Undo Score failed.')
        return 0

###############################################################################
# 计算 SQL 模块分值
def GenSQLScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate SQL Score starting...')
    res_sql_1 = GSS.GenLongExecScore(mysql_conn, inst_id, snap_id)
    res_sql_2 = GSS.GenSQLCPUScore(mysql_conn, inst_id, snap_id)
    res_sql_3 = GSS.GenSQLIOScore(mysql_conn, inst_id, snap_id)
    #print res_sql_1, res_sql_2, res_sql_3

    if res_sql_1 == -1 and res_sql_2 == -1 and res_sql_3 == -1:
        logger.info('Generate SQL Score succss.')
        return 1

    if res_sql_1 == 1 and res_sql_2 == 1 and res_sql_3 == 1:
        res_sql_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sql_long')
        res_sql_f2 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sql_cpu')
        res_sql_f3 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'sql_io')
        #print res_sql_f1, res_sql_f2, res_sql_f3

        if res_sql_1 == 1 and res_sql_2 == 1 and res_sql_3 == 1 and \
                        res_sql_f1 == 1 and res_sql_f2 == 1 and res_sql_f3 == 1:
            logger.info('Generate SQL Score succss.')
            return 1
        else:
            logger.info('Generate SQL Score failed.')
            return 0

# 计算 RAC 模块分值
def GenRacScore(mysql_conn, inst_id, snap_id):
    logger.info('Generate RAC Score starting...')
    res_rac_1=GRAC.GenRacScore(mysql_conn, inst_id, snap_id)
    res_rac_2=GRAC.GenRacBusy(mysql_conn, inst_id, snap_id)
    if res_rac_1 == -1 and res_rac_2 == -1 :
        logger.info('Generate rac Score succss.')
        return 1
    if res_rac_1 == 1 and res_rac_2 == 1 :
	res_rac_f1 = GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'rac_ksxp')
	res_rac_f2= GFS.GenLevel3forecast(mysql_conn, inst_id, snap_id, 'rac_busy')

	if res_rac_1 == 1 and res_rac_2 == 1  and \
                        res_rac_f1 == 1 and res_rac_f2 == 1 :
            logger.info('Generate rac Score succss.')
            return 1
        else:
            logger.info('Generate rac Score failed.')
            return 0
###############################################################################
# 汇总Level2 层分值
def GenLevel2Score(mysql_conn, inst_id, snap_id):
    logger.info('Generate level 2 score starting...')
    res_level2 = GMHLS.GenLevel2Score(mysql_conn, inst_id, snap_id)
    res_level2_f = GFS.GenLevel2forecast(mysql_conn, inst_id, snap_id)

    if res_level2 == 1 and res_level2_f == 1:
        logger.info('Generate level 2 Score succss.')
        return 1
    else:
        logger.info('Generate level 2 Score failed.')
        return 0

###############################################################################
# 汇总Level1 层分值
def GenLevel1Score(mysql_conn, inst_id, snap_id):
    logger.info('Generate level 1 score starting...')
    res_level1 = GMHLS.GenLevel1Score(mysql_conn, inst_id, snap_id)
    res_level1_f = GFS.GenLevel1forecast(mysql_conn, inst_id, snap_id)

    if res_level1 == 1 and res_level1_f == 1:
        logger.info('Generate level 1 Score succss.')
        return 1
    else:
        logger.info('Generate level 1 Score failed.')
        return 0


###############################################################################
def do(res):
    dbname = res[0]
    inst_id = res[1]
    last_snap_id = res[2]
    ora_conn = res[3]
    object_id = res[5]

    mysql_conn = mysql_handle.ConnectMysql(dbname)
    mysql_maindb = mysql_handle.ConnectMysql('hzmc_data')

    query_str = """select max(snap_id) from aop_snap"""
    max_snap_id = mysql_handle.GetSingleValue(mysql_conn, query_str)
    #print last_snap_id, max_snap_id
    if last_snap_id >= max_snap_id:
        return 1

    for snap_id in range(last_snap_id + 1, max_snap_id + 1):
        result = GenScore(ora_conn, dbname, object_id, inst_id, snap_id)

    exec_str = """update aop_loadinfo set snap_id = %d where dbname = '%s' """%(snap_id, dbname)

    mysql_handle.ExecuteSQL(mysql_maindb, exec_str)

    mysql_handle.CloseMysql(mysql_maindb)
    mysql_handle.CloseMysql(mysql_conn)
    #print dbname

#####################################################################################
##### Main function
#####################################################################################
if __name__ == "__main__":
    pool = multiprocessing.Pool(multiprocessing.cpu_count() / 2)

    mysql_maindb = mysql_handle.ConnectMysql('hzmc_data')
    query_str = """select dbname, inst_id,
                          snap_id, oraconn,
                          mysqlconn, object_id,
                          status
                     from aop_loadinfo"""
    res_list = mysql_handle.GetMultiValue(mysql_maindb, query_str)
    mysql_handle.CloseMysql(mysql_maindb)

    for res in res_list:
        result = pool.apply_async(do, (res,))

    pool.close()
    pool.join()

    #ora_conn = "xxx"
    #dbname = "rac2"
    #target_name = "192.168.80.129"
    #GenScore(ora_conn, dbname, target_name, 1, 418)

    #max_snap_id = common.GetLastSnapId(mysql_conn)
    #res_list = mysql_handle.GetMultiValue(mysql_conn, )
    #ora_conn = "xxx"
    #target_name = "10.134.x.x"
    #mysql_dbname = "hzmc"
    #GenScore(ora_conn, mysql_dbname, target_name, 1, max_snap_id)




