#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# Generate Metric Score Script
# Created on 2017-04-06
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################
import mysql_handle
import db2_handle
import ibm_db

import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('GenMetric')

#################################################################################################
# 获取Tivoli中相关表的最大时间
def Get_Last_SCN(db2_conn, mysql_conn, system_name, tab_name):
    if mysql_conn == None or db2_conn == None:
        return 0

    query_str = """SELECT last_scn from tivoli_data_sync where system_name = '%s' and table_name = '%s' """ \
                %(system_name, tab_name)
    last_scn = mysql_handle.GetSingleValue(mysql_conn, query_str)

    timestamp = ""
    if last_scn:
        timestamp = last_scn
    else:
        query_tivoli_str = """SELECT max(WRITETIME) from "ITMUSER"."%s" where "System_Name" = '%s' """ \
                %(tab_name, system_name)
        logger.debug(query_tivoli_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_tivoli_str)
        result = db2_handle.GetMultiValue(stmt)
        print result
        if(result[0]):
            res = result[0]
            timestamp = res[:-3]

    logger.debug("timestamp: " + timestamp)
    return timestamp


#################################################################################################
# 更新Tivoli中相关表的最大时间
def Update_Last_SCN(mysql_conn, system_name, timestamp, tab_name):
    if mysql_conn == None:
        return 0

    update_str = """update tivoli_data_sync set last_scn = '%s', update_time = NOW() where system_name = '%s' and table_name = '%s' """ \
                %(timestamp, system_name, tab_name)
    #logger.debug("update_str: " + update_str)
    res = mysql_handle.ExecuteSQL(mysql_conn, update_str)
    return res


#################################################################################################
#######     Linux
#################################################################################################
# 获取Tivoli中KLZ_CPU表的信息
def Get_KLZ_CPU(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "KLZ_CPU")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."KLZ_CPU" where CPU_ID = -1 and"System_Name" = '%s' and WRITETIME >= '%s' """ \
                    %(system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while(result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_klz_cpu values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s)""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17])

            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'KLZ_CPU')


#################################################################################################
# 获取Tivoli中KLZ_VM_Stats表的信息
def Get_KLZ_VM_Stats(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "KLZ_VM_Stats")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."KLZ_VM_Stats" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    %(system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while(result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_klz_memory values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s)""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17], res[18], res[19], res[20],
                         res[21], res[22], res[23], res[24], res[25], res[26], res[27], res[28], res[29], res[30],
                         res[31], res[32], res[33], res[34], res[35], res[36])
            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'KLZ_VM_Stats')


#################################################################################################
# 获取Tivoli中KLZ_Network表的信息
def Get_KLZ_Network(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "KLZ_Network")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."KLZ_Network" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    % (system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while (result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_klz_network values('%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, '%s', %s, %s)""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17], res[18], res[19], res[20],
                         res[21], res[22], res[23], res[24], res[25], res[26], res[27], res[28], res[29], res[30],
                         res[31], res[32], res[33], res[34], res[35])
            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'KLZ_Network')


#################################################################################################
# 获取Tivoli中Disk_IO表的信息
def Get_Disk_IO(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "KLZ_Disk_IO")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."KLZ_Disk_IO" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    %(system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while(result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = "insert into tivoli_klz_disk_io values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,'%s')" \
             %(res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10], res[11])


            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'KLZ_Disk_IO')

#################################################################################################
# 获取Tivoli中System_Statistics表的信息
def Get_System_Statistics(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "KLZ_System_Statistics")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."KLZ_System_Statistics" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    % (system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while (result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_klz_system values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s)""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17], res[18], res[19], res[20],
                         res[21], res[22], res[23], res[24])

            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'KLZ_System_Statistics')


#################################################################################################
########  AIX
#################################################################################################
# 获取Tivoli中cpu表的信息
def Get_CPU(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "SMP_CPU")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."SMP_CPU"  where CPU_ID = -1 and "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    %(system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while(result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_aix_cpu values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s)""" \
                      %(res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                        res[11], res[12], res[13], res[14], res[15], res[16],res[17], res[18], res[19], res[20],
                        res[21], res[22], res[23], res[24], res[25], res[26],res[27], res[28], res[29], res[30],
                        res[31], res[32], res[33], res[34], res[35], res[36],res[37], res[38])

            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'SMP_CPU')


#################################################################################################
# 获取Tivoli中Memory表的信息
def Get_Memory(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "Unix_Memory")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."Unix_Memory" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    % (system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)

        while (result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_aix_memory values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s)""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17], res[18], res[19], res[20],
                         res[21], res[22], res[23], res[24], res[25], res[26], res[27], res[28], res[29], res[30],
                         res[31], res[32], res[33], res[34], res[35], res[36], res[37], res[38], res[39], res[40],
                         res[41], res[42], res[43], res[44], res[45], res[46], res[47], res[48], res[49], res[50],
                         res[51], res[52], res[53], res[54], res[55], res[56], res[57], res[58], res[59], res[60],
                         res[61], res[62], res[63], res[64], res[65], res[66], res[67], res[68], res[69], res[70],
                         res[71], res[72], res[73], res[74], res[75])

            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'Unix_Memory')

#################################################################################################
# 获取Tivoli中Network表的信息
def Get_Network(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "Network")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."Network" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    % (system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while (result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_aix_network values('%s', '%s', '%s', '%s', '%s', '%s','%s', %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      '%s', %s, %s, %s, %s, '%s', %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, '%s', '%s', '%s')""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17], res[18], res[19], res[20],
                         res[21], res[22], res[23], res[24], res[25], res[26], res[27], res[28], res[29], res[30],
                         res[31], res[32], res[33], res[34], res[35], res[36], res[37], res[38], res[39], res[40],
                         res[41], res[42], res[43], res[44], res[45], res[46], res[47], res[48], res[49], res[50],
                         res[51], res[52], res[53], res[54], res[55], res[56], res[57], res[58], res[59])

            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'Network')


#################################################################################################
# 获取Tivoli中Disk_Performance表的信息
def Get_Disk_Performance(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "Disk_Performance")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."Disk_Performance" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    %(system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while(result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_aix_disk_perf values('%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, %s,
                      '%s', %s, %s, %s, %s, %s, %s, %s, '%s', '%s',
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, '%s')""" \
                      % (res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                         res[11], res[12], res[13], res[14], res[15], res[16], res[17], res[18], res[19], res[20],
                         res[21], res[22], res[23], res[24], res[25], res[26], res[27], res[28], res[29], res[30],
                         res[31], res[32], res[33], res[34], res[35], res[36], res[37], res[38], res[39], res[40],
                         res[41], res[42], res[43], res[44], res[45], res[46], res[47], res[48], res[49], res[50],
                         res[51], res[52], res[53], res[54], res[55])
            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'Disk_Performance')


#################################################################################################
# 获取Tivoli中System表的信息
def Get_System(db2_conn, mysql_conn, system_name):
    # 获取mysql中记录的最后同步时间；如果是第一次，则取Tivoli数据库里面的最后一次记录的时间
    timestamp = Get_Last_SCN(db2_conn, mysql_conn, system_name, "System")
    if timestamp == "":
        logger.error("Error: Have not get the last timestamp!!! Return")
        return 0

    if db2_conn and mysql_conn:
        query_str = """SELECT * from "ITMUSER"."System" where "System_Name" = '%s' and WRITETIME >= '%s' """ \
                    %(system_name, timestamp)
        logger.debug(query_str)
        stmt = db2_handle.ExecQuery(db2_conn, query_str)
        result = db2_handle.GetMultiValue(stmt)
        while(result):
            res = result.copy()
            for k in result:
                if result[k]:
                    if isinstance(result[k], str):
                        res[k] = result[k].strip()
                    else:
                        res[k] = result[k]
                else:
                    res[k] = 'NULL'

            ins_str = """insert into tivoli_aix_system values('%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s,
                      '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      '%s', %s, %s, %s, '%s', %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, '%s', %s, '%s', '%s', %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s)""" \
                      %(res[1], res[2], res[3], res[4], res[5], res[6], res[7], res[8], res[9], res[10],
                        res[11], res[12], res[13], res[14], res[15], res[16],res[17], res[18], res[19], res[20],
                        res[21], res[22], res[23], res[24], res[25], res[26],res[27], res[28], res[29], res[30],
                        res[31], res[32], res[33], res[34], res[35], res[36],res[37], res[38], res[39], res[40],
                        res[41], res[42], res[43], res[44], res[45], res[46],res[47], res[48], res[49], res[50],
                        res[51], res[52], res[53], res[54], res[55], res[56],res[57], res[58], res[59], res[60],
                        res[61], res[62], res[63], res[64], res[65], res[66],res[67], res[68], res[69], res[70],
                        res[71], res[72], res[73], res[74], res[75], res[76],res[77], res[78], res[79], res[80],
                        res[81], res[82], res[83], res[84], res[85], res[86],res[87], res[88], res[89], res[90],
                        res[91], res[92], res[93], res[94], res[95], res[96],res[97], res[98], res[99], res[100],
                        res[101], res[102], res[103])

            logger.debug(ins_str)
            res = mysql_handle.ExecuteSQL(mysql_conn, ins_str)

            result = db2_handle.GetMultiValue(stmt)

        Update_Last_SCN(mysql_conn, system_name, timestamp, 'System')



###############################################################################
# main function
###############################################################################
if __name__=="__main__":
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('GenMetric')

    db2_conn = db2_handle.ConnectDB2()
    mysql_conn = mysql_handle.ConnectMysql('hzmc_3p_data')

    query_str = """SELECT system_name, table_name, system_type from tivoli_data_sync """
    res = mysql_handle.GetMultiValue(mysql_conn, query_str)
    if res == None:
        exit

    for row in res:
        system_name = row[0]
        table_name = row[1]
        system_type = row[2]

        if system_type == 'AIX':
            if table_name == 'SMP_CPU':
                Get_CPU(db2_conn, mysql_conn, system_name)
            elif table_name == 'Unix_Memory':
                Get_Memory(db2_conn, mysql_conn, system_name)
            elif table_name == 'Network':
                Get_Network(db2_conn, mysql_conn, system_name)
            elif table_name == 'Disk_Performance':
                Get_Disk_Performance(db2_conn, mysql_conn, system_name)
            elif table_name == 'System':
                Get_System(db2_conn, mysql_conn, system_name)
        elif system_type == 'Linux':
            if table_name == 'KLZ_CPU':
                Get_KLZ_CPU(db2_conn, mysql_conn, system_name)
            elif table_name == 'KLZ_VM_Stats':
                Get_KLZ_VM_Stats(db2_conn, mysql_conn, system_name)
            elif table_name == 'KLZ_Network':
                Get_KLZ_Network(db2_conn, mysql_conn, system_name)
            elif table_name == 'KLZ_Disk_IO':
                Get_Disk_IO(db2_conn, mysql_conn, system_name)
            elif table_name == 'KLZ_System_Statistics':
                Get_System_Statistics(db2_conn, mysql_conn, system_name)




