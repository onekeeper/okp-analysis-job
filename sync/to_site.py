#-*-coding:utf-8-*-
import pymysql
import time
import os
import sys
import warnings
import configparser
from datetime import datetime,date
warnings.filterwarnings("ignore")


#logging config
import traceback
import logging
logging.basicConfig(level=logging.WARNING,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='mylog',
                filemode='a+')


if __name__ == "__main__":
    config=configparser.ConfigParser()
    mainpath=os.getcwd()
    sys.path.append(mainpath)
    config.read(mainpath+'/table_sync.ini')
    
#cloud mysql load
    TARG_USER=config.get('target_load','TARG_USER')
    TARG_IP=config.get('target_load','TARG_IP')
    TARG_PORT=config.getint('target_load','TARG_PORT')
    TARG_PASSWD=config.get('target_load','TARG_PASSWD')
    ZABBIX_DB=config.get('target_load','ZABBIX_DB')


    try:
        myconn=pymysql.connect(host=TARG_IP,port=TARG_PORT,user=TARG_USER,password=TARG_PASSWD,db=ZABBIX_DB,charset='utf8')
        mycur=myconn.cursor()
    except: 
        logging.warning(traceback.format_exc())
    sql_str='''select b.area_code, b.longitude, b.latiude, count(c.site_id) site_count,count(a.site_id) warn_count    \
		from zabbix_warning a, stoa b, sites c \
 		where a.site_id=b.site_id and a.site_id=c.site_id \
		and a.time>=date_sub(now(), interval 10 minute) \
 		group by b.area_code, b.longitude, b.latiude '''
    try:
        mycur.execute(sql_str)
        myres=mycur.fetchall()[0]
        mycur.execute("select * from site_map where area_code='%s'"%myres[0])
        the_area=mycur.fetchall()
        if the_area is None:
            mycur.execute("insert into site_map values %s "%str(myres))
            myconn.commit()
        else:
            mycur.execute("update site_map set longitude='%s',latitude='%s',site_count=%d,warn_count=%d where area_code='%s'" \
                           %(myres[1],myres[2],myres[3],myres[4],myres[0]))
            myconn.commit()
    except:
        logging.warning(traceback.format_exc())
    mycur.close()
    myconn.close()
