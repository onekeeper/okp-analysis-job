#-*-coding:utf-8-*-
import pymysql
import time
import os
import sys
import urllib
import urllib.request
import urllib.parse
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
    fs=open('/docker/okp-docker/okp_env/var/lib/alertlogs/alerts.log','a+')
    fs.seek(0,2)
    while True:
        getline=fs.readline()
        getline=''.join(getline).strip('\n')
        one_data=getline.split('||')
        if one_data != ['']:
            one_data[1]=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(one_data[1])))
            one_data=one_data[1:]
            one_data=tuple(one_data)
            try:
                info=str(one_data[2])
                one_url="http://120.27.237.163/service_message.php?alert_token=tG7niucgEiMxkwt8&site_id=TEST_001&content="
                info=urllib.parse.quote(info)
                url=one_url+info
#                req = urllib.request(url)

 #               res_data = urllib.urlopen(req)
                res_data = urllib.request.urlopen(url)
                res = res_data.read().decode('utf-8')
            except :
                logging.warning(traceback.format_exc())
            try:
                print("insert into zabbix_warning values %s"%str(one_data))
                mycur.execute("insert into zabbix_warning values %s"%str(one_data))
                myconn.commit()
            except :
                logging.warning(traceback.format_exc())
        time.sleep(1)
    mycur.close()
    myconn.close()
    fs.close()
