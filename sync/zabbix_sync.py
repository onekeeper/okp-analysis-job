#-*-coding:utf-8-*-
import pymysql
import time
import os
import sys
import uniout
import urllib
import urllib2
import warnings
import ConfigParser
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
    config=ConfigParser.ConfigParser()
    mainpath=os.path.dirname(__file__)
    sys.path.append(mainpath)
    config.read(os.path.join(mainpath, 'table_sync.ini'))

#cloud mysql load
    TARG_USER=config.get('target_load','TARG_USER')
    TARG_IP=config.get('target_load','TARG_IP')
    TARG_PORT=config.getint('target_load','TARG_PORT')
    TARG_PASSWD=config.get('target_load','TARG_PASSWD')
    ZABBIX_DB=config.get('target_load','ZABBIX_DB')
# wechat info
    ALERT_IP=config.get('wechat_info','ALERT_IP')
    ALERT_TOKEN=config.get('wechat_info','ALERT_TOKEN')


    try:
        myconn=pymysql.connect(host=TARG_IP,port=TARG_PORT,user=TARG_USER,password=TARG_PASSWD,db=ZABBIX_DB,charset='utf8')
        mycur=myconn.cursor()
    except:
        logging.warning(traceback.format_exc())
    fs=open('alertlogs/alerts.log','a+')
    fs.seek(0,2)
    while True:
        getline=fs.readline()
        getline=''.join(getline).strip('\n')
        one_data=getline.split('||')
        if one_data != ['']:
            one_data[1]=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(one_data[1])))
            one_data=one_data[1:]
            try:
                info=str(one_data[2])
                site_id=str(one_data[1])
                one_url="http://%s/service_message.php?alert_token=%s&site_id=%s&content=" % (ALERT_IP,ALERT_TOKEN,site_id)
                info=urllib.quote(info)
                url=one_url+info
                req = urllib2.Request(url)
                res_data = urllib2.urlopen(req)
                res = res_data.read()

                sql="insert into zabbix_warning values ('%s','%s','%s','%s','%s')"%(one_data[0],one_data[1],one_data[2],one_data[3],one_data[4])
                mycur.execute(sql)
                myconn.commit()
            except pymysql.Error:
                try:
                    myconn=pymysql.connect(host=TARG_IP,port=TARG_PORT,user=TARG_USER,password=TARG_PASSWD,db=ZABBIX_DB,charset='utf8')
                    mycur=myconn.cursor()
                    sql="insert into zabbix_warning values ('%s','%s','%s','%s','%s')"%(one_data[0],one_data[1],one_data[2],one_data[3],one_data[4])
                    mycur.execute(sql)
                    myconn.commit()
                except:
                    pass
            except:
                logging.warning(traceback.format_exc())
        time.sleep(1)
    mycur.close()
    myconn.close()
    fs.close()
