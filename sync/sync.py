import pymysql
import os
import sys
import time
import warnings
import ConfigParser
import multiprocessing
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



def run_sync(source_db,target_db,tables,MAIN_USER,MAIN_IP,MAIN_PORT,MAIN_PASSWD,TARG_USER,TARG_IP,TARG_PORT,TARG_PASSWD):
    try:
        sur_conn=pymysql.connect(host=MAIN_IP,port=MAIN_PORT,user=MAIN_USER,password=MAIN_PASSWD,db=source_db)
        scur=sur_conn.cursor()
        tar_conn=pymysql.connect(host=TARG_IP,port=TARG_PORT,user=TARG_USER,password=TARG_PASSWD,db=target_db)

        tcur=tar_conn.cursor()
    except:
	logging.warning(source_db+'  '+target_db+'   '+traceback.format_exc())
    for table_name in tables:
        tcur.execute("select count(*) from %s"%(table_name))
        count=tcur.fetchone()[0]
        if count ==0 :
	    tar_id=0
        else:
	    try:
	        tcur.execute("select max(snap_id) from %s"%table_name)
	        tar_id=tcur.fetchone()[0]
	    except:
	        logging.warning(table_name+'  '+source_db+'  '+target_db+'   '+traceback.format_exc())
        scur.execute("select max(snap_id) from %s"%table_name)
        sur_id=scur.fetchone()[0]+1
        try:
	    scur.execute("select * from %s where snap_id>=%d and snap_id <%d"%(table_name,tar_id,sur_id))
	    myres=scur.fetchall()
	    for data in myres:
	        data=list(data)
	        data[2]=str(data[2])
	        data[3]=str(data[3])
	        data=tuple(data)
	        tcur.execute("insert into %s values %s"%(table_name,str(data)))
	    tar_conn.commit()
        except:
            logging.warning(table_name+'  '+source_db+'  '+target_db+'   '+traceback.format_exc())
    tcur.close()
    scur.close()
    tar_conn.close()
    sur_conn.close()
if __name__ == "__main__":
#local mysql load
    config=ConfigParser.ConfigParser()
    mainpath=os.getcwd()
    sys.path.append(mainpath)
    config.read(mainpath+'/table_sync.ini')

    MAIN_USER=config.get('local_load','MAIN_USER')
    MAIN_IP=config.get('local_load','MAIN_IP')
    MAIN_PORT=config.getint('local_load','MAIN_PORT')
    MAIN_PASSWD=config.get('local_load','MAIN_PASSWD')

#cloud mysql load
    TARG_USER=config.get('target_load','TARG_USER')
    TARG_IP=config.get('target_load','TARG_IP')
    TARG_PORT=config.getint('target_load','TARG_PORT')
    TARG_PASSWD=config.get('target_load','TARG_PASSWD')

#get sync list
    sync_list=config.items('sync_list')
    table_list=config.get('table_list','table_name')
    table_list=table_list.replace(' ', '')
    tables=table_list.split(',')
    res=[]
    pool = multiprocessing.Pool(multiprocessing.cpu_count()/2)
    for info in sync_list:
	source_db=info[0]
	target_db=info[1]
	res.append(pool.apply_async(run_sync,(source_db,target_db,tables,MAIN_USER,MAIN_IP,MAIN_PORT,MAIN_PASSWD,TARG_USER,TARG_IP,TARG_PORT,TARG_PASSWD,)))
    for k in res:
        k.get()
    pool.close()
