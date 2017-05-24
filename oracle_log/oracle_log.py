#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
This is a  gather information program of oracle
Written by Zhangqz from hzmc
Mail address : zhangqz@mchz.com.cn

"""


import cx_Oracle
import ConfigParser
import multiprocessing
import sys
from multiprocessing import Queue
import random
import traceback
import os
import string
import re
import time
import pymysql
import fcntl
from datetime import  datetime
import warnings
warnings.filterwarnings("ignore")

#logging config
import logging
logging.basicConfig(level=logging.WARNING,
                format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='mylog',
                filemode='a+')





class item:
    def __init__(self,pid,date,sleep_time):
        self.pid=pid
        self.date=date
	self.sleep_time=sleep_time
class getlog(multiprocessing.Process,item):
    def __init__(self,db,jn,sleep_time,num,queue,lock,mainpath):
        multiprocessing.Process.__init__(self)
	self.db=db
        self.jn=jn
        self.sleep_time=sleep_time
        self.num=num
        self.queue=queue
        self.lock=lock
        self.mainpath=mainpath
    def run(self):
        import time
        comms=[]
        times=[]
        types=[]
        names=[]
        jobs=[]
        formats=[]
	primary_keys=[]
        diff_nums=[]
	match_nums=[]
        config=ConfigParser.ConfigParser()
        config.read(os.path.join(mainpath, 'exam.ini'))
        config.sections()
	custom_db=self.db
	orauser=custom_db[3]
	orapass=custom_db[4]
	oraip=custom_db[0]
	orasid=custom_db[2]
	mysqldb=custom_db[5]
        for name in config.sections():
            try:
                job=config.get(name,"job")
                if job==self.jn:
                    times.append((config.getint(name,"time"))/self.sleep_time)
                    comms.append(config.get(name,"comm"))
                    types.append(config.get (name,"type"))
                    names.append(config.get (name,"table"))
                    jobs.append(job)
                    formats.append(config.get(name,"format"))
		    primary_keys.append(config.get(name,"primary_key"))
                    diff_nums.append(config.get(name,"diff_num"))
		    match_nums.append(config.get(name,"match_num"))
            except:
                pass
	t=0
	while t<=self.num-1:
	    t1=time.time()
	    for r,tt in enumerate(times):
		p=int(t%tt)
	        if p<1:
	            comm=comms[r:r+1][0]
		    type=types[r:r+1][0]
		    name=names[r:r+1][0]
		    job=jobs[r:r+1][0]
		    primary_key=primary_keys[r:r+1][0]
                    format=formats[r:r+1][0]
                    diff_num=diff_nums[r:r+1][0]
		    match_num=match_nums[r:r+1][0]
		    if type=='OS':
			os.system(comm)
                    elif type=='SNAP':
                        myconn=pymysql.connect(host=MAIN_IP,port=MAIN_PORT,user=MAIN_USER,password=MAIN_PASSWD,db=mysqldb)
                        mycur=myconn.cursor()
                        try:
                            mycur.execute("create table %s (%s, primary key(%s))engine=myisam"%(name,format,primary_key))
                            myconn.commit()
                        except pymysql.err.InternalError:
                            pass
                        try:
                            conn=cx_Oracle.connect(orauser,orapass,oraip+'/'+orasid)
                            cur=conn.cursor()
                            x=cur.execute(comm)
                            y=x.fetchall()
                        except:
			    logging.warning(traceback.format_exc())
                        if mycur.execute("select * from %s"%name)==0:
			    try:
                                mycur.execute("insert into %s (end_time) values ('%s') "%(name,str(y[0][0])))
                                myconn.commit()
			    except:
                                logging.warning(traceback.format_exc())
                        else:
			    try:
                                mycur.execute("select date_format(max(end_time),'%Y-%m-%d %H:%i:%s') from aop_snap")
                                myres=mycur.fetchone()
                                myres=list(myres)
                                myres.insert(1,y[0][0])
                                myres=tuple(myres)
                                mycur.execute("insert into %s (start_time,end_time) values %s "%(name,str(myres)))
                                myconn.commit()
			    except:
				logging.warning(traceback.format_exc())
		    elif type=='DB':
			diff_num=diff_num.replace(' ', '')
                        arr=diff_num.split(',')
			match_num=match_num.replace(' ', '')
			match_arr=match_num.split(',')
			myconn=pymysql.connect(host=MAIN_IP,port=MAIN_PORT,user=MAIN_USER,password=MAIN_PASSWD,db=mysqldb)
                        mycur=myconn.cursor()
			try:
			    mycur.execute("create table %s (snap_id int,start_time datetime, %s,primary key(snap_id,%s))engine=myisam"%(name,format,primary_key))
                            myconn.commit()
                        except pymysql.err.InternalError:
                            pass
			try:
			    conn=cx_Oracle.connect(orauser,orapass,oraip+'/'+orasid)
        		    cur=conn.cursor()
		            x=cur.execute(comm)
			    y=x.fetchall()
			    cur.close()
			    conn.close()
			except:
			    logging.warning(traceback.format_exc())
			snapname=name.split('_')[0]
			mycur.execute("select max(snap_id) from %s_snap"%snapname)
			myid=mycur.fetchone()[0]
			if arr[0]=='0':
			    	for data in y:
				    data=list(data)
				    data.insert(0,str(data[0]))
                                    data.insert(0,myid)
                                    data=tuple(data)
				    try:
				        mycur.execute("insert into %s values %s"%(name,str(data)))
				        myconn.commit()
				    except:
					#logging.warning(name+'  '+traceback.format_exc())
					pass
			else:
				try:
                            	    mycur.execute("create table %s_tmp (%s,primary key(%s))engine=myisam"%(name,format,primary_key))
                            	    myconn.commit()
                        	except pymysql.err.InternalError:
                            	    pass
				if mycur.execute("select * from %s_tmp"%name)==0:
                            	    for data in y:
					try:
                                	    mycur.execute("insert into %s_tmp values %s"%(name,str(data)))
                                	    myconn.commit()
					except:
					    logging.warning(name+traceback.format_exc())
				else:
                            	    res=mycur.execute("select * from %s_tmp"%(name))
                            	    myres=mycur.fetchall()
				    for data in y:
				        data=list(data)
                                        for mydata in myres:
                                            mydata=list(mydata)
					    for match_n in match_arr:
						if str(data[int(match_n)-1]) == str(mydata[int(match_n)-1]):
						    match_res='true'
						else:
						    match_res='no'
					    if len(data)==len(mydata) and match_res is 'true':
				#	        startt=time.mktime(time.strptime(str(mydata[0]),'%Y-%m-%d %H:%M:%S'))
                                        #        endt=time.mktime(time.strptime(data[0],'%Y-%m-%d %H:%M:%S'))
                                         #       mytime=endt-startt
				    	        for d in arr:
					  #          data[int(d)-1]=float(abs(data[int(d)-1]-float(str(mydata[int(d)-1]))))/mytime
						    data[int(d)-1]=float(abs(data[int(d)-1]-float(str(mydata[int(d)-1]))))
				    	        data.insert(0,str(mydata[0]))
				    	        data.insert(0,myid)
				    	        data=tuple(data)
						try:
				    	            mycur.execute("insert into %s values %s"%(name,str(data)))
						except:
						    logging.warning(name+traceback.format_exc())
				mycur.execute("truncate table %s_tmp"%name)
				for data in y:
				    try:
                                        mycur.execute("insert into %s_tmp values %s"%(name,str(data)))
				        myconn.commit()
				    except:
					logging.warning(name+' '+mysqldb+' '+traceback.format_exc())
			myconn.commit()
	    t2=time.time()
            instance=item(job,round((time.time()-t2+t1),1),self.sleep_time)
            self.lock.acquire()
            self.queue.put(instance,timeout=self.sleep_time-t2+t1)
            self.lock.release()
	    try:
	        time.sleep(self.sleep_time+t2-t1)
	    except:
		logging.warning(traceback.format_exc())
	    t+=1
def p_monitor(queue,job_num):
    dict={}
    i=1
    while len(dict)<=int(job_num):
        ins=queue.get()
        dict[ins.pid]=ins.date
        if len(dict)==int(job_num):
	    break
    while True:
	if queue.empty() is False:
	    instance=queue.get()
	    pid=instance.pid
	    sleep_time=instance.sleep_time
	    date=instance.date
	    if round((date-dict[pid])/sleep_time,1)>1.5 and (date-dict[pid])/sleep_time<2:
	        logging.warning('%s,OVERTIME:%d\n'%(pid,date-dict[pid]-sleep_time))
	    elif round((date-dict[pid])/sleep_time,1)>=2.0:
	        logging.warning('%s,ERRO,OVERTIME:%d\n'%(pid,date-dict[pid]-sleep_time))
	    elif round((date-dict[pid]),1)==0.0:
	        pass
	    elif round((date-dict[pid])/sleep_time,1)==1.0:
		pass
	    dict[pid]=date
	else:
	    pass
	time.sleep(600)
def justone():
    load_times=1
    try:
	fcntl.lockf(load_times,fcntl.LOCK_EX | fcntl.LOCK_NB)
	return load_times
    except IOError:
	print "\nanother instance is running..."
	sys.exit(0)
if __name__ == "__main__":
    mul=[]
    queue= multiprocessing.Queue()
    lock=multiprocessing.Lock()
    config=ConfigParser.ConfigParser()
    mainpath=os.path.dirname(__file__)
    sys.path.append(mainpath)
    config.read(os.path.join(mainpath, 'exam.ini'))
    mtime=config.getint('Main','time')
    alter_log_file=mainpath+config.get('Main','alter_log_file')
    jobs=config.items('job_time')
    job_num=config.get('Main','job_num')
    justone()
    ckpt=multiprocessing.Process(name='ckpt',target=p_monitor,args=(queue,job_num))
#mysql load

    MAIN_IP=config.get('mysql_load','MAIN_IP')
    MAIN_PORT=config.getint('mysql_load','MAIN_PORT')
    MAIN_USER=config.get('mysql_load','MAIN_USER')
    MAIN_PASSWD=config.get('mysql_load','MAIN_PASSWD')
    MAIN_DB=config.get('mysql_load','MAIN_DB')
    myconn=pymysql.connect(host=MAIN_IP,port=MAIN_PORT,user=MAIN_USER,password=MAIN_PASSWD,db=MAIN_DB)
    mycur=myconn.cursor()
    mycur.execute('select object_id, object_name,instance_name,username,password,sys_id from aop_object_score')
    dbs=mycur.fetchall()
    for db in dbs:
	try:
	    mycur.execute('create database %s'%db[5])
	    myconn.commit()
	except:
	    pass
#	if mycur.execute("select * from information_schema.tables where table_schema='%s' and  table_name like 'score_%%'"%db[5]) == 0:
#	    try:
#		mycur.execute("use test")
#	        mycur.execute("source /hzmc/mysql/TableDesign.sql")
#		mycur.execute("source /hzmc/mysql/InitScoreRule.sql")
#		mycur.execute("source /hzmc/mysql/InitTable.sql")
#	        myconn.commit()
 #           except:
#                logging.warning(traceback.format_exc())
#	else:
#	    pass
        for info in jobs:
	    jn=info[0]
	    sleep_time=int(info[1])
	    num=mtime/sleep_time
	    mylog=getlog(db,jn,sleep_time,num,queue,lock,mainpath)
	    mul.append(mylog)
    for m in mul:
	m.daemon = True
	m.start()
    ckpt.daemon = True
    ckpt.start()
    for i in range(mtime/10):
	if ckpt.is_alive() is True:
	    pass
	else:
	    ckpt.terminate()
	    ckpt=multiprocessing.Process(name='ckpt',target=p_monitor,args=(queue,job_num))
	    ckpt.daemon=True
	    ckpt.start()
	    logging.warning('CKPT IS DOWN!')
	time.sleep(10)
    mycur.close()
    myconn.close()
