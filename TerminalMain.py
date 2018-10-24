#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-09-25 14:48:57
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.6.5
# 功能     ： Python框架

import os,sys,time,re,logging
import threading,queue,apscheduler,schedule
import csv, configobj,json
#import pyodbc,pymysql,pymssql,cx_Oracle,sqlite3,DBUtils,_mssql
import serial,modbus_tk,OpenOPC
from log import Logger
from ReadConfig import Config
from ToTime import ToTime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

import OracleConnect,MysqlConnect,MssqlConnect,AccessConnect,SqliteConnect

# import scripts
class main(object):
    """docstring for main"""
    def __init__(self):
        super(main, self).__init__()
        #读取配置文件
        self.config=Config("./config/config.ini").read()
        #日志
        self.logger=Logger(self.config['Log'])
        self.log=self.logger.write
        #脚本路径
        self.scriptPath=self.config['Modules']['scripts']
        #自动增加任务扫描周期
        self.scriptScanCycle=int(self.config['Modules']['scriptScanCycle'])
        self.addModulesPath()
        # 调度任务实例后台类型
        self.schedudler=BackgroundScheduler()
        #数据库连接
        self.DBconn=self.GetDBConnect()

        #运行调度任务
        self.runTask()
    #连接数据库
    def GetDBConnect(self):
        DBconn={
                "Oracle":{},
                "Mysql":{},
                "Mssql":{},
                "Access":{},
                "Sqlite":{}
                }

        DBconfig=[v for v in self.config.values() if v.get('type') in('Oracle','Mssql','Access','Mysql','Sqlite') ]
        if DBconfig and len(DBconfig)>=1:
            for v in DBconfig:
                if re.findall('\w+',v.get('type'))[0]=='Oracle' :
                    Oracle=None
                    if v.get('pool'):
                        self.log("创建Oracle数据库连接池")
                        Oracle=OracleConnect.PoolConnect(v,self.log)
                    else:
                        self.log("创建Oracle数据库连接")
                        Oracle=OracleConnect.Connect(v,self.log)
                    DBconn['Oracle'].setdefault(v.get('id'),Oracle)
                elif re.findall('\w+',v.get('type'))[0]=='Mssql' :
                    Mssql=None
                    if v.get('pool'):
                        self.log("创建Mssql数据库连接池")
                        Mssql=MssqlConnect.PoolConnect(v,self.log)
                    else:
                        self.log("创建Mssql数据库连接")
                        Mssql=MssqlConnect.Connect(v,self.log)
                    DBconn['Mssql'].setdefault(v.get('id'),Mssql)
                elif re.findall('\w+',v.get('type'))[0]=='Mysql' :
                    Mysql=None
                    if v.get('pool'):
                        self.log("创建Mysql数据库连接池")
                        Mysql=MysqlConnect.PoolConnect(v,self.log)
                    else:
                        self.log("创建Mysql数据库连接")
                        Mysql=MysqlConnect.Connect(v,self.log)
                    DBconn['Mysql'].setdefault(v.get('id'),Mysql)
                elif re.findall('\w+',v.get('type'))[0]=='Sqlite' :
                    Sqlite=None
                    if v.get('pool'):
                        self.log("创建Sqlite数据库连接池")
                        Sqlite=SqliteConnect.PoolConnect(v,self.log)
                    else:
                        self.log("创建Sqlite数据库连接")
                        Sqlite=SqliteConnect.Connect(v,self.log)
                    DBconn['Sqlite'].setdefault(v.get('id'),Sqlite)
                elif re.findall('\w+',v.get('type'))[0]=='Access' :
                    Access=None
                    if v.get('pool'):
                        self.log("创建Access数据库连接池")
                        Access=AccessConnect.PoolConnect(v,self.log)
                    else:
                        self.log("创建Access数据库连接")
                        Access=AccessConnect.Connect(v,self.log)
                    DBconn['Access'].setdefault(v.get('id'),Access)
        return DBconn
    #脚本使用主程序数据
    def globalObj(self):
        g={}
        g.setdefault('configInfo',dict(self.config))
        g.setdefault('writeLog',self.logger.write)
        g.setdefault('dbConnect',self.DBconn)
        g.setdefault('ToTime',ToTime)

        return g
    #动态增加任务
    def dynamicAddScript(self):
        taskList=[v.id for v in self.schedudler.get_jobs()]
        scriptList=self.getScripts()
        if len(taskList)>=1 and len(scriptList)>=1:
            for task in scriptList:
                taskName=os.path.split(task)[1]
                if taskName not in  taskList:
                    self.log("这是新增加的任务调度：%s"%taskName)
                    cycle=int(re.findall('\w+\.(\d+)',taskName)[0]) or 5
                    trigger=IntervalTrigger(seconds=cycle)
                    self.schedudler.add_job(self.execPyFile,trigger,args=(task,),id=taskName)
    #获取运行脚本 os.path.splitext(v)[0]
    def getScripts(self):
        scriptList=[]
        for file in os.listdir(self.scriptPath):
            if os.path.isfile(os.path.join(self.scriptPath,file)) and re.findall('\w+\.\d+s\.\w+',file):
                file=os.path.abspath(os.path.join(self.scriptPath,file))
                fileOpen=open(file,encoding='utf-8')
                with open(file,encoding='utf-8') as f:
                    if len(f.read())>=1:
                        scriptList.append(file)
        return scriptList
    #增加运行模块脚步进环境变量
    def addModulesPath(self):
        sys.path.append(os.path.abspath(self.config['Modules']['modules']))
        #sys.path.append(self.config['Modules']['scripts'])
    #任务异常
    def taskError(self,event):
        if event.exception:
            self.log("%s任务执行出错：%s"%('*'*20,event.exception),40)
            self.log("%s任务执行出错：退出程序 "%('*'*20),40)
            os.system("TASKKILL /F /IM TerminalMain.exe")
            sys.exit(1)
    #自动增加任务
    def runTask(self):
        scriptList=self.getScripts()
        if scriptList and len(scriptList)>=1:
            for v in scriptList:
                fileName=os.path.split(v)[1]
                cycle=int(re.findall('\w+\.(\d+)',fileName)[0]) or 5
                trigger=IntervalTrigger(seconds=cycle)
                self.schedudler.add_job(self.execPyFile,trigger,args=(v,),id=fileName)
                self.log("增加调度任务：%s"%fileName,20)
            self.schedudler.add_job(self.dynamicAddScript,IntervalTrigger(seconds=self.scriptScanCycle),id='dynamicAddScript')
            #任务异常
            self.schedudler.add_listener(self.taskError,EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            # 任务启动
            self.schedudler.start()
        else:
            self.log("脚本目录：%s,没有脚本任务"%self.scriptPath,30)
    #执行脚本文件
    def execPyFile(self,file):
        print('\n')
        taskName=os.path.split(file)[1]
        self.log("当前执行的任务是：%s"%taskName)
        beginTime=time.clock()
        g=self.globalObj()
        with open(file,encoding= 'utf-8') as fileOpen:
            PyFile=fileOpen.read()
            if len(PyFile)>=1:
                a=exec(PyFile,g)

        self.log("%s 执行耗时：%s秒"%(taskName,time.clock()-beginTime))
if __name__ == '__main__':
    #实例主程序
    app=main()
    #程序标题名称
    os.system("title %s"%app.config['Modules']['terminalName'])
    while True:
        print("\n\n")
        app.log("%s当前正在运行的调度任务数量：%s"%('*'*30,len(app.schedudler.get_jobs())),20)
        app.log("%s当前正在运行的调度任务：%s"%('*'*30,[v.id for v in app.schedudler.get_jobs()]),20)
        if len(app.schedudler.get_jobs())<=0:
            app.log("%s没有调度的任务\n\n"%("-"*30),30)
        time.sleep(app.scriptScanCycle*10)
