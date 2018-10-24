#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-10-15 08:41:10
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：调度模板

import time,uuid

# #配置
# configInfo config文件夹congfig.ini 配置文件内容
# #日志
# writeLog('MSG',level=10) 日志等级：10:debug,20:info,30:warning,40:error,50:critical
# 数据库
# dbConnect 字典，Oracle,mysql,mssql,sqlite,access 多数据用id,如
# dbConnect['Mssql']['配置文件ID']
#注意：不要使用 if __name__ == '__main__' : XXX，否则不会执行

writeLog("%s存样票下发处理模块%s"%('*'*30,'*'*30),20)

class main(object):
    """docstring for main"""
    def __init__(self, writeLog=writeLog,configInfo=configInfo,dbConnect=dbConnect):
        super(main, self).__init__()
        self.log=writeLog
        self.config=configInfo
        self.dbCon=dbConnect
        #数据库连接
        self.xxDbconn=self.dbCon['Mssql']['1']
        self.icsDbconn=self.dbCon['Sqlite']['2']
        self.devDbconn=self.dbCon['Access']['3']
        # 运行任务
        self.taskRun()
    #主任务
    def taskRun(self):
        self.log("测试任务")
#主程序
app=main()
del app
