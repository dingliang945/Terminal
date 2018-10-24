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
        self.xxDbconn=self.dbCon['Mssql']['1']
        self.icsDbconn=self.dbCon['Sqlite']['2']
        self.devDbconn=self.dbCon['Access']['3']
        self.taskRun()
    #查询待处理存样票
    def QueryCUNY(self):
        sql="SELECT * FROM CYG_CY WHERE DQZT IS NULL OR DQZT=0;"
        self.log("查询待读取存样票：%s"%sql)
        data=self.xxDbconn.loadSqlDict(sql)
        if data and len(data)>=1:
            self.log("%s接收到%s张待存样操作票"%('*'*30,len(data)),20)
            return data
        else:
            self.log("当前没有待存样的操作票",20)
            return False
    #任务运行主体
    def taskRun(self):
        #查询票
        data=self.QueryCUNY()
        if data :
            for k,row in enumerate(data):
                self.log("读取第%s张操作票制样码：%s"%(k+1,row['ZYBM']),20)
                #根据票关联制样票查询到具体RFID
                subSql="SELECT b.ZYBM,b.CYBM,a.* FROM ZYJ_JL a LEFT JOIN ZYJ_JH b ON a.ZYJ_JH_FK=b.ZYJ_JH_ID WHERE b.ZYBM='%s';"%row['ZYBM']
                self.log("读取待存样瓶信息：%s"%subSql)
                subData=self.xxDbconn.loadSqlDict(subSql)
                if subData and len(subData)>=1:
                    self.log("读取到%s个待存样瓶子"%len(subData),20)
                    ypxx=[]
                    #插入语句模板
                    sInsertSql="INSERT INTO main.TaskMain(GUID, ZYBM, CYBM, RWLX, YPLX, RFID, XRSJ, FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
                    for i,sRow in enumerate(subData):
                        self.log("读取第%s个待存样瓶"%(i+1))
                        sData=(
                                str(uuid.uuid1(None,i+1)), #guid
                                sRow['ZYBM'],
                                sRow['CYBM'],
                                "CUNYANG",
                                sRow['YPLX'],
                                sRow['XPID'],
                                ToTime.getCurTime()[0],
                                '0'
                                )
                        ypxx.append(sData)

                    #写存样子表到中间表,生成存样任务
                    iRec=self.icsDbconn.execSqlList(sInsertSql,ypxx)
                    if iRec and iRec==len(ypxx):
                        upSql="UPDATE CYG_CY SET DQSJ=GETDATE(),DQZT='1' WHERE CYG_CY_ID='%s';"%row['CYG_CY_ID']
                        #更新标记
                        uRec=self.xxDbconn.execSql(upSql)
                        if uRec and uRec>=1:
                            self.log("生成存样任务成功，制样码：%s"%row['ZYBM'],20)
                else:
                    self.log("异常存样票，制样票结果未找到待存样的样瓶信息",30)
#主程序
app=main()
del app
