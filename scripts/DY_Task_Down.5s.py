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

writeLog("%s取样票下发处理模块%s"%('*'*30,'*'*30),20)

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
    #查询待处理取样票
    def QueryQUY(self):
        sql="SELECT a.*,b.CYBM  FROM CYG_DY a LEFT JOIN CYG_CY b ON a.CYG_CY_FK=b.CYG_CY_ID  WHERE a.DQZT IS NULL OR a.DQZT=0;"
        self.log("查询待读取取样票：%s"%sql)
        data=self.xxDbconn.loadSqlDict(sql)
        if data and len(data)>=1:
            self.log("%s接收到%s个待取样瓶"%('*'*30,len(data)),20)
            return data
        else:
            self.log("当前没有待取样的样瓶",20)
            return False

    #查询设备历史数据
    def Query_HISTORY(self,TYPE,RFID,QueryTimeOut):
        try:
            sql=None
            if RFID:
                sql="SELECT TOP 1 * FROM T_HISTORY WHERE F_TYPE IN (%s) AND len(F_RFID)=24 AND F_RFID='%s' AND F_UTC>=%s  ORDER BY F_UTC DESC "%(TYPE,RFID,int(QueryTimeOut))
            self.log("查询历史表数据：%s"%sql)
            data=self.devDbconn.loadSqlDict(sql)
            if data and len(data)>=1:
                self.log("历史信息表找到:%s编码的样瓶"%RFID,20)
                return data[0]
            elif data is 0:
                self.log("历史信息表未找到:%s编码的样瓶"%RFID,30)
                return 0
        except Exception as e:
            self.log("%s查询历史表数据失败%s"%('*'*20,e),40)
            return False
    #查询设备舱位数据
    def Query_BERTHDB(self,RFID):
        try:
            sql=None
            if RFID:
                sql="SELECT TOP 1  * FROM T_BERTHDB WHERE F_STATUS=1  AND len(F_RFID)=24 AND F_RFID='%s' ORDER BY F_UTC DESC"%RFID
            self.log("查询仓位表数据：%s"%sql)
            data=self.devDbconn.loadSqlDict(sql)
            if data and len(data)>=1:
                self.log("仓位信息表找到:%s编码的样瓶"%RFID,20)
                return data[0]
            elif data is 0:
                self.log("仓位信息表未找到:%s编码的样瓶"%RFID,30)
                return 0
        except Exception as e:
            self.log("%s查询仓位表数据失败%s"%('*'*20,e),40)
            return False

    #任务运行主体
    def taskRun(self):
        #查询票
        data=self.QueryQUY()
        #插入语句模板
        '''
        sInsertSql="INSERT INTO main.TaskMain(GUID, RWLX, YPLX, RFID, XRSJ,ZT, FLAG) VALUES (?, ?, ?, ?, ?, ?, ?);"
        if data :
            qData=[]
            for k,row in enumerate(data):
                self.log("读取第%s张操作票,待取样品码：%s"%(k+1,row['YPBM']),20)
                sData=(
                        row['CYG_DY_ID'], #guid
                        "QUYANG",
                        row['YPLX'],
                        row['YPBM'],
                        ToTime.getCurTime()[0],
                        '0',
                        '0'
                        )
                qData.append(sData)

            #写取样子表到中间表,生成取样任务
            iRec=self.icsDbconn.execSqlList(sInsertSql,qData)
            if iRec and iRec==len(qData):
                upSql="UPDATE CYG_DY SET DQSJ=GETDATE(),DQZT='1' WHERE CYG_DY_ID in (%s);"
                uRec=self.xxDbconn.execSqlList(upSql,[(v[0],) for v in qData])
                if uRec and uRec==len(qData):
                    self.log("生成取样任务成功，ID：%s"%[(v[0],) for v in qData],20)
        '''

        if data:
            values=[]
            for k,row in enumerate(data):
                sql="""
                INSERT INTO T_MISSION (F_GID,F_ID,F_ORDER,F_TYPE,F_STATUS,F_YPMM,F_UserOperation,F_DtStarPlan,F_MaxTimeOut,F_DTSTARTOP,F_DTENDOP,F_ZDSBH,F_ErrInfo )
                VALUES                ( '%s','%s',    1,      1,      3,     %s,          570,          %s,           0,         0,         0,       %s,      0    )
                """
                sData=(
                        row['CYG_DY_ID'][:18],
                        row['CYG_DY_ID'][19:],
                        row['CYBM'],
                        ToTime.getCurTime()[1],
                        "3"
                        )
                #values.append(sData)
                sql=sql%sData
                self.log("下发操作票：%s"%sql)
                self.log("下发第%s个取样任务"%(k+1))
                dRec=self.devDbconn.execSql(sql)
                if dRec and len(values)==dRec:
                    upSql="UPDATE CYG_DY SET DQSJ=GETDATE(),DQZT='1' WHERE CYG_DY_ID ='%s';"%row['CYG_DY_ID']
                    uRec=self.xxDbconn.execSql(upSql)
                    if uRec and uRec==len(qData):
                        self.log("下发取样票任务成功，ID：%s"%row['CYG_DY_ID'],20)
                else:
                    self.log("下发取样票任务失败",40)

#主程序
app=main()
del app
