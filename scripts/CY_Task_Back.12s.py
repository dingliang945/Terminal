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

writeLog("%s存样票任务处理模块%s"%('*'*30,'*'*30),20)

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
    #查询任务
    def QueryCYTask(self):
        sql="SELECT * FROM TaskMain WHERE FLAG='0' AND RWLX='CUNYANG';"
        self.log("查询待执行存样票：%s"%sql)
        data=self.icsDbconn.loadSqlDict(sql)
        if data and len(data)>=1:
            self.log("%s待执行%s个样瓶"%('*'*30,len(data)),20)
            return data
        else:
            self.log("当前没有待执行存样样瓶记录",20)
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
        #查询任务
        data=self.QueryCYTask()
        if data:
            for k,row in enumerate(data):

                #查询瓶子超时为任务下发的前N天以后的数据,防止数据时间误差
                QueryTimeOut=ToTime.strToUtc(row['XRSJ'])-float(self.config['Device']['cunYTimeOut'])*24*3600
                #任务超时
                TaskTimeOut=ToTime.strToUtc(row['XRSJ'])+float(self.config['Device']['cunYTimeOut'])*24*3600
                YCYBZ=None #已存样标记
                YPBM=None
                RFID=row['RFID']
                self.log("处理第%s个样瓶，编码：%s"%(k+1,RFID),20)
                #查询仓位表
                RtData= self.Query_BERTHDB(RFID)
                if RtData and RtData is not 0:
                    YPBM=RtData['F_BARCODE']
                    YCYBZ=True
                # 查询历史表
                HiData=self.Query_HISTORY('11,21',RFID,QueryTimeOut)
                if HiData and HiData is not 0:
                    YPBM=HiData['F_BARCODE']
                    YCYBZ=True
                #查询到数据
                if YCYBZ:
                    upIcsSql="UPDATE TaskMain SET YPBM='%s',GXSJ='%s',FLAG='2' WHERE ZYBM='%s' AND RFID='%s' AND RWLX='CUNYANG' AND FLAG='0'"%(YPBM or "",ToTime.getCurTime()[0],row["ZYBM"],RFID)
                    #更新完成任务
                    iRec=self.icsDbconn.execSql(upIcsSql)
                    if iRec and iRec>=1:
                        self.log("完成样瓶编码:%s的任务"%RFID,20)
                else:
                    if ToTime.getCurTime()[1]>=TaskTimeOut:
                        upIcsSql="UPDATE TaskMain SET GXSJ='%s',FLAG='3' WHERE ZYBM='%s' AND RFID='%s' AND RWLX='CUNYANG' AND FLAG='0'"%(ToTime.getCurTime()[0],row["ZYBM"],RFID)
                        #更新完成任务
                        iRec=self.icsDbconn.execSql(upIcsSql)
                        if iRec and iRec>=1:
                            self.log("超过任务有效果期,结束样瓶编码:%s的任务,超时时限是:%s"%(RFID,ToTime.utcToDate(TaskTimeOut)),30)
                    else:
                        self.log("样瓶编码:%s 样品可能还未存到存样柜,或异常等待超时:%s"%(RFID,ToTime.utcToDate(TaskTimeOut)),20)


#主程序
app=main()
del app

