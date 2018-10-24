#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-06-09 16:08:16
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：

import pyodbc,os,re
from queue import Queue
class PoolConnect(object):
    """docstring for Connect"""
    #connect=None


    def __init__(self, arg,log):
        super(PoolConnect, self).__init__()
        self.arg = arg
        self.log=log
        self.maxConn=self.getMaxConn()
        self.createConn()
    #初始队列
    def getMaxConn(self):
        if not int(self.arg['maxconnections']):
            return Queue()
        else:
            return Queue(int(self.arg['maxconnections']))
    #初始创建连接
    def createConn(self):
        try:
            if int(self.arg['mincached'])>=1:
                for i in range(int(self.arg['mincached'])):
                    conn=self.create()
                    self.maxConn.put(conn)
                self.log("初始化连接数：%s"%self.maxConn.qsize())
        except Exception as e:
            self.log("初始化连接数失败：%s"%e)
    #回收连接
    def closeConn(self,conn):
        if self.maxConn.qsize()<int(self.arg['maxconnections']):
            self.maxConn.put(conn)
        elif self.maxConn.qsize()==0:
             self.createConn()
        else:
            del conn
    #创建连接
    def create(self):
        dbPath=self.arg['database']
        exp=re.findall('\.(\w+)',os.path.split(dbPath)[1])[0]
        userinfo='Dbq=%s;Pwd=%s;'%(dbPath,self.arg['password'])
        if exp.lower()=='accdb':
            if 'Microsoft Access Driver (*.mdb, *.accdb)'in [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]:
                return pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};'+userinfo)
            else:
                self.setupAccessDriver()
        else:
            return pyodbc.connect('Driver={Microsoft Access Driver (*.mdb)};'+userinfo)
    def setupAccessDriver(self):
        pass

    def loadSqlDict(self,sql):
        try:
            connect=self.maxConn.get()
            cursor=connect.cursor()
            data=[]
            cursor.execute(sql)
            value=cursor.fetchall()
            if value and len(value)>=1:
                field=tuple([d[0] for d in cursor.description])
                for v in value:
                    data.append(dict(zip(field,v)))
                cursor.close()
                return data
            else:
                #self.log("查询数据0条2")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False
        finally:
            self.closeConn(connect)
    def loadSqlList(self,sql):
        try:
            connect=self.maxConn.get()
            cursor=connect.cursor()
            data=[]
            cursor.execute(sql)
            value=cursor.fetchall()
            if value and len(value)>=1:
                data=value
                cursor.close()
                return data
            else:
                self.log("查询数据0条")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False
        finally:
            self.closeConn(connect)
    def execSql(self,sql):
        try:
            connect=self.maxConn.get()
            cursor=connect.cursor()
            cursor.execute(sql)
            rec=cursor.rowcount
            connect.commit()
            #self.log("执行语句：%s 2"%sql)
            if rec>=1 :
                self.log("数据执行成功，影响%d行 "%rec)
                cursor.close()
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec)
                return False
        except Exception as e:
            self.log("sql:%s,执行数据失败：%s "%(sql,e))
        finally:
            self.closeConn(connect)
    def execSqlList(self,sql,values):
        try:
            connect=self.maxConn.get()

            cursor=connect.cursor()
            cursor.executemany(sql,values)
            rec=cursor.rowcount
            connect.commit()
            #self.log("执行语句：%s 2"%sql)
            if rec>=1 :
                self.log("数据执行成功，影响%d行 "%rec)
                cursor.close()
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec)
                return False
        except Exception as e:
            self.log("执行数据失败：%s "%e)
        finally:
            self.closeConn(connect)

class Connect(object):
    """docstring for Connect"""
    def __init__(self, arg,log):
        super(Connect, self).__init__()
        self.arg = arg
        self.log=log
        self.conn=self.connect()
        self.conn.close()
    def connect(self):
        dbPath=self.arg['database']
        exp=re.findall('\.(\w+)',os.path.split(dbPath)[1])[0]
        userinfo='Dbq=%s;Pwd=%s;'%(dbPath,self.arg['password'])
        if exp.lower()=='accdb':
            if 'Microsoft Access Driver (*.mdb, *.accdb)'in [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')]:
                return pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};'+userinfo)
            else:
                # self.setupAccessDriver()
                pass
        else:
            return pyodbc.connect('Driver={Microsoft Access Driver (*.mdb)};'+userinfo)

    def loadSqlDict(self,sql):
        try:
            connect=self.connect()
            cursor=connect.cursor()
            data=[]
            cursor.execute(sql)
            value=cursor.fetchall()
            if value and len(value)>=1:
                field=tuple([d[0] for d in cursor.description])
                for v in value:
                    data.append(dict(zip(field,v)))
                cursor.close()
                connect.close()
                return data
            else:
                #self.log("查询数据0条2")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False

    def loadSqlList(self,sql):
        try:
            connect=self.connect()
            cursor=connect.cursor()
            data=[]
            cursor.execute(sql)
            value=cursor.fetchall()
            if value and len(value)>=1:
                data=value
                cursor.close()
                connect.close()
                return data
            else:
                self.log("查询数据0条")
                return False
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False
    def execSql(self,sql):
        try:
            connect=self.connect()
            cursor=connect.cursor()
            cursor.execute(sql)
            rec=cursor.rowcount
            connect.commit()
            #self.log("执行语句：%s 2"%sql)
            if rec>=1 :
                self.log("数据执行成功，影响%d行 "%rec)
                cursor.close()
                connect.close()
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec)
                return False
        except Exception as e:
            self.log("sql:%s,执行数据失败：%s "%(sql,e))
    def execSqlList(self,sql,values):
        try:
            connect=self.connect()
            cursor=connect.cursor()
            cursor.executemany(sql,values)
            rec=cursor.rowcount
            connect.commit()
            #self.log("执行语句：%s 2"%sql)
            if rec>=1 :
                self.log("数据执行成功，影响%d行 "%rec)
                cursor.close()
                connect.close()
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec)
                return False
        except Exception as e:
            self.log("执行数据失败：%s "%e)




if __name__=="__main__":
    import ReadConfig,log,time
    config=ReadConfig.Config('./config/config.ini')
    coninfo=config.read()
    logg=log.Logger(coninfo['Log'])
    connect=PoolConnect(coninfo['Access'],logg.write)

    conn=Connect(coninfo['Access'],logg.write)
    logg.write(conn.loadSqlDict("SELECT now"))
    while 1:
        logg.write(connect.loadSqlDict(" SELECT now"))
        time.sleep(5)


