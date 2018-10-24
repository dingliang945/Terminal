#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-06-09 16:08:16
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：

import sqlite3,re
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
                    #print(self.maxConn.qsize(),conn,id(conn),'00000000000000000000')
                self.log("初始化连接数：%s"%self.maxConn.qsize())
        except Exception as e:
            self.log("初始化连接数失败：%s"%e)
    #回收连接
    def closeConn(self,conn):
        #print(self.maxConn.qsize(),conn,id(conn),int(self.arg['maxconnections']),self.maxConn.qsize()<int(self.arg['maxconnections']),11111111111111111)
        if self.maxConn.qsize()<int(self.arg['maxconnections']):
            self.maxConn.put(conn)
        elif self.maxConn.qsize()==0:
             self.createConn()
        else:
            del conn
    #创建连接
    def create(self):
        return sqlite3.connect(self.arg['database'],check_same_thread=False)
    def loadSqlDict(self,sql):
        try:
            connect=self.maxConn.get()
            #print(self.maxConn.qsize(),connect,id(connect),22222222222222)
            # for i in range(self.maxConn.qsize()):
            #     a=self.maxConn.get()
            #     print(121441515111111,a,id(a))
            cursor=connect.cursor()
            data=[]
            cursor.execute(sql)
            value=cursor.fetchall()
            if value and len(value)>=1:
                field=tuple([d[0] for d in cursor.description])
                for v in value:
                    data.append(dict(zip(field,v)))
                return data
            else:
                self.log("查询数据0条")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False
        finally:
            cursor.close()
            self.closeConn(connect)
    def loadSqlList(self,sql):
        try:
            connect=self.maxConn.get()
            #print(self.maxConn.qsize(),connect,id(connect),33333333333333333)
            cursor=connect.cursor()
            data=[]
            cursor.execute(sql)
            value=cursor.fetchall()
            if value and len(value)>=1:
                data=value

                return data
            else:
                self.log("查询数据0条")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False
        finally:
            cursor.close()
            self.closeConn(connect)
    def execSql(self,sql):
        try:
            connect=self.maxConn.get()
            #print(self.maxConn.qsize(),connect,id(connect),4444444444444444444)
            cursor=connect.cursor()
            cursor.execute(sql)
            rec=cursor.rowcount
            connect.commit()
            #self.log("执行语句：%s 2"%sql)
            if rec>=1 :
                self.log("数据执行成功，影响%d行 "%rec)
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec)
                return False
        except Exception as e:
            self.log("sql:%s,执行数据失败：%s "%(sql,e))
        finally:
            cursor.close()
            self.closeConn(connect)
    def execSqlList(self,sql,values):
        try:
            connect=self.maxConn.get()
            #print(self.maxConn.qsize(),connect,id(connect),555555555555555555)
            cursor=connect.cursor()
            cursor.executemany(sql,values)
            rec=cursor.rowcount
            connect.commit()
            #self.log("执行语句：%s 2"%sql)
            if rec>=1 :
                self.log("数据执行成功，影响%d行 "%rec)
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec)
                return False
        except Exception as e:
            self.log("执行数据失败：%s "%e)
        finally:
            cursor.close()
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
        try:
            conn=sqlite3.connect(self.arg['database'])
            # self.log("sqlite3连接成功")
            return conn
        except Exception as e:
            self.log("sqlite3连接失败，错误信息：%s"%e)
            return False

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
            cursor.executemany(sql,tuple(values))
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
    connect=PoolConnect(coninfo['Sqlite'],logg.write)

    conn=Connect(coninfo['Sqlite'],logg.write)
    logg.write(conn.loadSqlDict("SELECT CURRENT_DATE"))
    while 1:
        logg.write(connect.loadSqlDict(" SELECT CURRENT_DATE"))
        time.sleep(5)
    #a=PoolConnect(coninfo['Sqlite'],logg.write)

