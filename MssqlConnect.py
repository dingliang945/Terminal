#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-06-09 16:08:16
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：

import pymssql,re
from DBUtils.PooledDB import PooledDB
class PoolConnect(object):
    """docstring for Connect"""
    #connect=None


    def __init__(self, arg,log):
        super(PoolConnect, self).__init__()
        self.arg = arg
        self.log=log
        self.pool=self.connect()
    def connect(self):
        if self.arg and self.arg['host']:
            try:
                pool=PooledDB(
                            creator=pymssql,        #使用链接数据库的模块
                            host= self.arg['host'],
                            user= self.arg['user'],
                            port=int(self.arg['port']),
                            password= self.arg['password'],
                            database = self.arg['database'],
                            maxconnections = int(self.arg['maxconnections']),#连接池允许的最大连接数，0和None表示没有限制
                            mincached = int(self.arg['mincached']),          #初始化时，连接池至少创建的空闲的连接，0表示不创建
                            maxcached = int(self.arg['maxcached']),          #连接池空闲的最多连接数，0和None表示没有限制
                            maxshared = int(self.arg['maxshared']),          #连接池中最多共享的连接数量，0和None表示全部共享
                            blocking = True,                                #链接池中如果没有可用共享连接后，是否阻塞等待，True表示等待，False表示不等待然后报错
                            charset='GBK',
                            login_timeout=1
                            )
                self.log('Mssql连接池初始成功')
                return pool
            except Exception as e:
                self.log("Mssql连接池初始失败，错误信息：%s"%e)
                return False
    def loadSqlDict(self,sql):
        try:
            connect=self.pool.connection()
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
            connect=self.pool.connection()
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
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e))
            return False
    def execSql(self,sql):
        try:
            connect=self.pool.connection()
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
            connect=self.pool.connection()
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

class Connect(object):
    """docstring for Connect"""
    def __init__(self, arg,log):
        super(Connect, self).__init__()
        self.arg = arg
        self.log=log
        self.conn=self.connect()
        self.conn.close()
    def connect(self):
        if self.arg and self.arg['host']:
            try:
                conn=pymssql.connect(self.arg['host'],self.arg['user'],self.arg['password'],self.arg['database'],port=int(self.arg['port']),charset='utf8')
                self.log("Mssql连接成功")
                return conn
            except Exception as e:
                self.log("Mssql连接失败，错误信息：%s"%e)
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
    connect=PoolConnect(coninfo['Mssql'],logg.write)
    logg.write(connect.loadSqlList("select GETDATE()"))
    conn=Connect(coninfo['Mssql'],logg.write)
    logg.write(conn.loadSqlDict("select GETDATE()"))
    while 1:
        logg.write(connect.loadSqlList("select GETDATE()"))
        time.sleep(5)


