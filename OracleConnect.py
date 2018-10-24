#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-06-09 16:08:16
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：

import cx_Oracle,re
from DBUtils.PooledDB import PooledDB
#连接池
class PoolConnect():
    """docstring for Connect"""
    #connect=None

    def __init__(self, arg,log):
        super(PoolConnect, self).__init__()
        self.arg = arg
        self.log=log
        self.pool=self.connect()
    def connect(self):
        if self.arg['host']:
            dsn = self.arg['host'] + ":" + str(self.arg['port']) + "/" + self.arg['database']
            try:
                pool=PooledDB(
                            creator=cx_Oracle,        #使用链接数据库的模块
                            user= self.arg['user'],
                            password= self.arg['password'],
                            dsn=dsn,
                            maxconnections = int(self.arg['maxconnections']),#连接池允许的最大连接数，0和None表示没有限制
                            mincached = int(self.arg['mincached']),          #初始化时，连接池至少创建的空闲的连接，0表示不创建
                            maxcached = int(self.arg['maxcached']),          #连接池空闲的最多连接数，0和None表示没有限制
                            maxshared = int(self.arg['maxshared']),          #连接池中最多共享的连接数量，0和None表示全部共享
                            blocking = True                             #链接池中如果没有可用共享连接后，是否阻塞等待，True表示等待，False表示不等待然后报错
                            #login_timeout=1
                            )
                self.log('Oracle连接池初始成功',20)
                return pool
            except Exception as e:
                self.log("Oracle连接池初始失败，错误信息：%s"%e,40)
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
                self.log("查询数据0条")
                return False
        except Exception as e:
            self.log("查询数据:%s 错误：%s"%(sql,e),30)
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
                #self.log("查询数据0条2")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e),30)
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
                self.log("数据执行成功，影响%d行 "%rec,20)
                cursor.close()
                connect.close()
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec,20)
                return False
        except Exception as e:
            self.log("执行数据失败：%s,%s "%(sql,e),30)
#短连接
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
            conn = cx_Oracle.connect("%s/%s@%s:%s/%s"%(self.arg['user'],self.arg['password'],self.arg['host'],self.arg['port'],self.arg['database']))
            self.log("Oracle连接成功")
            return conn
        except Exception as e:
            self.log("连接数据库失败",40)
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
                #self.log("查询数据0条")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s"%(sql,e),30)
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
                #self.log("查询数据0条2")
                return 0
        except Exception as e:
            self.log("查询数据:%s 错误：%s "%(sql,e),30)
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
                self.log("数据执行成功，影响%d行 "%rec,20)
                cursor.close()
                connect.close()
                return rec
            else:
                self.log("数据执行成功，影响%d行"%rec,20)
                return False
        except Exception as e:
            self.log("执行数据失败：%s,%s "%(sql,e),30)


if __name__=="__main__":
    import ReadConfig,log
    config=ReadConfig.Config('./config/config.ini')
    coninfo=config.read()
    logg=log.Logger(coninfo['Log'])
    connect=PoolConnect(coninfo['Oracle'],logg.write)
    logg.write(connect.loadSqlList("select sysdate from dual"))
    conn=Connect(coninfo['Oracle'],logg.write)

    logg.write(conn.loadSqlDict("SELECT * FROM  T_RL_CZPMAIN WHERE CPHBH=0"))

