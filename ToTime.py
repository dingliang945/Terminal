#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 时间     : 2018-09-03 11:13:58
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：

import time,re
class ToTime(object):
    """输入任意格式时间，年月日时分秒转成utc时间戳，如2018 11 12 00:21:50 或2018-11-12 00:21:59
        年月日时分秒中间必须用任意符号空格等非数字分割
    """
    def __init__(self):
        super(ToTime, self).__init__()
    def strToUtc(string=None):
        try:

            if string:
                t=re.findall('\d+',string)
                t='%04d-%02d-%02d %02d:%02d:%02d'%tuple([int(v) for v in t])
                return int(time.mktime(time.strptime(t, '%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            print("正确认格式为：年-月-日 时：分：秒，日期格式不正确认:%s"%e)
            return False
    def utcToDate(string=None):
        try:
            if string and len(str(string))>=10:
                t=str(string)
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(t[:10])))
        except Exception as e:
            print("正确认格式为：1970-1-1到现在的秒数，如：1535980488，日期格式不正确认:%s"%e)
            return False
    def getCurTime():
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),int(time.time())
    def formatStrDate(string):
        try:

            if string:
                t=re.findall('\d+',string)
                t='%04d-%02d-%02d %02d:%02d:%02d'%tuple([int(v) for v in t])
                return t
        except Exception as e:
            print("正确认格式为：年-月-日 时：分：秒，日期格式不正确认:%s"%e)
            return False

if __name__ == '__main__':


    print(ToTime.utcToDate(1535945151))
    print(ToTime.getCurTime())
    print(ToTime.strToUtc('2018-09-03 10:29:36'))
    print(ToTime.formatStrDate('2017:7/9:1:25,33'))

#print time.mktime(time.strptime(timestring, '%Y-%m-%d %H:%M:%S'))