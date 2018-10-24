
# -*- coding: utf-8 -*-
# 时间     : 2018-06-09 15:59:19
# 作者     : DL (dingliang@ygct.com)
# 网址     : http://www.ygct.com
# 软件版本 : Python3.5.4
# 功能     ：
import re,os
from configobj import ConfigObj

class Config(object):
    '''
        读取info配置文件,默认读取当前目录下config.info,
        可选参数 file,非默认名配置文件路径
    '''

    def __init__(self, file=False):
        super(Config,self).__init__()
        self.file=file
        #self.log=Logger( consolELevel=10)
        #print(lambda value:print(value))
    def read(self,encoding='utf-8'):
        self.info={}
        #存储配置文件
        if not self.file:
            self.file='./config.ini'
        try:
            file =ConfigObj(os.path.abspath(self.file),encoding=encoding)
            for key in file.keys():
                for k in file[key].keys():

                    if file[key][k].lower()=='true':
                        file[key][k]=True
                    elif file[key][k].lower()=='false':
                        file[key][k]=False
                    # elif file[key][k].isdecimal():
                    #     file[key][k]=int(file[key][k])
                    # elif not re.findall('\d+\.\d+\.\d+\.\d+',file[key][k]) and re.findall('\d+\.\d+',file[key][k]):
                    #     file[key][k]=float(file[key][k])
                    # elif re.findall('\d+x[a-f0-9]+',file[key][k],re.I):
                    #     file[key][k]=hex(int(file[key][k],16))
            print(" * 打开配置文件:%s,成功 "%(os.path.abspath(self.file)))
            # self.config.emit(file)
            return file
        except Exception as e:
            print("打开配置文件:%s,失败%s "%(os.path.abspath(self.file),e))
    def write(self,itemName,keyName,value,encoding='utf-8'):
        '''
        write(itemName,keyName,value)
        '''
        try:
            if not self.file:
                    self.file='./config.ini'
            config = ConfigObj(os.path.abspath(self.file),encoding=encoding)
            config[itemName][keyName] = value
            config.write()
            print("修改配置%s: %s=%s 成功 "%(itemName,keyName,value))
            #self.config.emit(config)
            return config
        except Exception as e:
            print("修改配置%s: %s=%s 失败 ,错误：%s "%(itemName,keyName,value,e))
    def addItem(self,itemName,keyName,value,encoding='utf-8'):
        try:
            if not self.file:
                self.file='./config.ini'
            config = ConfigObj(os.path.abspath(self.file),encoding=encoding)
            config[itemName] = {}
            config[itemName][keyName] = value
            config.write()
            print("增加配置%s: %s=%s 成功 "%(itemName,keyName,value))
            #self.config.emit(config)
            return config
        except Exception as e:
            print("增加配置%s: %s=%s 失败  ,错误：%s "%(itemName,keyName,value,e))
    def delItem(self,itemName,keyName=None,encoding='utf-8'):
        '''
            delItem(self,itemName,keyName=None)
        '''
        try:
            if not self.file:
                self.file='./config.ini'
            config = ConfigObj(os.path.abspath(self.file),encoding=encoding)
            if config[itemName] and config[itemName][keyName]:
                del config[itemName][keyName]
                print("删除配置：%s[%s] 成功 "%(itemName,keyName))
                #self.config.emit(config)
                config.write()
                return config
            else :
                del config[itemName]
                print("删除配置：%s 成功 "%(itemName))
                self.config.emit(config)
                config.write()
                return config
        except Exception as e:
            try:
                config[itemName]
                print("删除配置文件 %s: %s 失败  ,错误：%s "%(itemName,keyName,e))
            except Exception as e:
                print("删除配置文件 %s 不存在 错误：%s "%(itemName,e))
    def asSave(self,dictTable,filePath=None,encoding='utf-8'):
        if not filePath:
                    filePath='./config.ini'

        config=ConfigObj(filePath,encoding=encoding)
        for key in dictTable.keys():
            config[key]=dictTable[key]
        config.write()
        print("另存配置文件成功 ")

if __name__=="__main__":
    #filename=r'.\gcl_sdk\config\apps.config.ini'
    app=Config()
    print(app.read())
    # app.write('ShareMemory','YcaddCount',20480)
    # # a={'xxx':{'aa':111,'bb':True}}
    # app.delItem('xxx','dd')
