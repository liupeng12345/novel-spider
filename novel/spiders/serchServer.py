# coding=utf-8
import configparser
import copy
import logging
import time
from socket import *
from urllib.request import quote

import os
import threading

# 或者
# cmdline.execute("scrapy crawl 名称".split())
# 读取配置文件
config = configparser.ConfigParser()
file = '../config/serverconfig.ini'
config.read(file)

# 获取端口、ip 和 线程池大小
serviceIp = config.get("service", "ip")
servicePort = int(config.get("service", "port"))
poolLength = int(config.get("service", "port"))
crawl_sign = False


# 执行爬虫
def execute(key):
    t_key = copy.deepcopy(key)
    if t_key == '':
        return
    if t_key.startswith("key"):
        t_key = t_key[3:]
        os.system("scrapy crawl search  -a key=%s" % quote(t_key, encoding="gbk"))
    elif t_key.startswith("chapter"):
        t_key = t_key[len("chapter"):]
        os.system("scrapy crawl chapter -a chapter=%s" % quote(t_key, encoding="gbk"))
    else:
        t_key = t_key[len("content"):]
        os.system("scrapy crawl content -a content=%s" % quote(t_key, encoding="gbk"))


# 返回 代理 ip 和 端口
def send(sock, addr):
    print('Accept new connection from %s:%s...' % addr)
    data = sock.recv(1024)
    execute(data.decode())
    sock.close()
    print('Connection from %s:%s closed' % addr)


# 开启服务
def start():
    tcpSocket = socket(AF_INET, SOCK_STREAM)
    # 重复使用绑定信息,不必等待2MSL时间
    tcpSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    address = (serviceIp, servicePort)
    tcpSocket.bind(address)
    tcpSocket.listen(5)
    try:
        while True:
            time.sleep(0.01)
            connection, addr = tcpSocket.accept()
            threading.Thread(target=send, args=(connection, addr,)).start()
            send(connection, addr)
    except Exception as e:
        print(e)
    finally:
        tcpSocket.close()


def main():
    # 开启tcp 搜索服务
    logging.info("准备开启tcp服务")
    start()
    logging.info("开启tcp服务完毕")


# tcpSocket.close()
if __name__ == '__main__':
    start()
