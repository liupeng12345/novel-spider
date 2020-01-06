import subprocess
import threading
import time

import pika


class MQBase(object):
    """ 消息队列基类 """

    def __init__(self, host, port, exchange, virtual_host, exchange_type, ack=True, persist=True):
        self.conn = None  # 连接
        self.channel = None  # 信道

        self.host = host  # 主机
        self.port = port  # 端口
        self.exchange = exchange  # 交换机名称
        self.exchange_type = exchange_type  # 交换机类型
        self.ack = ack  # 是否开启消息手动确认机制, 默认是自动确认机制
        self.persist = persist  # 消息是否持久化
        self.virtual_host = virtual_host
        self.param = pika.ConnectionParameters(host=self.host, port=self.port,
                                               virtual_host=self.virtual_host)  # 转换为连接参数

    def _open_channel(self):
        """ 建立连接并开辟信道 """

        self.conn = pika.BlockingConnection(self.param)  # 建立连接
        self.channel = self.conn.channel()  # 在连接中开辟信道

        self.channel.exchange_declare(exchange=self.exchange,  # 交换机名称
                                      exchange_type=self.exchange_type,  # 交换机类型
                                      durable=self.persist)  # 交换机是否持久化; 该方法用于声明交换机, 声明多次仅会创建一次
        if self.ack:
            self.channel.confirm_delivery()  # 在该信道中开启消息手动确认机制

    def _close_channel(self):
        """ 关闭信道并断开连接 """

        if self.channel and self.channel.is_open:  # 检测信道是否还存活
            self.channel.close()  # 关闭信道

        if self.conn and self.conn.is_open:  # 检测连接是否还存活
            self.conn.close()  # 断开连接


class MQSender(MQBase):
    """ 消息队列-生产者 """

    def send(self, route, msg):
        self._open_channel()

        properties = pika.BasicProperties(delivery_mode=(2 if self.persist else 0))  # delivery_mode为2时表示消息持久化, 其他值时非持久化
        self.channel.confirm_delivery()  # 开启消息送达确认(注意这里是送达消息队列即可)
        ret = self.channel.basic_publish(exchange=self.exchange,  # 指定发送到的交换机
                                         routing_key=route,  # 消息中的路由键
                                         body=msg,  # 消息中的有效载荷
                                         properties=properties)  # 该方法用于发送消息, 消息成功送达消息队列时返回True, 否则返回False
        self._close_channel()
        return ret


class MQReceiver1(MQBase):
    """ 消息队列-消费者1 """

    def _declare_queue(self, queue_name):
        """ 声明队列 """

        self.channel.queue_declare(queue=queue_name,  # 队列名称
                                   durable=self.persist)  # 队列是都否持久化; 该方法用于声明队列, 声明多次仅会创建一次

        self.channel.queue_bind(queue=queue_name,  # 队列
                                exchange=self.exchange,  # 交换机
                                routing_key=queue_name)  # 绑定键, 该方法用于将队列通过绑定键绑定到交换机, 之后交换机会将对应路由键的消息转发到该队列上

    def _subscribe_queue(self, queue_name):
        """ 订阅队列 """

        self._declare_queue(queue_name)
        self.channel.basic_qos(prefetch_count=1)  # 该方法起到负载均衡的作用, 表明一次只接受prefetch_count条信息, 直到消息确认后再接收新的
        self.channel.basic_consume(consumer_callback=self.handler,  # 收到消息后的回调方法, 即消息处理器
                                   queue=queue_name,  # 订阅的队列名称
                                   no_ack=not self.ack)  # 是否不使用手动消息确认机制; 该方法用于订阅队列, 并分配消息处理器

    def start(self, queue, func):
        """ 开始消费, 以回调方式 """

        self.func = func  # 真正的消息处理器

        self._open_channel()
        self._subscribe_queue(queue)
        self.channel.start_consuming()  # 该方法用于开始消费消息队列中的消息, 会阻塞住的. 该方法虽然是由channel调用的, 一个连接下有多个channel, 但一个连接只能调用一次该方法

    def end(self):
        """ 结束消费 """

        self.channel.stop_consuming()  # 该方法用于停止消费
        self._close_channel()

    def handler(self, channel, method_frame, header_frame, body):
        """ 收到消息后的回调方法, 即消息处理器 """

        # from pprint import pprint
        # pprint(method_frame), pprint(header_frame)  # 可以通过pprint打印出对象的内容

        self.func(body)  # 调用真正的消息处理器

        if self.ack:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)  # 手动确认消息已被成功处理


class MQReceiver2(MQBase):
    """ 消息队列-消费者2 """

    def start(self, queue, func):
        """ 开始消费, 以迭代方式 """

        self._open_channel()
        for method_frame, properties, msg in self.channel.consume(queue=queue,  # 队列名称
                                                                  no_ack=not self.ack):  # 是否不使用手动消息确认机制; 该方法用于消费消息队列中的消息
            func(msg)  # 调用消息的处理器
            if self.ack:
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)  # 手动确认消息已被成功处理

    def end(self):
        """ 结束消费 """

        self.channel.cancel()


def producer_handler():
    for msg in range(18):
        print('producer send %s' % msg)

        sender = MQSender(host='localhost',
                          port=5672,
                          exchange='test_exchange',
                          exchange_type='direct',
                          ack=True,
                          persist=True)

        sender.send('test_queue', str(msg))


def consumer_handler(num):
    def func(t_key):
        """ 真正的消息处理器 """
        print('consumer_%s handling message: %s, sleep: %s' % (num, t_key, t_key))
        t_key = t_key.decode("utf-8")
        if t_key == '':
            return
        if t_key.startswith("key"):
            t_key = t_key[3:]
            subprocess.Popen("scrapy crawl search  -a key=%s" % t_key, shell=True)
        elif t_key.startswith("chapter"):
            t_key = t_key[len("chapter"):]
            subprocess.Popen("scrapy crawl chapter -a chapter=%s" % t_key, shell=True)
        else:
            t_key = t_key[len("content"):]
            subprocess.Popen("scrapy crawl content -a content=%s" % t_key, shell=True)

    receiver = MQReceiver2(host='localhost',
                           port=5672,
                           virtual_host="/novel",
                           exchange='novel.spider.direct',
                           exchange_type='direct',
                           ack=True,
                           persist=True)
    receiver.start('novel.spider.cancel', func)


def main():
    for i in range(8):
        threading.Thread(target=consumer_handler, args=(i + 1,)).start()

    # Thread(target=producer_handler).start()  # 生产者


if __name__ == '__main__':
    main()
