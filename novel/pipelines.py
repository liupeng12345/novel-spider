# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from twisted.internet import reactor, defer

from novel.items import Chapters, NovelItem, Content


class NovelPipeline(object):
    """
        异步插入MongoDB
        """

    def __init__(self, mongo_uri, mongo_db, mongo_col):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_col = mongo_col

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_col=crawler.settings.get('MONGO_NOVEL_COL')
        )

    def open_spider(self, spider):
        """
        爬虫启动时，启动
        :param spider:
        :return:
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.mongodb = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        爬虫关闭时执行
        :param spider:
        :return:
        """
        # todo 爬虫结束
        self.client.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        if isinstance(item, NovelItem):
            out = defer.Deferred()
            reactor.callInThread(self._insert, item, out, spider)
            yield out
            defer.returnValue(item)
        return item

    def _insert(self, item, out, spider):
        """
        插入函数
        :param item:
        :param out:
        :return:
        """
        myquery = {"_id": item["_id"]}
        find = self.mongodb[self.mongo_col].find_one(myquery)
        print(find)
        if find is None:
            self.mongodb[self.mongo_col].insert(dict(item))
        else:
            self.mongodb[self.mongo_col].update(myquery, dict(item))
        reactor.callFromThread(out.callback, item)


class ChaptersPipeline(object):
    """
    异步插入MongoDB
    """

    def __init__(self, mongo_uri, mongo_db, mongo_col):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_col = mongo_col

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_col=crawler.settings.get('MONGO_CHAPTER_COL')
        )

    def open_spider(self, spider):
        """
        爬虫启动时，启动
        :param spider:
        :return:
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.mongodb = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        爬虫关闭时执行
        :param spider:
        :return:
        """
        # todo 爬虫结束
        self.client.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        if isinstance(item, Chapters):
            out = defer.Deferred()
            reactor.callInThread(self._insert, item, out, spider)
            yield out
            defer.returnValue(item)
        return item

    def _insert(self, item, out, spider):
        """
        插入函数
        :param item:
        :param out:
        :return:
        """

        self.mongodb[self.mongo_col].insert(dict(item))
        reactor.callFromThread(out.callback, item)


class ContentPipeline(object):
    """
    异步插入MongoDB
    """

    def __init__(self, mongo_uri, mongo_db, mongo_col):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_col = mongo_col

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_col=crawler.settings.get('MONGO_CONTENT_COL')
        )

    def open_spider(self, spider):
        """
        爬虫启动时，启动
        :param spider:
        :return:
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.mongodb = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        爬虫关闭时执行
        :param spider:
        :return:
        """
        # todo 爬虫结束
        self.client.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        if isinstance(item, Content):
            out = defer.Deferred()
            reactor.callInThread(self._insert, item, out, spider)
            yield out
            defer.returnValue(item)
        return item

    def _insert(self, item, out, spider):
        """
        插入函数
        :param item:
        :param out:
        :return:
        """
        self.mongodb[self.mongo_col].insert(dict(item))
        reactor.callFromThread(out.callback, item)
