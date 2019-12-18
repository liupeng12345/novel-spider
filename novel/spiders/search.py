# 搜索爬虫

from urllib.parse import quote
import scrapy
import json
from novel.spiders.config import *
from novel.spiders.utils import *
from novel.items import NovelItem
import redis

pool = redis.ConnectionPool(host='dev', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
config_list = get_novel_spider_config_list()


class NovelSearchSpider(scrapy.Spider):
    name = "search"

    def __init__(self, key=None, *args, **kwargs):
        super(NovelSearchSpider, self).__init__(*args, **kwargs)
        urlList = []
        for dd in config_list:
            urlList.append(dd["searchUrl"] % quote(key, encoding=dd["code"]))
        self.start_urls = urlList
        self.key = key

    def parse(self, response):
        # 搜索结果
        website = get_web_site(response.url)
        dd = get_config(website, config_list)
        info = response.xpath(dd["searchInfo"])
        for node in info:
            search_novel = NovelItem()
            search_novel["name"] = filter_null(node.xpath(dd["novelName"]).extract())
            search_novel["url"] = filter_null(node.xpath(dd["novelUrl"]).extract())
            search_novel["url"] = add_head(dd["websiteUrl"], search_novel["url"])
            search_novel["website"] = dd["websiteName"]
            search_novel["chaptersUrl"] = search_novel["url"]
            search_novel["author"] = filter_null(node.xpath(dd["novelAuthor"]).extract())
            search_novel["lastChapter"] = filter_null(node.xpath(dd["novelLatestChapter"]).extract())
            search_novel["lastChapterUrl"] = delete_char(dd["websiteUrl"], search_novel["url"],
                                                         filter_null(node.xpath(dd["novelLatestChapterUrl"]).extract()))
            search_novel["wordCount"] = filter_null(node.xpath(dd["wordContent"]).extract())
            search_novel["updateTime"] = filter_null(node.xpath(dd["novelLastUpdateTime"]).extract())
            search_novel["synopsis"] = filter_null(node.xpath(dd["novelDescription"]).extract())
            search_novel["type"] = filter_null(node.xpath(dd["novelType"]).extract())
            search_novel["img"] = filter_null(node.xpath(dd["novelImgUrl"]).extract())
            search_novel["img"] = add_head(dd["websiteUrl"], search_novel["img"])
            search_novel["status"] = filter_null(node.xpath(dd["novelStatus"]).extract())
            yield scrapy.Request(search_novel['url'], callback=self.parse_info, meta={'novel': search_novel, 'dd': dd})

    def parse_info(self, response):
        search_novel = response.meta["novel"]
        dd = response.meta["dd"]
        search_novel["author"] = filter_null2(response.xpath(dd["novelAuthor"]).extract(), search_novel["author"])
        search_novel["lastChapter"] = filter_null2(response.xpath(dd["novelLatestChapter"]).extract(),
                                                   search_novel["lastChapter"])
        search_novel["lastChapterUrl"] = delete_char(dd["websiteUrl"], search_novel["url"], filter_null2(
            response.xpath(dd["novelLatestChapterUrl"]).extract(),
            search_novel["lastChapterUrl"]))
        search_novel["wordCount"] = filter_null2(response.xpath(dd["wordContent"]).extract(), search_novel["wordCount"])
        search_novel["updateTime"] = filter_null2(response.xpath(dd["novelLastUpdateTime"]).extract(),
                                                  search_novel["updateTime"])
        search_novel["synopsis"] = filter_null2(response.xpath(dd["novelDescription"]).xpath('string(.)').extract(),
                                                search_novel["synopsis"])
        search_novel["type"] = filter_null2(response.xpath(dd["novelType"]).extract(), search_novel["type"])
        search_novel["img"] = filter_null2(response.xpath(dd["novelImgUrl"]).extract(),
                                           search_novel["img"])
        search_novel["img"] = add_head(dd["websiteUrl"], search_novel["img"])
        search_novel["chaptersUrl"] = filter_null2(response.xpath(dd["novelChaptersUrl"]).extract(),
                                                   search_novel["chaptersUrl"])
        search_novel["status"] = filter_null2(response.xpath(dd["novelStatus"]).extract(),
                                              search_novel["status"])
        print(search_novel)
        search_novel_str = json.dumps(dict(search_novel), ensure_ascii=False)
        r.sadd("novel:" + self.key, search_novel_str)
        yield search_novel
