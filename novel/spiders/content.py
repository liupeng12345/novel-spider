# 搜索爬虫
import scrapy
import json
import redis
import urllib.parse

from novel.items import Content
from novel.spiders.utils import *
from novel.spiders.config import get_novel_spider_config_list

pool = redis.ConnectionPool(host='cloud-redis', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
config_list = get_novel_spider_config_list()


class NovelContentSpider(scrapy.Spider):
    name = "content"

    def __init__(self, content=None, *args, **kwargs):
        super(NovelContentSpider, self).__init__(*args, **kwargs)
        print(content)
        self.start_urls = \
            [urllib.parse.unquote(content)]

    def parse(self, response):
        print(response.url)
        if response.url.endswith("html"):
            website = get_web_site(response.url)
            dd = get_config(website, config_list)
            content = {"content": strengthen(
                "".join(response.xpath(dd["contentInfo"] + "/text()" + "|" + dd["contentInfo"] + "/*").extract())),
                "nextPage": handleContentUr(response.url, response.xpath(dd["contentNextPage"]).extract()[0]),
                "prePage": handleContentUr(response.url, response.xpath(dd["contentPreciousPage"]).extract()[0])}
            contentStr = json.dumps(content, ensure_ascii=False)
            r.set(response.url.replace("/", ":"), contentStr)
            reContent = Content()
            reContent["novelUrl"] = get_chapter_url(response.url)
            reContent["contentUrl"] = response.url
            reContent["contentInfo"] = contentStr
            yield reContent
            if response.meta.__contains__('next'):
                nextNumber = response.meta["next"] - 1
            else:
                nextNumber = 3
            print(content["nextPage"])
            if nextNumber > 0 and content["nextPage"] is not None:
                yield scrapy.Request(content["nextPage"], callback=self.parse, meta={"next": nextNumber})
