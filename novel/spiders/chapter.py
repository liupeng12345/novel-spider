# 搜索爬虫
import scrapy
import json
import redis
import urllib.parse
from novel.spiders.utils import *
from novel.spiders.config import get_novel_spider_config_list
from novel.items import Chapters

pool = redis.ConnectionPool(host='cloud-redis', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
config_list = get_novel_spider_config_list()


class NovelChapterSpider(scrapy.Spider):
    name = "chapter"

    def __init__(self, chapter=None, *args, **kwargs):
        super(NovelChapterSpider, self).__init__(*args, **kwargs)
        self.start_urls = \
            [urllib.parse.unquote(chapter)]

    def parse(self, response):
        website = get_web_site(response.url)
        dd = get_config(website, config_list)
        info = response.xpath(dd["chapterListInfo"])
        chapters = []
        for i in info:
            chapter = {"name": i.xpath(dd["chapterName"])[0].extract(),
                       "url": i.xpath(dd["chapterUrl"])[0].extract()}
            if not chapter["url"].startswith("http"):
                chapter["url"] = delete_char(dd["websiteUrl"], response.url, chapter["url"])
            chapters.append(chapter)
        chaptersStr = json.dumps(chapters)
        r.set(response.url.replace("/", ":"), chaptersStr)
        nChapter = Chapters()
        nChapter["novelUrl"] = response.url
        nChapter["chapterList"] = json.dumps(chapters, ensure_ascii=False)
        yield nChapter
