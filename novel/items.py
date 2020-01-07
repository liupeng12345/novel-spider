# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NovelItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    _id = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    lastChapter = scrapy.Field()
    lastChapterUrl = scrapy.Field()
    author = scrapy.Field()
    wordCount = scrapy.Field()
    updateTime = scrapy.Field()
    img = scrapy.Field()
    synopsis = scrapy.Field()
    type = scrapy.Field()
    chaptersUrl = scrapy.Field()
    status = scrapy.Field()
    website = scrapy.Field()


class Chapters(scrapy.Item):
    novelUrl = scrapy.Field()
    chapterList = scrapy.Field()


class Content(scrapy.Item):
    novelUrl = scrapy.Field()
    contentUrl = scrapy.Field()
    contentInfo = scrapy.Field()
