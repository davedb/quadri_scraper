# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class QuadriItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id_el = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    price = scrapy.Field()
    rooms = scrapy.Field()
    bathrooms = scrapy.Field()
    surface = scrapy.Field()
    desc = scrapy.Field()
    date_published = scrapy.Field()
    date_scraped = scrapy.Field()
    other_info = scrapy.Field()
    agency = scrapy.Field()
    rif = scrapy.Field()

class SubitoSingleItemList(scrapy.Item):
    # define the fields for your item here like:
    id_el = scrapy.Field() # id annuncio
    title = scrapy.Field()
    link = scrapy.Field()
    date_scraped = scrapy.Field()
    price = scrapy.Field()
    date_published = scrapy.Field()
    location = scrapy.Field()
    surface = scrapy.Field()
    rooms = scrapy.Field()
    agency = scrapy.Field()
    zone_id = scrapy.Field()
    zone_label = scrapy.Field()


class SubitoCategoryLabelsList(scrapy.Item):
    # define the fields for your item here like:
    category = scrapy.Field()
    labels = scrapy.Field()
