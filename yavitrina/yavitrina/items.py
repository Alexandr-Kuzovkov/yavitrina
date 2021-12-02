# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CategoryItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    img = scrapy.Field()
    parent = scrapy.Field()
    html = scrapy.Field()
    description = scrapy.Field()

class CategoryDescriptionItem(scrapy.Item):
    url = scrapy.Field()
    description = scrapy.Field()

class TagItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    page = scrapy.Field()
    html = scrapy.Field()

class SearchTagItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    page = scrapy.Field()
    html = scrapy.Field()


class CategoryTagItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    page = scrapy.Field()
    html = scrapy.Field()


class ProductCardItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    img = scrapy.Field()
    price = scrapy.Field()
    page = scrapy.Field()
    html = scrapy.Field()
    product_id = scrapy.Field()

class ProductItem(scrapy.Item):
    product_id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    shop_link = scrapy.Field()
    shop_link2 = scrapy.Field()
    html = scrapy.Field()
    description = scrapy.Field()
    parameters = scrapy.Field()
    feedbacks = scrapy.Field()
    category = scrapy.Field()
    rate = scrapy.Field()
    colors = scrapy.Field()
    related_products = scrapy.Field()

class ImageItem(scrapy.Item):
    url = scrapy.Field()
    path = scrapy.Field()
    product_id = scrapy.Field()
    category_url = scrapy.Field()
    data = scrapy.Field()
    filename = scrapy.Field()

class SettingItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()

class SettingValueItem(scrapy.Item):
    settings_name = scrapy.Field()
    value = scrapy.Field()