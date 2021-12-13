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
    created_at = scrapy.Field()

class SettingValueItem(scrapy.Item):
    settings_name = scrapy.Field()
    value = scrapy.Field()
    url = scrapy.Field()
    created_at = scrapy.Field()

#####################################
########### Export database
#####################################

class ExSettingItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    created_at = scrapy.Field()

class ExSettingValueItem(scrapy.Item):
    settings_name = scrapy.Field()
    value = scrapy.Field()
    url = scrapy.Field()
    created_at = scrapy.Field()

class ExProductItem(scrapy.Item):
    title = scrapy.Field()
    description = scrapy.Field()
    price = scrapy.Field()
    is_in_stock = scrapy.Field()
    url = scrapy.Field()
    url_review = scrapy.Field()
    rating = scrapy.Field()
    count_review = scrapy.Field()
    created_at = scrapy.Field()
    product_id = scrapy.Field()
    rate = scrapy.Field()

class ExProductColorItem(scrapy.Item):
    hex = scrapy.Field()
    product_id = scrapy.Field()
    created_at = scrapy.Field()

class ExSearchProductItem(scrapy.Item):
    product_id = scrapy.Field()
    child_id = scrapy.Field()
    created_at = scrapy.Field()

class ExProductImageItem(scrapy.Item):
    path = scrapy.Field()
    type = scrapy.Field()
    product_id = scrapy.Field()
    created_at = scrapy.Field()

class ExProductPriceItem(scrapy.Item):
    url = scrapy.Field()
    name = scrapy.Field()
    delivery = scrapy.Field()
    price = scrapy.Field()
    product_id = scrapy.Field()
    count_review = scrapy.Field()
    rating = scrapy.Field()
    discount_price = scrapy.Field()
    created_at = scrapy.Field()

class ExReviewItem(scrapy.Item):
    name = scrapy.Field()
    dignity = scrapy.Field()
    flaw = scrapy.Field()
    grade = scrapy.Field()
    product_id = scrapy.Field()
    date = scrapy.Field()
    image = scrapy.Field()
    comment = scrapy.Field()
    use_experince = scrapy.Field()
    city = scrapy.Field()
    created_at = scrapy.Field()

class ExCategoryItem(scrapy.Item):
    name = scrapy.Field()
    parent_id = scrapy.Field()
    description = scrapy.Field()
    image_path = scrapy.Field()
    created_at = scrapy.Field()

class ExCategorySearchItem(scrapy.Item):
    child_id = scrapy.Field()
    category_id = scrapy.Field()
    created_at = scrapy.Field()

class ExProductCategoryItem(scrapy.Item):
    product_id = scrapy.Field()
    category_id = scrapy.Field()
    created_at = scrapy.Field()

class ExTagItem(scrapy.Item):
    name = scrapy.Field()
    category_id = scrapy.Field()
    created_at = scrapy.Field()

class ExProductSettingsItem(scrapy.Item):
    product_id = scrapy.Field()
    settings_id = scrapy.Field()
    settings_value_Id = scrapy.Field()
    created_at = scrapy.Field()

class ExNewCategoryItem(scrapy.Item):
    category_id = scrapy.Field()
    new_category_id = scrapy.Field()
    created_at = scrapy.Field()

class ExCategoryHasSettingsItem(scrapy.Item):
    settings_id = scrapy.Field()
    category_id = scrapy.Field()
    created_at = scrapy.Field()

