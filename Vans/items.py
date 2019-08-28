from scrapy import Field, Item


class VansItem(Item):
    brand = Field()
    category = Field()
    description = Field()
    gender = Field()
    image_urls = Field()
    name = Field()
    price = Field()
    currency = Field()
    retailer_sku = Field()
    skus = Field()
    url = Field()
    market = Field()
    retailer = Field()
    date = Field()
    industry = Field()
    product_hash = Field()
    spider_name = Field()
    meta = Field()
