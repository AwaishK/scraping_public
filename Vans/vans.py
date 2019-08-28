import json
import re
from datetime import datetime
from urllib.parse import urlencode

from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from vans.items import VansItem


class Mixin:
    allowed_domains = ["vans.fr"]
    start_urls = ["http://www.vans.fr/"]

    retailer = "vans-fr"
    market = "FR"

    pagniation_req_url_t = "https://fsm-vfc.attraqt.com/zones-js.aspx?{0}"


class VansParser(Mixin):
    name = Mixin.retailer + "-parser"

    def parse_product(self, response):
        item = VansItem()
        item["name"] = self.product_name(response)
        item["gender"] = self.get_gender(response)
        item["market"] = self.market
        item["retailer"] = self.retailer
        item["retailer_sku"] = self.retailer_sku(response)
        item["description"] = self.product_description(response)
        item["category"] = self.product_category(response)
        item["spider_name"] = Mixin.retailer
        item["date"] = datetime.now().strftime("%Y-%m-%d")
        item["skus"] = []
        item["image_urls"] = []
        item["meta"] = {"requests": self.color_requests(response, item)}

        return self.next_request_or_item(item)

    def skus(self, response):
        color_css = ".attr-selected-color-js::text"
        skus = []

        sku = self.product_pricing(response)
        sku["color"] = response.css(color_css).extract_first()

        sizes = self.product_sizes(response)
        for size in sizes:
            sku["size"] = size
            sku["sku_id"] = f"{sku['color']}_{size}"
            skus.append(sku)

        return skus

    def next_request_or_item(self, item):
        color_requests = item["meta"]["requests"]
        yield (color_requests and color_requests.pop(0)) or item

    def parse_colors(self, response):
        item = response.meta["item"]
        item["skus"] += self.skus(response)
        item["image_urls"] += self.image_urls(response)
        return self.next_request_or_item(item)

    def product_id(self, response):
        css = ".step-container::attr(data-product-id)"
        return response.css(css).extract_first()

    def retailer_sku(self, response):
        css = "input[name='productCode']::attr(value)"
        return response.css(css).extract_first()

    def product_name(self, response):
        css = "h1.product-info-js::text"
        return response.css(css).extract_first()

    def product_price(self, response):
        css = "meta[property='og:price:amount']::attr(content)"
        return int(float(response.css(css).extract_first()) * 100)

    def previous_price(self, response):
        script_re = "var itemPrices = (.+?);\n"
        price_details = json.loads(re.findall(script_re, response.body.decode("utf-8"))[0])
        raw_price = price_details[self.product_id(response)]["pricing"]

        return int(float(raw_price["default"]["lowListPriceNumeric"]) * 100)

    def product_description(self, response):
        css = ".desc-container ::text"
        return response.css(css).extract_first()

    def product_care(self, response):
        css = ".desc-container ::text"
        return response.css(css).extract()[-1] if response.css(css).extract() else []

    def product_category(self, response):
        css = "#product-attr-form::attr(data-seo-category)"
        return response.css(css).extract()

    def product_sizes(self, response):
        css = ".swatches .attr-box::attr(data-attribute-value)"
        return response.css(css).extract()

    def get_gender(self, response):
        css = "#product-attr-form::attr(data-master-category-identifier)"
        return response.css(css).extract_first()

    def get_crawl_id(self):
        return f"vans-us-{datetime.now().strftime('%Y%m%d-%H%M%s')}-axuj"

    def currency_type(self, response):
        css = "meta[property='og:price:currency']::attr(content)"
        return response.css(css).extract_first()

    def product_pricing(self, response):
        pricing_details = {
            "price": self.product_price(response),
            "currency": self.currency_type(response)}

        prev_price = self.previous_price(response)
        if prev_price != pricing_details["price"]:
            pricing_details["previous_price"] = prev_price

        return pricing_details

    def image_urls(self, response):
        css = ".vfdp-s7-viewer-preload-image::attr(src)"
        urls = response.css(css).extract()
        return [response.urljoin(url) for url in urls]

    def color_requests(self, response, item):
        urls = response.css("button img::attr(data-product-url)").extract()
        return [Request(url, callback=self.parse_colors, meta={"item": item},
                dont_filter=True) for url in urls]

    def clean(self, dirty_strs):
        return [re.sub(':\s+', ' ', text).strip() for text in dirty_strs.split(";")]


class VansCrawler(CrawlSpider, Mixin):
    name = Mixin.retailer + "-crawler"
    parser = VansParser()
    listings_css = [".topnav-main-item"]
    product_css = [".product-block-figure"]
    deny_re = [".html"]
    PAGE_SIZE = 48

    rules = (
        Rule(LinkExtractor(restrict_css=listings_css, deny=deny_re), callback="parse_pagination"),)

    def category_zones(self, response):
        css = ".body-container div::attr(lmzone)"
        return response.css(css).extract()

    def site_id(self, response):
        script_re = "WCS_CONFIG.ATTRAQT = (.+?);"
        raw_site_id = json.loads(re.findall(script_re, response.body.decode("utf-8").replace("\n", ""))[0])
        return re.findall("zones/(.*).min", raw_site_id["MAINJS"])[0]

    def config_categorytree(self, response):
        return re.findall('categorytree : "(.*)"', response.body.decode("utf-8"))[0]

    def config_language(self, response):
        css = "meta[name='locale']::attr(content)"
        return response.css(css).extract_first()

    def parse_pagination(self, response):
        pages = self.page_count(response)
        cat_zones = self.category_zones(response)
        lang = self.config_language(response)

        parameters = {
            "zone0": cat_zones[0], "zone1": cat_zones[1], "mergehash": "true",
            "config_categorytree": self.config_categorytree(response),
            "siteId": self.site_id(response), "config_language": lang,
            "language": lang, "config_country": self.market}

        for page in range(0, pages + self.PAGE_SIZE, self.PAGE_SIZE):
            parameters["pageurl"] = f"{response.url}#esp_pg={page//self.PAGE_SIZE}"
            url = self.pagniation_req_url_t.format(urlencode(parameters))

            yield Request(url, callback=self.parse_raw_content, dont_filter=True)

    def parse_raw_content(self, response):
        script_re = "LM.buildZone\((.*)\)"
        raw_html = json.loads(re.findall(script_re, response.body.decode("utf-8"))[0])
        new_response = response.replace(body=raw_html["html"])

        return [Request(url, callback=self.parse_item) for url in self.product_urls(new_response)]

    def parse_item(self, response):
        return self.parser.parse_product(response)

    def product_urls(self, response):
        css = ".product-block-pdp-url::attr(href)"
        urls = response.css(css).extract()
        return [f"{self.start_urls[0]}{url}" for url in urls]

    def page_count(self, response):
        css = ".header-result-counter ::text"
        return int(response.css(css).re_first("\d+") or '0')
