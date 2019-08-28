class VansPipeline(object):
    def process_item(self, item, spider):
        skus = item["skus"]

        item["price"] = min(sku["price"] for sku in skus)
        item["currency"] = skus[0]["currency"]
        del item["meta"]
        return item
