import re
import json
import argparse
import requests
import pypyodbc

from parsel import Selector


class AirbnbScraperItem:
    listing_id: int
    name: str
    location: str
    description: str
    features: str
    url: str
    person_capacity: int
    bedrooms: int
    beds: int
    bath: int
    rating_value: int
    review_score: int
    review_count: int
    type: str
    image_urls: list

    def to_json(self):
        return json.loads(json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4))


class AirbnbDatabaseConnecter:

    def __init__(self):
        self.connection = pypyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                                           'Server=localhost;'
                                           'Database=Airbnb;'
                                           'uid=sa;pwd=,.Rathi1995,.')

    def create_tables(self):
        cursor = self.connection.cursor()
        query_record_table = "CREATE TABLE [Listing_Records] ([listing_id] INT, [name] VARCHAR(max ), [location] NVARCHAR(max ), [description] VARCHAR(max), [features] VARCHAR(1000), [url] VARCHAR(max ), [person_capacity] INT, [bedrooms] INT, [beds] INT, [bath] INT, [rating_value] INT, [review_score] INT, [review_count] INT, [type] VARCHAR(max ) PRIMARY KEY (listing_id));"
        query_image_urls_table = "CREATE TABLE [Images] ([Image_id] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [listing_id] INT, [url] VARCHAR(1000), FOREIGN KEY (listing_id) REFERENCES Listing_Records(listing_id));"

        if not cursor.tables(table='Listing_Records', tableType='TABLE').fetchone():
            cursor.execute(query_record_table)

        if not cursor.tables(table='Images', tableType='TABLE').fetchone():
            cursor.execute(query_image_urls_table)

        self.connection.commit()

    def insert_data(self, item):
        image_urls = item.pop("image_urls")
        data = [item["listing_id"], item["name"], item["location"], item["description"], item["features"], item["url"],
                item["person_capacity"], item["bedrooms"], item["beds"], item["bath"], item["rating_value"],
                item["review_score"], item["review_count"], item["type"]]

        command = ("INSERT INTO Listing_Records "
                   "(listing_id, name, location, description, features, url, person_capacity, bedrooms, beds, bath, rating_value, review_score, review_count, type)"
                   "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        cursor = self.connection.cursor()
        cursor.execute(command, data)
        self.connection.commit()

        command = ("INSERT INTO Images"
                   "(listing_id, url) "
                   "VALUES (?,?)")

        for url in image_urls:
            cursor.execute(command, [item['listing_id'], url])
            self.connection.commit()

        self.connection.close()


class AirbnbScraper:
    url_t = 'https://www.airbnb.com/rooms/{}'

    def scrape(self, listing_id):
        url = self.url_t.format(listing_id)
        response = Selector(text=requests.get(url).text)
        return self.parse(response)

    def parse(self, response):
        airbnb_item = AirbnbScraperItem()
        raw_item = self.raw_listing(response)
        airbnb_item.name = self.listing_name(raw_item)
        airbnb_item.listing_id = self.listing_id(raw_item)
        airbnb_item.location = self.listing_location(raw_item)
        airbnb_item.url = self.listing_url(raw_item)
        airbnb_item.description = self.listing_description(raw_item)
        airbnb_item.person_capacity = self.listing_allowed_guests(raw_item)
        airbnb_item.beds = self.listing_bed_count(raw_item)
        airbnb_item.bedrooms = self.listing_bedroom_count(raw_item)
        airbnb_item.bath = self.listing_bathroom_count(raw_item)
        airbnb_item.rating_value = self.listing_star_rating(raw_item)
        airbnb_item.features = self.listing_features(raw_item)
        airbnb_item.review_count = self.listing_review_count(raw_item)
        airbnb_item.review_score = self.listing_review_score(raw_item)
        airbnb_item.type = self.listing_property_type(raw_item)
        airbnb_item.image_urls = self.listing_image_urls(raw_item)
        return airbnb_item.to_json()

    def listing_name(self, raw_item):
        return raw_item['name']

    def listing_id(self, raw_item):
        return raw_item['id']

    def listing_url(self, raw_item):
        return self.url_t.format(raw_item['id'])

    def listing_location(self, raw_item):
        return raw_item['p3_summary_address']

    def listing_description(self, raw_item):
        return (raw_item['alternate_sectioned_description_for_p3'] or raw_item['sectioned_description'])['description']

    def listing_features(self, raw_item):
        return '|'.join([f['name'] for f in raw_item['listing_amenities'] if f['is_present']][:5])

    def listing_allowed_guests(self, raw_item):
        return int((re.findall('\d+', raw_item['guest_label']) or ['0'])[0])

    def listing_bed_count(self, raw_item):
        return int((re.findall('\d+', raw_item['bed_label']) or ['0'])[0])

    def listing_bedroom_count(self, raw_item):
        return int((re.findall('\d+', raw_item['bedroom_label']) or ['0'])[0])

    def listing_bathroom_count(self, raw_item):
        return int((re.findall('\d+', raw_item['bathroom_label']) or ['0'])[0])

    def listing_review_count(self, raw_item):
        return raw_item['review_details_interface']['review_count']

    def listing_review_score(self, raw_item):
        return raw_item['review_details_interface']['review_score']

    def listing_star_rating(self, raw_item):
        return raw_item['star_rating']

    def listing_property_type(self, raw_item):
        return raw_item['localized_room_type']

    def listing_image_urls(self, raw_item):
        return [img['large'] for img in raw_item['photos']]

    def raw_listing(self, response):
        xpath = '//script[contains(text(), "bootstrapData")]'
        raw_item = json.loads(self.clean(response.xpath(xpath).re('<!--(.*)-->'))[0])
        return raw_item['bootstrapData']['reduxData']['homePDP']['listingInfo']['listing']

    def _sanitize(self, input_val):
        """ Shorthand for sanitizing results, removing unicode whitespace and normalizing end result"""
        if isinstance(input_val, str):
            to_clean = input_val
        else:
            to_clean = input_val.extract()

        return re.sub('\s+', ' ', to_clean.replace('\xa0', ' ')).strip()

    def clean(self, lst_or_str):
        """ Shorthand for sanitizing results in an iterable, dropping ones which would end empty """
        if not isinstance(lst_or_str, str) and getattr(lst_or_str, '__iter__',False):
            return [x for x in (self._sanitize(y) for y in lst_or_str if y is not None) if x]
        return self._sanitize(lst_or_str)


def main():
    parser = argparse.ArgumentParser(
        description='Get a listing id to scrape.')
    parser.add_argument(
        '-id', '--listing_id', type=int, help='Listing_ID', required=True)
    args = parser.parse_args()

    airbnb_scraper = AirbnbScraper()
    item = airbnb_scraper.scrape(args.listing_id)

    airbnb_connector = AirbnbDatabaseConnecter()
    airbnb_connector.create_tables()
    airbnb_connector.insert_data(item)


if __name__ == "__main__":
    main()
