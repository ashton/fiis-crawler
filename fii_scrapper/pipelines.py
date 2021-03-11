from pymongo import MongoClient
from itemadapter import ItemAdapter


class MongoPipeline:
    def open_spider(self, spider):
        self.client = MongoClient()
        self.mongo = self.client['fiis']

    def close_spider(self, spider):
        self.client.close()

class FIIMongoPipeline(MongoPipeline):
    collection_name = 'fiis'

    def process_item(self, item, spider):
        self.mongo[self.collection_name].insert_one(ItemAdapter(item).asdict())
        return item

class FIIHistoricalDataMongoPipeline(MongoPipeline):
    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        dy = sum(map(lambda i: i['value'], sorted(item_dict['revenues'], key=lambda item: item['date'])[:12]))/item_dict['last_price']*100
        historical_data = {x: item[x] for x in item if x not in ['revenues', 'news']}

        self.mongo['fiis'].update_one({'code': item_dict['code']}, {'$set': {'dy': dy}})
        self.mongo['historical_data'].insert_one(historical_data)
        for revenue in item_dict['revenues']:
            self.mongo['revenues'].update_one({'code': revenue['code'], 'date': revenue['date']}, {'$set': {'value': revenue['value'], 'base_price': revenue['base_price'], 'dy': revenue['dy']}}, upsert=True)

        for news_item in item_dict['news']:
            self.mongo['news'].update_one({'code': news_item['code'], 'date': news_item['date']}, {'$set': {'link': news_item['link'], 'title': news_item['title']}}, upsert=True)

        return item
