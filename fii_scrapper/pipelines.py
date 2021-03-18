import requests
from itemadapter import ItemAdapter


class FIIDarkLangPipeline():
    url = 'https://matheusashton-fiis.builtwithdark.com/fiis'

    def process_item(self, item, spider):
        requests.post(self.url, json=ItemAdapter(item).asdict())
        return item

class FIIHistoricalDataDarkLangPipeline():
    url = 'https://matheusashton-fiis.builtwithdark.com/fiis'

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        dy = sum(map(lambda i: i['value'], sorted(item_dict['revenues'], key=lambda item: item['date'])[:12]))/item_dict['last_price']*100
        requests.patch('%s/%s' % (self.url, item_dict['code']), json={"dy": dy})

        historical_data = {x: item[x] for x in item if x not in ['revenues', 'news']}
        historical_data['date'] = historical_data['date'].isoformat()
        requests.post('%s/%s' % (self.url, 'history'), json=historical_data)

        for revenue in item_dict['revenues']:
            revenue.update({'date': revenue['date'].isoformat()})

        for news_item in item_dict['news']:
            news_item.update({'date': news_item['date'].isoformat()})

        requests.put('%s/%s/%s' % (self.url, item_dict['code'], 'revenues'), json=item_dict['revenues'])
        requests.put('%s/%s/%s' % (self.url, item_dict['code'], 'news'), json=item_dict['news'])

        return item
