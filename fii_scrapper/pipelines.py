import json
import requests
from base64 import urlsafe_b64encode
from itemadapter import ItemAdapter
from datetime import datetime


class FIIClojureApiPipeline():
    url = 'https://fiis-api-4x3iz.ondigitalocean.app/api/funds'
    # url = 'http://localhost:3000/api/funds'

    def process_item(self, item, spider):
        requests.post(self.url, json=ItemAdapter(item).asdict())
        return item

class FIIHistoricalDataClojureApiPipeline():
    url = 'https://fiis-api-4x3iz.ondigitalocean.app/api/funds'
    # url = 'http://localhost:3000/api/funds'

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        cnpj = item_dict['document']
        requests.patch('%s/%s' % (self.url, item_dict['code']), json={"document": cnpj})

        b3_params = urlsafe_b64encode(str.encode(json.dumps({"identifierFund": item_dict['code'].upper(), "typeFund": 7, "cnpj": cnpj}))).decode()
        b3_revenues = requests.get('https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetListedSupplementFunds/%s' % b3_params, verify=False).json().get('cashDividends', [])

        revenues = []
        for item in b3_revenues:
            revenues.append({'date': "-".join(list(reversed(item['paymentDate'].split('/')))), 'value': float(item['rate'].replace('.', '').replace(',','.'))})

        dy = sum(map(lambda i: i['value'], sorted(revenues, key=lambda rev: rev['date'])[:12]))/item_dict['last_price']*100

        historical_data = {x: item_dict[x] for x in item_dict if x not in ['revenues', 'news', 'document']}
        historical_data['date'] = historical_data['date'].strftime('%Y-%m-%d')
        historical_data['dy'] = dy
        requests.post('%s/history' % self.url, json=historical_data)



        for news_item in item_dict['news']:
            news_item.update({'date': news_item['date'].strftime('%Y-%m-%d')})

        requests.put('%s/%s/%s' % (self.url, item_dict['code'], 'revenues'), json=revenues)

        return item
