import re
import scrapy
from datetime import datetime
from pymongo import MongoClient

class HistoricalDataSpider(scrapy.Spider):
    name = "historical_data"
    custom_settings = {
        'ITEM_PIPELINES': {
            'fii_scrapper.pipelines.FIIHistoricalDataMongoPipeline': 100,
        }
    }

    def __init__(self, urls=[], *args, **kwargs):
        self.mongo = MongoClient().fiis
        super(HistoricalDataSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        fiis = self.mongo.fiis.find({})

        for fii in fiis:
            url = 'https://fiis.com.br/%s' % (fii['code'],)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        quotations = response.css('#quotations--infos-wrapper')
        summary = response.css('#informations--indexes')
        last_revenues = response.xpath('//*[@id="last-revenues--table"]/tbody/tr')
        news = response.css('#news--wrapper')

        code = response.css('#fund-ticker::text').get()
        dy = self._parse_dy(summary.css('.item .value::text')[0].get())
        patrimonial_value = self._parse_patrimonial_value(summary.css('.item .value::text')[2].get())
        price = self._parse_price(quotations.css('.value::text').get())
        revenues = self._parse_revenues(code, last_revenues)
        news = self._parse_news(code, news)

        if not price:
            return False


        yield {
            'code': code,
            'dy': dy,
            'p_vp': patrimonial_value / price,
            'last_price': price,
            'date': datetime.now(),
            'revenues': revenues,
            'news': news
        }


    def _parse_dy(self, value):
        return float(value.replace('%', '').replace(',', '.'))

    def _parse_price(self, value):
        return float(value.replace('.', '').replace(',', '.'))

    def _parse_patrimonial_value(self, value):
        unit = value[-1]
        regex = re.compile(r'(\d+),(\d{2})\s+[MB]')
        result = regex.sub(r'\1.\2', value)

        if unit == 'M':
            return float(result) * 1000000
        elif unit == 'B':
            return float(result) * 1000000000
        else:
            return float(value.replace(',', '.'))

    def _parse_revenues(self, code, revenues_elements):
        revenues = []

        for row in revenues_elements:
            regex = re.compile('[^0-9,]')
            [base_date, date, price, profitability, value] = row.css('td::text').getall()
            date = datetime.strptime(date, '%d/%m/%y')
            price = float(regex.sub('', price).replace(',', '.'))
            profitability = float(profitability.replace('%', '').replace(',', '.'))
            value = float(regex.sub('', value).replace(',', '.'))
            revenues.append({ 'code': code, 'date': date, 'base_price': price, 'dy': profitability, 'value': value })

        return revenues

    def _parse_news(self, code, news_element):
        news = []
        for item in news_element.css('ul li'):
            date = datetime.strptime(item.css('a span.date::text').get(), '%y.%m.%d')
            news_item = {
                'code': code,
                'link': item.css('a::attr(href)').get(),
                'date': date,
                'title': item.css('a span.title::text').get()
            }

            news.append(news_item)

        return news

