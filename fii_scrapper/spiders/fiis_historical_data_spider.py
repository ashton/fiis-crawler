import re
import scrapy
import requests
from datetime import datetime

class HistoricalDataSpider(scrapy.Spider):
    name = "historical_data"
    custom_settings = {
        'ITEM_PIPELINES': {
            'fii_scrapper.pipelines.FIIHistoricalDataClojureApiPipeline': 100,
        }
    }

    def start_requests(self):
        fiis = requests.get('https://matheusashton-fiis.builtwithdark.com/fiis').json()

        for fii in fiis:
            url = 'https://fiis.com.br/%s' % (fii['code'],)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        quotations = response.css('#quotations--infos-wrapper')
        summary = response.css('#informations--indexes')
        news = response.css('#news--wrapper')
        code = response.css('#fund-ticker::text').get()
        cnpj = self.normalize_cnpj(response.xpath('//*[@id="informations--basic"]/div[2]/div[3]/span[2]//text()').get())
        dy = self._parse_dy(summary.css('.item .value::text')[0].get())
        value_per_quota = self._parse_price(summary.css('.item .value::text')[3].get())
        price = self._parse_price(quotations.css('.value::text').get())
        news = self._parse_news(code, news)

        if not price:
            return False


        yield {
            'document': cnpj,
            'code': code,
            'dy': dy,
            'p_vp': price / value_per_quota,
            'last_price': price,
            'date': datetime.now(),
            'news': news
        }

    def normalize_cnpj(self, value):
        return re.sub(r'[^0-9]', '', value)

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

