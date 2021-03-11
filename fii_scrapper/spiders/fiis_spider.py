import scrapy

class FIISpider(scrapy.Spider):
    name = 'fiis'
    start_urls = ['https://fiis.com.br/lista-de-fundos-imobiliarios/']

    custom_settings = {
        'ITEM_PIPELINES': {
            'fii_scrapper.pipelines.FIIMongoPipeline': 100,
        }
    }

    def parse(self, response):
        items = response.css('#items-wrapper .item')
        for item in items:
            code = item.css('.ticker::text').get()
            name = item.css('.name::text').get()
            yield {"name": name, "code": code}
