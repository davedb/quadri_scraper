# -*- coding: utf-8 -*-
import scrapy
import logging
from quadri.items import QuadriItem
import datetime as datetime


class ImmobiliareTestContentSpider(scrapy.Spider):
    item_seen = set()
    description_pages_scraped = 0

    name = "immobiliare_test_content"
    allowed_domains = ["immobiliare.it"]
    start_urls = ['http://www.immobiliare.it/Milano/vendita_case-Milano.html?criterio=rilevanza']


    # custom settings
    ELEMENT_TO_CHECK = 'Spese condominio'

    custom_settings = {
        'ITEM_PIPELINES': {
            # 'quadri.pipelines.CheckElementIsDuplicate': 200,
            # 'quadri.pipelines.CheckItemValuesPipeline': 300
        }
    }

    def closed(self, reason):
        print(self.item_seen)
        print('Pagine analizzate: {0}'.format(self.description_pages_scraped))
        print('Numero di contenuti diversi trovati alla voce Superficie: {0}'.format(len(self.item_seen)))


    def parse(self, response):
        listing = response.css('#listing-container > li')

        if len(listing) > 0:
            # qui siamo nella pagina di listing
            for el in listing:
                detail_page = el.css('.titolo>a::attr(href)').extract_first()
                if detail_page != None:
                    try:
                        yield scrapy.Request(detail_page, callback=self.parse)
                    except ValueError as e:
                        print(e)
        else:
            section_data = response.css('.section-data')
            self.description_pages_scraped = self.description_pages_scraped + 1
            # surface
            for label, data in zip(section_data.css('dl > dt'), section_data.css('dl > dd')):
                if label.css('dt::text').extract_first() == self.ELEMENT_TO_CHECK:
                    if data.css('dd::text').extract_first():
                        self.item_seen.add(data.css('dd::text').extract_first())
                    else:
                        self.item_seen.add(data.css('dd>span::text').extract_first())



        next_page = response.css('#listing-pagination > .pull-right > li > a::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
