# -*- coding: utf-8 -*-
import scrapy
import logging


class ImmobiliareOtherInfoSpider(scrapy.Spider):
    """
    Classe Helper.
    La classe permette di recuperare tutti gli atrtibuti di una pagina dettaglio
    Vengono stampati all'evento spider close. Tutti gli elementi sono salvati nel set
    item_seen.

    Viene sovrascritta la pipelines cosi da non salvare i dati su mongodb
    """
    item_seen = set()

    name = "immobiliare_other_info"
    allowed_domains = ["immobiliare.it"]
    start_urls = ['http://www.immobiliare.it/Milano/vendita_case-Milano.html?criterio=rilevanza']


    custom_settings = {
        'ITEM_PIPELINES': {
            'quadri.pipelines.CheckElementIsDuplicate': 200,
            'quadri.pipelines.CheckItemValuesPipeline': 300
        }
    }


    def closed(self, reason):
        print(len(self.item_seen))
        print(self.item_seen)


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

            for label in section_data.css('dl > dt'):
                self.item_seen.add(label.css('dt::text').extract_first())

        next_page = response.css('#listing-pagination > .pull-right > li > a::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
