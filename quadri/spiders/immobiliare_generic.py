# -*- coding: utf-8 -*-
import scrapy
import logging
from quadri.items import QuadriItem

import datetime as datetime

class ImmobiliareGenericSpider(scrapy.Spider):

    name = "immobiliare_generic"
    allowed_domains = ["immobiliare.it"]
    start_urls = ['http://www.immobiliare.it/Milano/vendita_case-Milano.html?criterio=rilevanza']

    # custom settings
    collection_name_to_save_data = "immobiliare_generic_data"

    current_date = (datetime.datetime.today()).replace(hour=0, minute=0, second=0, microsecond=0)

    FEATURES_POSITION = {2: 'rooms', 3: 'bathrooms', 4: 'surface'}

    def parse_details_page(self, response):
        """Parse a details page

        :param response:the response object from spider
        :rtype: QuadriItem"""
        # qui siamo nella pagina di dettaglio
        current_item = None
        current_item = QuadriItem()
        current_item['id_el'] = int(response.css('title::text').extract_first().split(' ')[-1])
        current_item['title'] = response.css('.title-detail::text').extract_first()
        current_item['link'] = response.url
        current_item['price'] = response.css('.features__price > span::text').extract_first()

        # rooms, bathrooms, surface
        feature_list = response.css('.detail-features > ul')
        for k, el in enumerate(feature_list.css('li')):
            if k in self.FEATURES_POSITION:
                try:
                    current_item[self.FEATURES_POSITION[k]] = int(el.css('li > div > strong::text').extract_first())
                except TypeError as e:
                    current_item[self.FEATURES_POSITION[k]] = None

        current_item['desc'] = response.css('#description > div > div::text').extract()

        current_item['other_info'] = {}
        section_data = response.css('.section-data')
        for label, data in zip(section_data.css('dl > dt'), section_data.css('dl > dd')):
            label = label.css('dt::text').extract_first().lower()
            current_item['other_info'][label] = data.css('dd::text').extract_first()


        current_item['agency'] = response.css('.contact-data > .row').css('.h5 > a::text').extract_first()
        current_item['date_scraped'] = self.current_date
        return current_item

    def parse(self, response):

        listing = response.css('#listing-container > li')

        if len(listing) > 0:
            # qui siamo nella pagina di listing
            for el in listing:
                detail_page = el.css('.titolo>a::attr(href)').extract_first()
                if detail_page != None:
                    # logging.debug('url da analizzare: {0}'.format(detail_page, '>20'))
                    try:
                        yield scrapy.Request(detail_page, callback=self.parse)
                    except ValueError as e:
                        print(e)
        else:
            yield self.parse_details_page(response)


        next_page = response.css('#listing-pagination > .pull-right > li > a::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
