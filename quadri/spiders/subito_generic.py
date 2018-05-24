# -*- coding: utf-8 -*-
import scrapy
import logging

from scrapy.loader import ItemLoader
from quadri.items import SubitoSingleItemList
import datetime as datetime

class SubitoSpider(scrapy.Spider):
    name = "subito_generic"
    allowed_domains = ["subito.it"]

    collection_name_to_save_data = "quadrilocale_subito"

    current_date = (datetime.datetime.today()).replace(hour=0, minute=0, second=0, microsecond=0)

    def start_requests(self):
        # zone:
        # 1: Centro storico
        # 2: Garibaldi/Isola/Centrale
        # 3: Testi/Monza/Via Padova
        # 4: Loreto/Abruzzi/Indipendenza
        # 5: Lambrate/Città Studi
        # 6: Argonne/Forlanini/Mecenate
        # 7: Umbria/C.so 22 Marzo
        # 8: Lodi/Corvetto/Ripamonti
        # 9: Rogoredo/Chiesa Rossa
        # 10: Tibaldi/Romolo/Famagosta
        # 11: P.ta Genova/Solari/Papiniano
        # 12: Barona/Lorenteggio/L.Moro
        # 13: Baggio/Forze Armate/B.Nere
        # 14: Vercelli/Fiera/Sempione
        # 15: San Siro/Bonola/Trenno
        # 16: Certosa/Q.Oggiaro/Bovisa
        # 17: Affori/Ornato/Niguarda
        ZONE_TO_SEARCH = [2,3,4,5,7,11]
        urls = [
            'https://www.subito.it/annunci-lombardia/vendita/appartamenti/milano/milano/?ros=4&sqs=100&pe=700000&search_zone=',
            ]
        for url in urls:
            for zone in ZONE_TO_SEARCH:
                url_to_scrape = url + str(zone)
                print(url_to_scrape)
                yield scrapy.Request(url=url_to_scrape, callback=self.parse)

    def parse(self, response):
        listing = response.css('ul.items_listing > li')
        #logging.debug('Elementi nel listato: {0}'.format(len(listing), '>20'))

        # dentro il listing
        if len(listing) > 0:
            for el in listing:
                current_item = SubitoSingleItemList()
                current_item['id_el'] = int(el.css('article::attr(data-id)').extract_first())
                item_desc = el.css('div.item_description')
                current_item['title'] = item_desc.css('h2>a::attr(title)').extract_first()
                current_item['link'] = item_desc.css('h2>a::attr(href)').extract_first()
                current_item['price'] = item_desc.css('span.item_price::text').extract_first()
                current_item['date_scraped'] = self.current_date

                item_info = item_desc.css('span.item_info')
                current_item['date_published'] = item_info.css('time::attr(datetime)').extract_first()
                current_item['location'] = item_info.css('span.item_location').css('em.item_city::text').extract_first()

                current_item['agency'] = el.css('div.shop_type::text').extract_first().strip() if el.css('div.shop_type::text').extract_first() is not None else 'Privato'

                item_specs = item_desc.css('span.item_specs::text').extract_first()
                current_item['surface'] = item_specs.strip().split(',')[0].strip()
                current_item['rooms'] = item_specs.strip().split(',')[1].strip()

                print(current_item)
                yield current_item

        next_page = response.css('div.pagination_next>a::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)


    # def parse_details_page(self, response):
    #
    #     for el in listing:
    #         listing = response.css('ul.items_listing > li')
    #         logging.debug('Elementi nel listato: {0}'.format(len(listing), '>20'))
    #
    #         for el in listing:
    #             current_item = SubitoSingleItemList()
    #             current_item['name'] = el.css('article::attr(data-id)').extract_first()
    #             item_desc = el.css('div.item_description')
    #             current_item['title'] = item_desc.css('h2>a::attr(title)').extract_first()
    #             current_item['link'] = item_desc.css('h2>a::attr(href)').extract_first()
    #             current_item['price'] = item_desc.css('span.item_price::text').extract_first()
    #             current_item['date_scraped'] = self.current_date
    #
    #             item_info = item_desc.css('span.item_info')
    #             current_item['date_published'] = item_info.css('time::attr(datetime)').extract_first()
    #             current_item['location'] = item_info.css('span.item_location').css('em.item_city::text').extract_first()
    #
    #             current_item['agency'] =
    #
    #             item_specs = item_desc.css('span.item_specs::text').extract_first()
    #             current_item['surface'] = s.strip().split(',')[0].strip()
    #             current_item['rooms'] = s.strip().split(',')[1].strip()
    #
    #             yield current_item
