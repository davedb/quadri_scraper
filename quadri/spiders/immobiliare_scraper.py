# -*- coding: utf-8 -*-
import scrapy
import logging
from quadri.items import QuadriItem

import datetime as datetime

class ImmobiliareSpider(scrapy.Spider):

    name = "immobiliare_maggiolina"
    allowed_domains = ["immobiliare.it"]
    start_urls = ['http://www.immobiliare.it/ricerca.php?idCategoria=1&idContratto=1&idTipologia=&sottotipologia=&idTipologiaStanza=&idFasciaPrezzo=&idNazione=IT&idRegione=&idProvincia=&idComune=&idLocalita=Array&idAreaGeografica=&prezzoMinimo=&prezzoMassimo=500000&balcone=&balconeOterrazzo=&boxOpostoauto=&stato=&terrazzo=&bagni=&mappa=&foto=&boxAuto=&riscaldamenti=&giardino=&superficie=&superficieMinima=120&superficieMassima=&raggio=&locali=&localiMinimo=&localiMassimo=&criterio=rilevanza&ordine=desc&map=0&tipoProprieta=&arredato=&inAsta=&noAste=&aReddito=&fumatore=&animali=&franchising=&flagNc=&gayfriendly=&internet=&sessoInquilini=&vacanze=&categoriaStanza=&fkTipologiaStanza=&ascensore=&classeEnergetica=&verticaleAste=&pag=1&vrt=45.492673,9.192534000000023;45.49560149695259,9.194957224884092;45.502664773793555,9.200580653488146;45.5018470769309,9.205817843994055;45.49425556202058,9.200370632293698;45.49213975206201,9.204035969619781;45.489803210389525,9.201169408088731;45.491137,9.198732000000064;45.487936009917284,9.195826910034157;45.489379578701346,9.191148166656376;45.492673,9.192534000000023']

    # custom settings

    collection_name_to_save_data = "quadrilocale_area_maggiolina_2017"

    current_date = (datetime.datetime.today()).replace(hour=0, minute=0, second=0, microsecond=0)

    FEATURES_POSITION = {2: 'rooms', 3: 'bathrooms', 4: 'surface'}


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
                #print('{0:_>15}:{1}'.format(label.css('dt::text').extract_first(), data.css('dd::text').extract_first()))
            # for label in section_data.css('dl > dt'):
            #     self.item_seen.add(label.css('dt::text').extract_first())


            current_item['agency'] = response.css('.contact-data > .row').css('.h5 > a::text').extract_first()
            current_item['date_scraped'] = self.current_date
            yield current_item

        next_page = response.css('#listing-pagination > .pull-right > li > a::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
