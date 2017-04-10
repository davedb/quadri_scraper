# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import re

import pymongo

import datetime

class QuadriPipeline(object):
    def process_item(self, item, spider):
        return item

# class GetAllOtherInfoElements(object):
#     def __init__(self):
#         self.item_seen = set()
#
#
#
#
#     def process_item(self,item,spider):
#         for k in item['other_info'].keys():
#             self.item_seen.add(k)
#
#         print(self.item_seen)
#         return item

class CheckElementIsDuplicate(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self,item,spider):
        if item['id_el'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['id_el'])
            return item

class CheckItemValuesPipeline(object):
    regex = r"\b(\w.+)\b"
    regex_extract_digit = r"(\d+)"

    def string_2_datetime(self, string_date):
        """Transforms a string to datetime.datetime

        :param string_date: a str representing a date formatted these ways:

        - DD/MM/YYYY
        - YYYY-MM-DD

        :rtype: datetime.datetime object"""
        date_list = []
        if len(string_date.split('-')[0]) == 2:
            date_list = [int(string_date.split('-')[2]),int(string_date.split('-')[1]),int(string_date.split('-')[0])]
        elif len(string_date.split('-')[0]) == 4:
            date_list = [int(string_date.split('-')[0]),int(string_date.split('-')[1]),int(string_date.split('-')[2])]
        elif len(string_date.split('/')[0]) == 2:
            date_list = [int(string_date.split('/')[2]),int(string_date.split('/')[1]),int(string_date.split('/')[0])]

        return datetime.datetime(date_list[0], date_list[1], date_list[2])

    def process_item(self, item, spider):

        #check and polish
        if item['id_el']:
            pass
        else:
            raise DropItem("Missing id_el in {0}".format(item))

        if item['desc']:
            description = ''
            for el in item['desc']:
                cleaned_desc = el.replace("'", "\'").replace('"', '\"') #.replace('\n', '')
                cleaned_desc = cleaned_desc.strip()#
                m = re.search(self.regex, cleaned_desc)
                if m != None:
                    description = description + ' ' + m.group(0)

            item['desc'] = description.strip()

        if item['price']:
            # from '€ 398.000' -> 398000
            price = int(item['price'].replace('€ ', '').replace('.', ''))
            item['price'] = price
        else:
            raise DropItem("Missing price in {0}".format(item))

        if item['rooms']:
            item['rooms'] = int(item['rooms'])

        if item['bathrooms']:
            item['bathrooms'] = int(item['bathrooms'])

        if item['surface']:
            try:
                item['surface'] = int(item['surface'])
            except ValueError as e:
                item['surface'] = '-'

        item_other_info = item['other_info']
        if item_other_info:
            rif_and_date_label = 'riferimento e data annuncio'
            rif_and_data_split_chars = ' - '
            construction_year_label = 'anno di costruzione'
            expenses_label = 'spese condominio'
            #for el in label_list_to_fix:
            #'VE70 - 23/02/2017'
            rif_and_data = []
            for key, value in item_other_info.items():
                #item_other_info[key] = value.strip()
                if value:
                    item_other_info[key] = value.strip()

                if key == rif_and_date_label:
                    rif_and_data = item_other_info[rif_and_date_label].split(rif_and_data_split_chars)

                if key == construction_year_label:
                    item_other_info[key] = int(value)

                if key == expenses_label:
                    #print(expenses_label)
                    expenses = value.replace('.', '')
                    match_digit = re.search(self.regex_extract_digit, expenses)
                    #print(match_digit)
                    if match_digit != None:
                        item_other_info[key] = int(match_digit.group(0))


            if len(rif_and_data)>0:
                item_other_info['rif'] = rif_and_data[0]
                # from str to datetime datetime, 23/03/2017
                item['date_published'] = self.string_2_datetime(rif_and_data[1])

                try:
                    item_other_info.pop(rif_and_date_label)
                except KeyError as e:
                    pass


        return item

class MongoPipeline(object):

    collection_name = 'quadrilocale_area_maggiolina_2017'

    def __init__(self, mongo_uri, mongo_db, mongo_secret):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        f = open(mongo_secret, 'r')
        ulist = f.read().split(',')
        self.mongo_u = ulist[0].strip()
        self.mongo_p = ulist[1].strip()
        f.close()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'),
            mongo_secret=crawler.settings.get('MONGO_SECRET')
        )

    def open_spider(self, spider):
        #self.client = pymongo.MongoClient(self.mongo_uri)
        self.client = pymongo.MongoClient(self.mongo_uri, 27017)
        try:
            self.db = self.client[self.mongo_db]
            self.db.authenticate(self.mongo_u, self.mongo_p, mechanism='SCRAM-SHA-1')
        except Exception as e:
            print('mongo db auth error %s' % e)
            #return self
        #self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        cursor = self.db[self.collection_name].find({'id_el':dict(item)['id_el']})

        if cursor.count() == 0:
            #print(dict(item)['name'])
            #print('New Doc! : {0}'.format(dict(item)['name']))
            self.db[self.collection_name].insert(dict(item))
        else:
            # the element is duplicated if any difference is found write it in updates
            # updates: {
            # 	'key': [['datetime', 'value'], ['datetime', 'value']],
            # 	'key': [['datetime', 'value'], ['datetime', 'value']],
            # }

            # for doc in cursor:
            #
            # self.db[self.collection_name].update_one(
            #     {'name': dict(item)['name']},
            #     { '$set':{
            #         'udpates':{
            #             'query': dict(item)['query'],
            #             'price': dict(item)['price'],
            #             'date_scraped': dict(item)['date_scraped'],
            #             'location': dict(item)['location'],
            #             'year': dict(item)['year'],
            #             'mileage': dict(item)['mileage']
            #             }
            #         }
            #     }
            # )
            cursor.close()


        return item
