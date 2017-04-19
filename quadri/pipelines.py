# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import re

import pymongo

import datetime as datetime

import logging


# needed to check frequency of letters in word.
# in particular it is used for checking date_published,
# where '/' has freq = 2
import collections

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
#         for k in item.get('other_info').keys():
#             self.item_seen.add(k)
#
#         print(self.item_seen)
#         return item

class CheckElementIsDuplicate(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self,item,spider):
        if item.get('id_el') in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item.get('id_el'))
            return item

class CheckItemValuesPipeline(object):
    regex = r"\b(\w.+)\b"
    regex_extract_digit = r"(\d+)"
    regex_rooms = r"(\d+)\+? (camer.|local.)"
    regex_bathrooms = r"(\d+)\+? (bagn.)"

    other_infos_label = {
        'rif_and_date_label' : 'Riferimento e Data annuncio',
        'rif_and_data_split_chars' : ' - ',
        'rooms_label' : 'Locali',
        'surface_label' : 'Superficie',
        'construction_year_label' : 'Anno di costruzione',
        'expenses_label' : 'Spese condominio'
    }

    def __init__(self):
        #set label to lower case, as in webpage label is in camel case
        for k, v in self.other_infos_label.items():
            self.other_infos_label[k] = v.lower()

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


    def process_other_info(self, item):
        item_other_info = item.get('other_info')
        if item_other_info:

            rif_and_data = []

            for key, value in item_other_info.items():
                if value:
                    item_other_info[key] = value.strip()

                #check and polish riferimento e date_published
                #'VE70 - 23/02/2017'
                if key == self.other_infos_label['rif_and_date_label']:
                    rif_and_data = item_other_info[self.other_infos_label['rif_and_date_label']]\
                    .split(self.other_infos_label['rif_and_data_split_chars'])

                # check and polish rooms, bathrooms
                if key == self.other_infos_label['rooms_label']:
                    rooms = None
                    bathrooms = None

                    rooms_value_match = re.search(self.regex_rooms, value)
                    bathrooms_value_match = re.search(self.regex_bathrooms, value)
                    if rooms_value_match:
                        try:
                            rooms = int(rooms_value_match.group(1))
                        except Exception as e:
                            raise
                    if bathrooms_value_match:
                        try:
                            bathrooms = int(bathrooms_value_match.group(1))
                        except Exception as e:
                            raise
                    item['rooms'] = rooms
                    item['bathrooms'] = bathrooms

                #check and polish surface
                if key == self.other_infos_label['surface_label']:
                    item['surface'] = int(re.search(self.regex_extract_digit,value).group(1))

                #check and polish year
                if key == self.other_infos_label['construction_year_label']:
                    item_other_info[key] = int(value)

                #check and polish expenses
                if key == self.other_infos_label['expenses_label']:
                    #print(expenses_label)
                    expenses = value.replace('.', '')
                    match_digit = re.search(self.regex_extract_digit, expenses)
                    #print(match_digit)
                    if match_digit != None:
                        item_other_info[key] = int(match_digit.group(0))

            # check and polish date_published
            if len(rif_and_data)>0:
                item['rif'] = rif_and_data[0]
                # from str to datetime datetime, 23/03/2017
                for el in rif_and_data:
                    if collections.Counter(el)['/'] == 2:
                        item['date_published'] = self.string_2_datetime(el)

                try:
                    item_other_info.pop(self.other_infos_label['rif_and_date_label'])
                except KeyError as e:
                    pass

            # polish other_info if surface exists in item:
            if type(item['surface']) == int:
                 item_other_info.pop(self.other_infos_label['surface_label'])
        return item

    def process_item(self, item, spider):

        #check and polish
        if item.get('id_el'):
            pass
        else:
            raise DropItem("Missing id_el in {0}".format(item))

        #check and polish description
        if item.get('desc'):
            description = ''
            for el in item.get('desc'):
                cleaned_desc = el.replace("'", "\'").replace('"', '\"') #.replace('\n', '')
                cleaned_desc = cleaned_desc.strip()#
                m = re.search(self.regex, cleaned_desc)
                if m != None:
                    description = description + ' ' + m.group(0)

            item['desc'] = description.strip()

        if item.get('price'):
            # from '€ 398.000' -> 398000
            price = None
            try:
                price = int(item.get('price').replace('€ ', '').replace('.', ''))
            except ValueError as e:
                pass

            item['price'] = price
        else:
            raise DropItem("Missing price in {0}".format(item))


        return self.process_other_info( item )

class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db, mongo_secret):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        f = open(mongo_secret, 'r')
        ulist = f.read().split(',')
        self.mongo_u = ulist[0].strip()
        self.mongo_p = ulist[1].strip()
        f.close()

        self.collection_name = ''
        self.items_already_on_db = set()
        self.current_date = (datetime.datetime.today()).replace(hour=0, minute=0, second=0, microsecond=0)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items'),
            mongo_secret=crawler.settings.get('MONGO_SECRET')
        )

    def open_spider(self, spider):
        try:
            self.collection_name = spider.collection_name_to_save_data
        except AttributeError as e:
            raise Exception('!!!Attention!!! collection_name_to_save_data to be defined in spider')


        self.client = pymongo.MongoClient(self.mongo_uri, 27017)
        try:
            self.db = self.client[self.mongo_db]
            self.db.authenticate(self.mongo_u, self.mongo_p, mechanism='SCRAM-SHA-1')
        except Exception as e:
            print('mongo db auth error %s' % e)
            #return self
        #self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        item_to_update_date_deleted = set()
        # self.items_already_on_db
        # controllo quali che siano gli elementi incontrati online e già salvati a db.
        # recupera da db tutti quei documenti che non fanno parte di quelli incontrati online
        # e che non hanno l'elemento date_deleted
        # a cui ora verrà assegnata la proprietà date_deleted con la data di oggi
        cursor = self.db[self.collection_name].find({'date_deleted':{'$exists': False}})

        # print(self.items_already_on_db)
        # print(self.collection_name)

        for el in cursor:
            if el['id_el'] not in self.items_already_on_db:
                item_to_update_date_deleted.add(el['id_el'])

        cursor.close()

        # print(item_to_update_date_deleted)

        for el_to_update in item_to_update_date_deleted:
            logging.warning('Element not in list anymore: {0}'.format(el_to_update))
            self.db[self.collection_name].update_one(
                {'id_el': el_to_update},
                { '$set':{
                    'date_deleted': self.current_date
                }}
            )
        self.client.close()

    def process_item(self, item, spider):

        cursor = self.db[self.collection_name].find({'id_el':dict(item)['id_el']})

        if cursor.count() == 0:
            #print(dict(item)['name'])
            #print('New Doc! : {0}'.format(dict(item)['name']))
            self.db[self.collection_name].insert(dict(item))
            # now the item is on db, so it adds the item to the set
            self.items_already_on_db.add(dict(item)['id_el'])
        else:
            #logging.info('Elemento gia presente a db: {0}'.format(dict(item)['id_el'], '>20'))
            self.items_already_on_db.add(dict(item)['id_el'])
            # the element is duplicated if any difference is found write it in updates
            # updates: {
            # 	'key': [['datetime', 'value'], ['datetime', 'value']],
            # 	'key': [['datetime', 'value'], ['datetime', 'value']],
            # }

            # for doc in cursor:
            #
            # self.db[collection_name].update_one(
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
