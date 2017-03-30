# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import re

import pymongo

class QuadriPipeline(object):
    def process_item(self, item, spider):
        return item

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

    other_info_mapping
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
            item['surface'] = int(item['surface'])

        return item

class MongoPipeline(object):

    collection_name = 'quadrilocale_2017'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        #self.client = pymongo.MongoClient(self.mongo_uri)
        self.client = pymongo.MongoClient(self.mongo_uri, 27017)
        try:
            self.client['admin'].authenticate('davide', 'laCucc4r4cia', mechanism='SCRAM-SHA-1')
            self.db = self.client[self.mongo_db]
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
            #print('New Doc! : {0}'.format(cursor.count()))
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
