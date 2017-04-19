from quadri.spiders.immobiliare_generic import ImmobiliareGenericSpider

class ImmobiliareMaggiolinaSpider(ImmobiliareGenericSpider):
    name = "immobiliare_maggiolina"
    collection_name_to_save_data = 'quadrilocale_area_maggiolina_2017'
