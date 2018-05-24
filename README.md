
# Quadri Scraper

Questo Scraper ricerca, analizza e salva su un'istanza di mongodb annunci immobiliari da diversi siti.

## Getting Started

Clona la repository, installa [virtualenv](https://virtualenv.pypa.io/en/stable/installation/) tramite pypi (pip 10.0.1 ) se non presente sul sistema


### Prerequisites

Python3.4 +, mongodb 3.2 +, pip 10.0.1 +, virtualenv 15.0.1 +


### Installing

```
$ cd path/to/project
```

lancia il virtualenv:

```
$ . bin/activate
```

Istalla tutte le dipendenze:

```
pip install -r requirements.txt
```

Lancia gli spider:

```
$ scrapy crawl nome_spider
```

Oppure lancia la shell di scrapy:

```
$ scrapy shell 'url'
```
