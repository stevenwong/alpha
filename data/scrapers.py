""" Scrapers.

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import requests

from core.utils import *
from lxml import html

class ScraperMixin(object):
	""" Scraper for ADVFN using the tutorial.

	.. _Tutorial:
		http://docs.python-guide.org/en/latest/scenarios/scrape/

	.. _ADVFN:
		http://au.advfn.com/stock-market/NYSE/SALT/share-price

	.. _XPath:
		http://www.w3schools.com/xml/xpath_nodes.asp

	"""

	def __init__(self):
		super(ScraperMixin, self).__init__()

	def fetch(self, url, params=None):
		""" Get the given URL with the given params.

		Args:
			url (str): URL to get.
			params (dict): Parameters for the URL.

		Returns:
			HTML tree.

		"""

		page = requests.get(url, params=params)
		tree = html.fromstring(page.content)

		return tree

class ADVFNStockInfoScraper(ScraperMixin):
	""" Scrapes the page:

	.. _ADVFN stock information:
		http://au.advfn.com/stock-market/NYSE/SALT/share-price

	"""

	INFO_URL = 'http://au.advfn.com/stock-market/%(exchange)s/%(ticker)s/share-price'

	def __init__(self):
		super(ADVFNStockInfoScraper, self).__init__()

	def parse(self, ticker, exchange='NYSE'):
		""" Parse the info page and return dict.

		Args:
			ticker (str): Stock ticker.
			exchange (str): If not sure, just use NYSE.

		Returns:
			dict with pre-defined data fields.

		"""

		url = ADVFNStockInfoScraper.INFO_URL % {'exchange' : exchange, 'ticker' : ticker}
		fields = dict()

		tree = super(ADVFNStockInfoScraper, self).fetch(url)

		table = tree.xpath('//tr[th="Stock Name"]/..')[0]

		text = table.xpath('./tr/td[1]//text()')
		fields['security_name'] = None if not text else text[0]

		text = table.xpath('./tr/td[2]//text()')
		fields['ticker'] = None if not text else text[0]

		text = table.xpath('./tr/td[3]//text()')
		fields['exchange'] = None if not text else text[0]

		text = table.xpath('./tr/td[4]//text()')
		fields['security_type'] = None if not text else text[0]

		text = table.xpath('./tr/td[5]//text()')
		fields['isin'] = None if not text else text[0]

		table = tree.xpath('//tr[th="Currency"]/..')[0]
		text = table.xpath('./tr/td[5]//text()')
		fields['currency_code'] = text[0]

		return fields
