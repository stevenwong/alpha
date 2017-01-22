""" Scrapers.

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import requests

from requests.exceptions import *
from core.utils import *

from bs4 import BeautifulSoup

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
		soup = BeautifulSoup(page.content, 'html.parser')

		return soup

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

		try:
			soup = super(ADVFNStockInfoScraper, self).fetch(url)
		except TooManyRedirects:
			return None

		table = soup.find('th', string='Stock Name').parent.parent

		if not table:
			return None

		rows = table.find_all('tr')

		if len(rows) < 2:
			return None

		row = rows[1]

		cells = row.find_all('td')
		fields['security_name'] = None if not len(cells) > 0 else cells[0].string.strip()

		fields['ticker'] = None if not len(cells) > 1 else cells[1].string.strip()

		fields['exchange'] = None if not len(cells) > 2 else cells[2].string.strip()

		fields['security_type'] = None if not len(cells) > 3 else cells[3].string.strip()

		fields['isin'] = None if not len(cells) > 4 else cells[4].string.strip()

		text = soup.find('span', id='quoteElementPiece24').string
		fields['currency_code'] = None if not text else text.strip()

		return fields

class ADVFNFinancials(ScraperMixin):
	""" Get stock financials from ADVFN.

	.. _ADVFN:
		http://au.advfn.com/stock-market/{exchange}/{ticker}/financials?btn=istart_date&istart_date={i}&mode=quarterly_reports

	"""

	QTR_URL = 'http://au.advfn.com/stock-market/%(exchange)s/%(ticker)s/financials?istart_date=%(i)s&mode=quarterly_reports'

	def __init__(self):
		super(ADVFNFinancials, self).__init__()

	def _parse(self, ticker, exchange, i):
		""" Parse the quarterly financials page and return a dict with key on years.

		"""

		url = ADVFNFinancials.QTR_URL % {'exchange' : exchange, 'ticker' : ticker, 'i' : i}

		try:
			tree = super(ADVFNFinancials, self).fetch(url)
		except TooManyRedirects:
			return None

		select = tree.xpath('/select[id="istart_dateid"]')

		return select

	def parse(self, ticker, exchange=None):
		pass
