"""
	Wrapper around quandl

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

import quandl
import core.config as config

class EquitySecurity(object):
	""" Representing security information

	"""

	def __init__(self, *args, **kwargs):
		super(EquitySecurity, self).__init__(*args, **kwargs)

class EquityPricing(object):
	""" Various pricing service. We want to align all equity prices into the following format

		['quote_date', 'uid', 'close', 'open', 'high', 'low', 'volume', 'adj_factor',
		'accum_adj_factor', 'bid', 'ask', 'source', 'last_update']

	"""

	def __init__(self, *args, **kwargs):
		"""

		"""

		super(EquityPricing, self).__init__(*args, **kwargs)

	def bulk(self, filename):
		""" Writes a bulk dataset to file

		"""

		pass

	def get(self, quote_date):
		pass

	def get_slice(self, start_date, end_date):
		pass

class QuandlEquityPricing(EquityPricing):
	""" Connects to quandl for pricing

		See `<https://www.quandl.com/tools/python>`

	"""

	QUANDL_API_KEY = 'quandl.api_key'

	def __init__(self, data_series, *args, **kwargs):
		super(QuandlEquityPricing, self).__init__(*args, **kwargs)

		self.data_series = data_series
		self.config = config.Config('data/quandl.json')
		
		if not quandl.ApiConfig.api_key:
			quandl.ApiConfig.api_key = self.config.get_value('QUANDL_API_KEY')

	def bulk(self, filename):
		return quandl.bulkdownload(self.data_series, filename=filename)

	def get(self, quote_date):
		pass

class QuandlWIKIEquityPricing(QuandlEquityPricing):
	""" Specifically crafted for WIKI EPD dataset

		See `<https://www.quandl.com/data/WIKI-Wiki-EOD-Stock-Prices/documentation/documentation?modal=null>`

		By default WIKI returns ['Date', 'Ticker', 'Open', 'High', 'Low',
		'Close', 'Volume', 'Ex-Dividend', 'Split Ratio', 'Adj. Open',
		'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume']

	"""

	def __init__(self, *args, **kwargs):
		super(QuandlWIKIEquityPricing, self).__init__(*args, **kwargs)
