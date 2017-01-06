""" Data service for equities, including prices and security information.

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import quandl
import certifi
import requests
import json
import csv
import io
import pandas as pd
import datetime as dt

import core.config as config

from core.utils import *
from .scrapers import ADVFNStockInfoScraper

class EquitySecurity(object):
	""" Representing security information. Ticker is local ticker, without "US Equity". Format::

		['uid', 'ticker', 'security_name', 'exchange', 'bb_cmd', 'sedol', 'isin', 'cusip', 'ric',
		'ibes_ticker', 'currency_code', 'gics', 'country', 'security_type', 'start_date', 'end_date',
		'source', 'last_updated_date']

	"""

	def __init__(self):
		super(EquitySecurity, self).__init__()

	def get(self, quote_date):
		pass

	def update(self, quote_date):
		""" Update process for all stocks in consideration.

		Args:
			quote_date (datetime): Quote date to update.
			uids (list of integers)

		"""

		pass

class ADVFNEquitySecurity(EquitySecurity, ADVFNStockInfoScraper):
	""" Scrape ADVFN for stock information. We first go to NASDAQ to get all available stocks in the
	US.

	.. _NASDAQ:
		http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download

	.. _NYSE:
		http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download

	.. _AMEX:
		http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download

	"""

	NASDAQ = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
	NYSE = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
	AMEX = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'

	def __init__(self):
		super(ADVFNEquitySecurity, self).__init__()

	def _get_available_stocks(self, url, exchange):
		""" Since the format for all three of NASDAQ's data source is the same, we do it all in one
		function.

		Args:
			url (str): Where to get CSV.
			exchange (str): String for which exchange.

		Returns:
			DataFrame of relevant stocks with 'exchange' column.

		"""

		r = requests.get(url)
		f = io.StringIO(r.text)
		reader = csv.reader(f)
		data = [tuple(row) for row in reader]
		data = data[1:]
		data = pd.DataFrame(data, columns=['ticker', 'security_name', 'last', 'mkt_cap', 'ipo_year',
			'sector', 'subsector', 'url', 'spare'])
		data = data.loc[data.mkt_cap != 'n/a']
		data['exchange'] = exchange
		data['ipo_year'].replace(to_replace='n/a', value='1950', inplace=True)
		data['start_date'] = [dt.datetime(int(y), 1, 1) for y in data.ipo_year]

		return data

	def get(self, quote_date):
		""" Get all stocks available in the US from NASDAQ, then go to ADVFN to get all stock info.

		Args:
			quote_date (datetime): Quote date.

		"""

		# get NASDAQ stocks
		data1 = self._get_available_stocks(ADVFNEquitySecurity.NASDAQ, 'NASDAQ')
		data2 = self._get_available_stocks(ADVFNEquitySecurity.NYSE, 'NYSE')
		data3 = self._get_available_stocks(ADVFNEquitySecurity.AMEX, 'AMEX')

		stocks = pd.concat([data1, data2, data3])
		details = stocks.head(100).apply(lambda x: pd.Series(self.parse(x.ticker, x.exchange)), axis=1)
		stocks = stocks.join(drop_column(details, 'security_name'), on=['ticker', 'exchange'])

		return stocks

class EquityPricing(object):
	""" Various pricing service. We want to align all equity prices into the following format::

		['quote_date', 'uid', 'currency_code', 'open', 'high', 'low', 'close', 'shares_os', 'volume',
		'adj_factor', 'accum_adj_factor', 'accum_index', 'bid_ask_spread', 'source', 'last_update_date']

	"""

	def __init__(self):
		"""

		"""

		super(EquityPricing, self).__init__()

	def bulk(self, filename):
		""" Writes a bulk dataset to file.

		"""

		pass

	def get(self, quote_date):
		pass

class QuandlEquityPricing(EquityPricing):
	""" Connects to quandl for pricing.

	.. _Quandl Python API:
		https://www.quandl.com/tools/python

	"""

	QUANDL_API_KEY = 'quandl.api_key'

	def __init__(self, data_series, *args, **kwargs):
		super(QuandlEquityPricing, self).__init__(*args, **kwargs)

		self.data_series = data_series
		self.config = config.Config('data_source.json')
		self.api_key = self.config.get_value(QuandlEquityPricing.QUANDL_API_KEY)
		
		if not quandl.ApiConfig.api_key:
			quandl.ApiConfig.api_key = self.api_key

	def bulk(self, filename):
		return quandl.bulkdownload(self.data_series, filename=filename)

	def get(self, quote_date):
		pass

class QuandlWIKIEquityPricing(QuandlEquityPricing):
	""" Specifically crafted for WIKI EPD dataset. Dataset has the following format::

		['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ex-Dividend', 'Split Ratio',
		'Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume']

	quandl returns json in the following format::

		{
			"datatable":{
				"data" : [[...], [...]],
				"columns":[
					{"name":"ticker","type":"String"},{"name":"date","type":"Date"},
					{"name":"open","type":"BigDecimal(34,12)"},{"name":"high","type":"BigDecimal(34,12)"},
					{"name":"low","type":"BigDecimal(34,12)"},{"name":"close","type":"BigDecimal(34,12)"},
					{"name":"volume","type":"BigDecimal(37,15)"},
					{"name":"ex-dividend","type":"BigDecimal(42,20)"},
					{"name":"split_ratio","type":"BigDecimal(40,18)"},
					{"name":"adj_open","type":"BigDecimal(50,28)"},
					{"name":"adj_high","type":"BigDecimal(50,28)"},
					{"name":"adj_low","type":"BigDecimal(50,28)"},
					{"name":"adj_close","type":"BigDecimal(50,28)"},
					{"name":"adj_volume","type":"double"}
				]
			},
			"meta":{"next_cursor_id":null}
		}}

	.. _Quandl WIKI dataset:
		https://www.quandl.com/data/WIKI-Wiki-EOD-Stock-Prices/documentation/documentation?modal=null

	Returns:
		DataFrame

	"""

	def __init__(self, *args, **kwargs):
		super(QuandlWIKIEquityPricing, self).__init__('WIKI', *args, **kwargs)

		self.url = 'https://www.quandl.com/api/v3/datatables/WIKI/PRICES.json'

	def get(self, quote_date):
		""" Get all prices for one date.

		Args:
			quote_date (timestamp): Quote date to get.

		Returns:
			DataFrame

		"""

		quote_date = pd.to_datetime(quote_date)

		params = {'api_key' : self.api_key, 'date' : quote_date.strftime('%Y%m%d')}

		r = requests.get(self.url, params=params)

		dataset = json.loads(r.text)
		dataset = [tuple(row) for row in dataset["datatable"]["data"]]
		dataset = pd.DataFrame(dataset, columns=["ticker", "quote_date", "open", "high", "low", "close",
			"volume", "dividiend", "adj_factor", "adj_open", "adj_high", "adj_low", "adj_close",
			"adj_volume"])

		# WIKI dataset's split ratio column can sometimes be wrong. Both close / adj_close and split ratio
		# needs to align

		dataset['uid'] = None
		dataset['currency_code'] = 'USD'
		dataset['shares_os'] = None
		dataset['accum_adj_factor'] = 1
		dataset['bid_ask_spread'] = None
		dataset['source'] = 'WIKI'
		dataset['last_updated_date'] = dt.now()

		return dataset
