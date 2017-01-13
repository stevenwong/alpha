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
import numpy as np
import pandas as pd
import datetime as dt

import core.config as config

from core.utils import *
from .scrapers import ADVFNStockInfoScraper

class EquitySecurity(object):
	""" Representing security information. Ticker is local ticker, without "US Equity". Format::

		['uid', 'ticker', 'security_name', 'exchange', 'bb_ticker', 'sedol', 'isin', 'cusip', 'ric',
		'ibes_ticker', 'currency_code', 'gics', 'icb', 'country', 'security_type', 'start_date', 'end_date',
		'source', 'last_updated_date']

	"""

	def __init__(self):
		super(EquitySecurity, self).__init__()

	def get(self, cxn, quote_date):
		# Implemented by subclass
		pass

	def insert_new_stocks(self, cxn, stocks):
		""" Given list of ready-to-insert stocks, insert them.

		Args:
			cxn (database): Database connection.
			stocks (list): List of stocks to be inserted.

		Returns:
			Status of insert.

		"""

		# insert new stocks
		sql = """
			insert into stock_info (uid, ticker, security_name, exchange, bb_ticker, sedol, isin, cusip, ric,
			ibes_ticker, currency_code, gics, icb, country, security_type, start_date, end_date,
			source) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		"""
		return cxn.executemany(sql, stocks)

	def set_new_uid(self, stocks, existing):
		""" Set new uids for stocks in stocks where uid is null.

		Args:
			stocks (list): List of stocks to set.
			existing (list): List of existing stocks.

		Args:
			stocks

		"""

		max_uid = existing.uid.max()
		if max_uid is None or max_uid is np.nan:
			max_uid = 0

		for i in range(len(stocks)):
			# loop through each stock and set its uid individually if they are None.
			if stocks.iloc[i, 0] is None or stocks.iloc[i, 0] is np.nan:
				max_uid += 1
				stocks.iloc[i, 0] = max_uid

		return stocks

	def update(self, cxn, quote_date, debug=False):
		""" Update process for all stocks in consideration.

		Args:
			cxn (Database): Database connection
			quote_date (datetime): Quote date to update.

		"""

		prev_date = cxn.get_prev_trade_date(quote_date)

		existing = cxn.get_stock_list(quote_date).reset_index()
		stocks = self.get(cxn, quote_date)

		# match existing and new stocks to see what's changed
		# order is [ticker, sedol, isin]
		stocks['uid'] = pd.merge(stocks[['ticker', 'exchange']], existing[['ticker', 'exchange', 'uid']], how='left', on=['ticker', 'exchange'])['uid']
		stocks['uid'].fillna(pd.merge(stocks.sedol.to_frame('sedol'), existing[['sedol', 'uid']], how='left', on='sedol')['uid'], inplace=True)
		stocks['uid'].fillna(pd.merge(stocks['isin'].to_frame('isin'), existing[['isin', 'uid']], how='left', on='isin')['uid'], inplace=True)

		stocks['status'] = 'new'
		stocks.loc[pd.notnull(stocks.uid), 'status'] = 'exist'
		stocks = stocks.where(pd.notnull(stocks), None)

		stocks = self.set_new_uid(stocks, existing)

		to_insert = stocks.loc[stocks.status == 'new']
		matched = stocks.loc[stocks.status == 'exist']
		matched.sort_values('uid', inplace=True)
		existing = existing.loc[existing.uid.isin(matched.uid)]
		existing.sort_values('uid', inplace=True)

		ne = (drop_columns(existing, ['source', 'last_updated_date', 'start_date', 'end_date']) !=
			drop_columns(matched, ['source', 'last_updated_date', 'start_date', 'end_date', 'status'])).any(1)

		print(ne)
		to_update = existing.loc[ne]
		to_insert2 = matched.loc[ne]

		# two steps, end date the previous entry then insert the new updates.
		sql2 = """
			update stock_info set end_date = ?
			where uid = ?
		"""
		cxn.executemany(sql2, to_update.uid)

		to_insert = pd.concat([to_insert, to_insert2])
		to_insert = drop_columns(to_insert, 'status')

		# insert new stocks
		self.insert_new_stocks(cxn, to_insert)

		return stocks

class ADVFNEquitySecurity(EquitySecurity, ADVFNStockInfoScraper):
	""" Scrape ADVFN for stock information. We first go to NASDAQ to get all available stocks in the
	US.

	.. US stock list:
		http://www.nasdaq.com/screening/company-list.aspx

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

		return data

	def _parse_ticker(self, x):
		return pd.Series(self.parse(x['ticker'], x['exchange']))

	def get(self, cxn, quote_date):
		""" Get all stocks available in the US from NASDAQ, then go to ADVFN to get all stock info.

		Args:
			quote_date (datetime): Quote date.

		"""

		# get NASDAQ stocks
		data1 = self._get_available_stocks(ADVFNEquitySecurity.NASDAQ, 'NASDAQ')
		data2 = self._get_available_stocks(ADVFNEquitySecurity.NYSE, 'NYSE')
		data3 = self._get_available_stocks(ADVFNEquitySecurity.AMEX, 'AMEX')

		stocks = pd.concat([data1, data2, data3])

		details = stocks.apply(lambda x: pd.Series(self.parse(x.ticker, x.exchange)), axis=1)
		
		stocks = pd.merge(stocks, drop_columns(details, 'security_name'), on=['ticker', 'exchange'], how='left')
		stocks = drop_columns(stocks, ['last', 'mkt_cap', 'url', 'spare', 'ipo_year'])

		icb = cxn.get_icb_sectors(unique_name=True)

		stocks.loc[stocks.security_name == 'n/a', 'security_name'] = None
		stocks.loc[stocks.sector == 'n/a', 'sector'] = None
		stocks.loc[stocks.subsector == 'n/a', 'subsector'] = None
		translated = pd.merge(stocks[['ticker', 'subsector']], icb[['code', 'name']], how='left', left_on='subsector',
			right_on='name')
		translated = translated.rename(columns={'code' : 'icb'})
		stocks['icb'] = translated.icb
		stocks['uid'] = None
		stocks['country'] = 'US'
		stocks['bb_ticker'] = stocks.ticker + ' ' + stocks.country + ' Equity'
		stocks['sedol'] = None
		stocks['cusip'] = None
		stocks['ric'] = None
		stocks['ibes_ticker'] = None
		stocks['gics'] = None
		stocks['source'] = 'WIKI'
		stocks['start_date'] = quote_date
		stocks['end_date'] = '9999-12-31'

		stocks = stocks[['uid', 'ticker', 'security_name', 'exchange', 'bb_ticker', 'sedol', 'isin', 'cusip', 'ric',
			'ibes_ticker', 'currency_code', 'gics', 'icb', 'country', 'security_type', 'start_date', 'end_date',
			'source']]

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

	def get(self, cxn, quote_date):
		""" Get all prices for one date.

		Args:
			cxn (Database): Database connection
			quote_date (timestamp): Quote date to get.

		Returns:
			DataFrame with all the columns in stock_prices

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
		dataset['accum_index'] = None

		# figure out the uids
		stocks = cxn.get_stock_list(quote_date).reset_index()
		dataset['uid'] = pd.merge(dataset[['ticker']], stocks[['ticker', 'uid']],
			how='left', on=['ticker'])['uid']

		e = EquitySecurity()

		missing = dataset.loc[pd.isnull(dataset.uid)]
		e.set_new_uid(missing, stocks)

		dataset = dataset[['quote_date', 'uid', 'currency_code', 'open', 'high', 'low', 'close',
			'shares_os', 'volume', 'adj_factor', 'accum_adj_factor', 'accum_index', 'bid_ask_spread',
			'source']]

		return dataset
