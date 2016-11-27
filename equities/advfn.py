"""
	Base class for a portfolio

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

import bs4
import urllib.request as request

class ADVFNFetch(object):
	""" Class for fetching pages on ADVFN

	"""

	BASE_URL = "http://au.advfn.com/stock-market/"
	EXCHANGE = { "NASDAQ" : "NASDAQ", "NYSE" : "NYSE" }

	def __init__(self):
		super(ADVFNFetch, self).__init__()

	def fetch(self, ticker, type, period):
		""" Fetch data based on ticker, data type and period

			:param ticker: Stock ticker
			:param type: annual/quarterly
			:param period: which financial period

		"""

		pass

	def build_financial_report_url(self, ticker, exchange, type, period_code):
		""" Build url

			:param ticker: Stock ticker
			:param type: annual/quarterly
			:param period_code: counter from first available period (0)

		"""

		exchange = 'NASDAQ' # how do we figure out which exchange?

		return ADVFNFetch.BASE_URL + 

class ADVFNAnnualFinancialsParser(object):
	""" Class for parsing financial statements on ADVFN

	"""

	def __init__(self):
		super(ADVFNFinancialStatementParser, self).__init__()

	def parse(self, html):
