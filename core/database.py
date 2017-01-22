""" core.database

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import pyodbc
import logging
import re
import pandas as pd

class Database(object):
	""" Database object wrapping a pyodbc connection.

	Provide our own wrapper around the pyodbc connection with some utility methods.

	Attributes:
		cxn (pyodbc.Connection): Database connection just in case you need low level access.

	"""

	def __init__(self, db_str):
		""" Connection string in ODBC format.

		Args:
			db_str (str): ODBC-compliant ODBC string.

		"""

		super(Database, self).__init__()

		self.db_str = db_str
		self._connected = False
		self.connect()

	def connect(self):
		self.cxn = pyodbc.connect(self.db_str)
		self._connected = True

	def __str__(self):
		return self.__class__.__name__ + '(' + self.db_str + ')'

	def cursor(self):
		""" Get database cursor.

		Returns:
			cursor (pyodbc.cursor): Database cursor.

		"""
		return self.cxn.cursor()

	def rollback(self):
		return self.cxn.rollback()

	def execute(self, sql, *args, **kwargs):
		# test the connection first
		try:
			with self.cursor() as cur:
				cur.execute('select 1')

		except:
			self.connect()

		try:
			cur = self.cursor()
			status = cur.execute(sql, *args, **kwargs)
			cur.commit()
			return status
		except:
			self._connected = False
			self.connect()
			raise

	def executemany(self, sql, params):
		""" Wrapper around cursor.executemany.

		Args:
			sql (str): SQL statement.
			params (list of tuples): List of tuples of values to be inserted. Can be DataFrame.

		Returns:
			status: Return value from pyodbc.

		"""

		# test the connection first
		try:
			with self.cursor() as cur:
				cur.execute('select 1')

		except:
			self.connect()

		if len(params) <= 0:
			return

		if isinstance(params, pd.Series):
			params = params.to_frame()
		
		if isinstance(params, pd.DataFrame):
			params = params.copy()
			# preprocess the data i.e., datatype conversion
			for x in params.select_dtypes(include=['datetime64']):
				params[x] = params[x].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
				params[x].replace(to_replace='NaT', value=None, inplace=True)
			params = params.values.tolist()

		try:
			cur = self.cursor()
			status = cur.executemany(sql, params)
			cur.commit()
			return status
		except:
			logging.error("Params were")
			logging.error(params)
			self._connected = False
			self.connect()
			raise

class CoreDatabase(Database):
	""" Implements all the database calls

	"""

	def __init__(self, *args, **kwargs):
		super(CoreDatabase, self).__init__(*args, **kwargs)

	def get_prev_trade_date(self, quote_date):
		""" Get previous trade date.

		Args:
			quote_date (datetime): Quote date.

		Returns:
			datetime preceding quote date.

		"""

		return self.execute('select max(quote_date) from trade_dates where quote_date < ?', (quote_date, )).fetchone()

	def get_trade_dates(self, start_date, end_date, freq='M'):
		pass

	def get_stock_list(self, quote_date, country=None, gics=None, index=None):
		""" Get all stocks at a given date, can also filter on country, gics, index etc.

		Args:
			quote_date (datetime): Stocks available at date.
			country (str): Country.
			gics (str): GICS industry, can be a sql pattern i.e., '1010%'.
			index (str): Universe, to be implemented.

		Returns:
			Dataframe of stocks available at that date.

		"""

		quote_date = pd.to_datetime(quote_date)

		sql = """
			select *
			from stock_info
			where ? between start_date and end_date
		"""

		stocks = pd.read_sql(sql, con=self, params=(quote_date, ))

		return stocks.set_index('uid')

	def get_max_uid(self):
		""" Get maximum UID in stock_info

		"""

		return self.execute("select max(uid) from stock_info").fetchone()[0]

	def get_icb_sectors(self, unique_name=False):
		""" Get ICB code and names.

		Args:
			unique_name (boolean): Only returns lowest level for every name if True. Else return all
			levels.

		Returns:
			DataFrame with code, name and level.

		"""

		icb = pd.read_sql('select * from icb_sectors', self)

		if not unique_name:
			return icb
		else:
			icb = icb.loc[icb.level == icb.groupby('name')['level'].transform(max)]
			return icb

	def get_stock_prices(cxn, quote_date, uids=None):
		""" Get all prices for a date and optionally for a list of uids.

		Args:
			cxn (database): Database.
			quote_date (datetime): Quote date.
			uids (list[int], optional): List of uid.

		Returns:
			Dataframe of prices.

		"""

		sql = """
			select p.*
			from stock_prices p
			where p.quote_date = ?
			[and p.uid in (%s)]
		"""

		if uids:
			sql = re.sub(r"[\[\]]", '', sql, 2)
			sql = sql % (','.join([str(int(uid)) for uid in uids]))
		else:
			sql = re.sub(r"\[.+\]", '', sql, 1)

		prices = pd.read_sql(sql, con=cxn, params=(quote_date, ))
		prices.set_index(['quote_date', 'uid'], inplace=True)

		return prices
	