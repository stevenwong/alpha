""" core.database

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import pyodbc
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

	def execute(self, sql, *args, **kwargs):
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
			self._connected = False
			self.connect()
			raise

class CoreDatabase(Database):
	""" Implements all the database calls

	"""

	def __init__(self, *args, **kwargs):
		super(CoreDatabase, self).__init__(*args, **kwargs)

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

		sql = """
			select 
			from 
		"""
	