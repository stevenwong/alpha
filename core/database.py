"""
	core.database

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

import pyodbc
import pandas as pd

class Database(object):
	""" Database object wrapping a pyodbc connection

	"""

	def __init__(self, db_str):
		super(Database, self).__init__()

		self.db_str = db_str
		self.cxn = pyodbc.connect(db_str)

	def __str__(self):
		return self.__class__.__name__ + '(' + self.db_str + ')'

	def cursor(self):
		return self.cxn.cursor()

	def execute(self, sql, *args, **kwargs):
		with self.cursor() as cur:
			return cur.execute(sql, *args, **kwargs)

	def executemany(self, sql, *args, **kwargs):
		with self.cursor() as cur:
			return cur.executemany(sql, *args, **kwargs)

class CoreDatabase(Database):
	""" Implements all the database calls

	"""

	def __init__(self, *args, **kwargs):
		super(CoreDatabase, self).__init__(*args, **kwargs)

	def get_trade_dates(self, start_date, end_date, freq):
	