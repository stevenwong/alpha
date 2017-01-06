""" Application

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import logging
import core.config as config
import core.database as database

class App(object):
	""" Singleton class that initialises all the settings.

	Attributes:
		config (core.Config): System config dict.

	"""

	__instance = None

	def __init__(self, *args, **kwargs):
		super(App, self).__init__(*args, **kwargs)
		
		self.config = config.Config('config.json')

		self.logger = logging.getLogger(self.config.get_value('name'))
		self.logger.setLevel(logging.DEBUG)

		fh = logging.FileHandler(self.config.get_value('logging.log_file'))
		fh.setLevel(logging.DEBUG)

		ch = logging.StreamHandler()
		ch.setLevel(logging.DEBUG)

		formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] %(message)s')
		fh.setFormatter(formatter)
		ch.setFormatter(formatter)

		self.logger.addHandler(fh)
		self.logger.addHandler(ch)

		self.cxns = {}
		self.default_db = 'local'

		dbs = self.config.get_value("database")

		for db in dbs:
			self.connect(db)

	def __new__(cls, *args, **kwargs):
		if not isinstance(cls.__instance, cls):
			cls.__instance = object.__new__(cls, *args, **kwargs)
		return cls.__instance

	def connect(self, db):
		""" Connect to database based on config.

		"""

		name = db['name']
		db_str = db['connection_str']

		self.cxns[name] = database.CoreDatabase(db_str)

	def cxn(self, datasource=None):
		""" Get default database connection"

		"""

		if datasource:
			return self.cxns[datasource]
		else:
			return self.cxns[self.default_db]
