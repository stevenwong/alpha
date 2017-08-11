""" Base class for a portfolio

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import pandas as pd

class AbstractPortfolio(object):
	""" Abstract class that provides common portfolio functionalities. It's up to the children classes to
	implement specifics for that asset class. All port

	"""

	def __init__(self):
		super(AbstractPortfolio, self).__init__()

class EquitiesPortfolio(AbstractPortfolio):
	""" Equities portfolio. Has the following standard columns::

		['quote_date', 'unique_id', 'portfolio.weight',
		'benchmark.weight', 'investable.weight', 'contraint.weight']

	The following extended columns may also be present::

		['portfolio.']

	"""

	def __init__(self):
		super(EquitiesPortfolio, self).__init__()
