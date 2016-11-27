"""
	Base class for a portfolio

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

import pandas as pd

class AbstractPortfolio(object):
	"""
		Abstract class that provides common portfolio functionalities. It's up to the children classes to
		implement specifics for that asset class.

	"""

	def __init__(self):
		super(AbstractPortfolio, self).__init__()
