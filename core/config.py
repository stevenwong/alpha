""" Config

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import json

from collections import UserDict

class Config(UserDict):
	""" Config class extending dictionary to support x.y.z addressing.

	"""

	def __init__(self, filename, *args, **kwargs):
		self.filename = filename

		with open(filename, 'r') as f:
			d = json.load(f)
			super(Config, self).__init__(d, *args, **kwargs)

	def get_value(self, key, default=None):
		""" Supports addressing nested structures via x.y.z nomenclature

		Args:
			key (str): Dictionary key
			default (optional) default value to use

		"""

		def _get_value(d, k, default=None):
			xs = k.split('.', 1)
			if len(xs) == 1:
				return d.get(k, default)
			else:
				a = xs.pop(0)
				b = '.'.join(xs)
				return _get_value(d.get(a), b, default)

		return _get_value(self, key, default)
