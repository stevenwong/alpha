"""
	Application

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

class Singleton(object):
	__instance = None

	def __new__(cls, *args, **kwargs):
		if not isinstance(cls.__instance, cls):
			cls.__instance = object.__new__(cls, *args, **kwargs)
		return cls.__instance

class Application(Singleton):
	""" All the application wide settings etc.

	"""

	def __init__(self, *args, **kwargs):
		super(Application, self).__init__(*args, **kwargs)
		