"""
	start

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

import pyximport;

pyximport.install(pyimport=True)

import logging

import core.app as app
app.App()
logging.info('imported core.app')

import pandas as pd
logging.info('imported pandas as pd')

import numpy as np
logging.info('imported numpy as np')

import core.utils as utils
logging.info('imported core.utils as utils')
