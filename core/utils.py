""" core.utils

Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

This file can not be copied and/or distributed without the express
permission of the owner.

"""

import collections
import numpy as np
import pandas as pd

from pandas.tseries.offsets import BMonthEnd, DateOffset
from multiprocessing import Pool, cpu_count

# rank z-score
from scipy.stats import rankdata, zscore

def drop_columns(df, to_drop):
	""" Drop specified columns.

	Args:
		df (pandas.DataFrame): DataFrame.
		to_drop (str or list(str)): Columns to drop.

	Returns:
		pandas.DataFrame: DataFrame without to_drop.

	"""

	if not isinstance(to_drop, collections.Iterable) or isinstance(to_drop, str):
		to_drop = [to_drop]

	xs = []
	for col in df.columns:
		if not col in to_drop:
			xs.append(col)

	return df[xs]

def generate_trade_dates(start_date='1900-01-01', end_date='2199-12-31'):
	""" Generate business dates. trade_dates table has columns:
	[quote_date, next_trade_date, next_eom_date, weekday, is_eom, is_mom, counter]
	Use this to insert into database::

		cxn.executemany('insert into trade_dates values (?, ?, ?, ?, ?, ?, ?)', ts)

	Args:
		start_date (datetime): Start date.
		end_date (datetime): End date.

	Returns:
		pandas.DataFrame: DataFrame with generated trade dates.

	"""

	ts = pd.bdate_range(start_date, end_date)

	eom = ts + BMonthEnd()
	weekdays = (ts.weekday + 1) % 7

	ts = pd.DataFrame(ts)
	ts.columns = ['quote_date']
	ts['next_trade_date'] = ts.quote_date.shift(-1)
	ts['next_eom_date'] = eom
	ts['weekday'] = weekdays

	eom = pd.bdate_range(start_date, end_date, freq='BM')
	# unfortunately postgre ODBC driver doesn't seem to handle Python's boolean properly
	ts['eom_yn'] = False
	ts.loc[ts.quote_date.isin(eom), 'eom_yn'] = True


	ts['mom_yn'] = False
	ts['yymm'] = ts.quote_date.apply(lambda x: x.strftime('%Y-%m'))
	ts['dd'] = ts.quote_date.apply(lambda x: x.day)
	mom = ts.groupby('yymm').apply(lambda x: x.loc[x.dd <= 15]).groupby(level='yymm').max()
	ts.loc[ts.quote_date.isin(mom.quote_date), 'mom_yn'] = True

	ts = drop_columns(ts, ['yymm', 'dd'])

	ts['counter'] = ts.index

	return ts

def ifempty(value, reset=None):
	""" Excel's iferror style function.

	"""

	if not value:
		return reset
	else:
		return value

def apply_parallel(grouped, func, spare=True):
	""" Apply `func` to a grouped dataframe.

	Args:
		grouped (pandas.GroupBy): Grouped dataframe.
		func (function): Function to apply.
		spare (boolean, optional): Leave a CPU core spare. Default True.

	Returns:
		Aggregated dataset.

	"""

	jobs = cpu_count() - 1 if spare  else cpu_count()
	with Pool(jobs) as p:
		result = p.map(func, [group for name, group in grouped])

	return pd.concat(result)

def apply_row(df, func, spare=True):
	""" Apply `func` to every row in the dataframe.

	Args:
		df (pandas.DataFrame): DataFrame.
		func (function): Function to apply.
		spare (boolean, optional): Leave a CPU core spare. Default True.

	Returns:
		Aggregated dataset.

	"""

	jobs = cpu_count() - 1 if spare  else cpu_count()
	with Pool(jobs) as p:
		result = p.map(func, [row for index, row in df.iterrows()])

	return pd.concat(result)

def rank_zscore(x):
	return pd.DataFrame(zscore(rankdata(x)))

def winsorise(df, level=0.01, exc=None, groupby=None):
	""" Winsorise both side of dataframe according to level.

	Args:
		df (pandas.DataFrame): Dataframe to winsorise.
		level (float, optional): Level to winsorise data by.
		exc (list(str)): columns to exclude.
		groupby (str or list(str)): Winsorise by index.

	Returns:
		pandas.DataFrame: Winsorised dataframe.

	"""

	# select numeric columns
	df = df.select_dtypes(include=[np.number])

	if exc is not None:
		df = drop_columns(df, exc)

	# work out upper and lower limits
	levels = [level, 1-level]

	def _winsorise(x):
		limits = x.quantile(q=levels)
		return ((x > limits.iloc[0]) & (x < limits.iloc[1])).all(axis=1)

	if groupby is not None:
		grouped = df.groupby(level=groupby)
		idx = grouped.apply(_winsorise)
		df = df.loc[idx.values]
	else:
		df = _winsorise(df)

	return df
