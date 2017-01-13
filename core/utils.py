"""
	core.utils

	Copyright (C) 2016 Steven Wong <steven.ykwong87@gmail.com>

	This file can not be copied and/or distributed without the express
	permission of the owner.

"""

import collections
import pandas as pd

from pandas.tseries.offsets import BMonthEnd, DateOffset
from multiprocessing import Pool, cpu_count

def drop_columns(df, to_drop):
	""" Drop specified columns

	:param df: DataFrame
	:param to_drop: Columns to drop

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
	Use this to insert into database

	cxn.executemany('insert into trade_dates values (?, ?, ?, ?, ?, ?, ?)', ts)

	:param start_date: Start date
	:param end_date: End date

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
	if not value:
		return reset
	else:
		return value

def apply_parallel(grouped, func, spare=True):
	jobs = cpu_count() - 1 if spare  else cpu_count()
	with Pool(jobs) as p:
		result = p.map(func, [group for name, group in grouped])
	return pd.concat(result)

def apply_row(df, func, spare=True):
	jobs = cpu_count() - 1 if spare  else cpu_count()
	with Pool(jobs) as p:
		result = p.map(func, [row for index, row in df.iterrows()])
	return pd.concat(result)
