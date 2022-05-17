from datetime import datetime, timezone
import urllib
import numpy as np
import pandas as pd
import psycopg2
import sys, os
import configparser
# import logging


# Define Global Variables
APP_ROOT = os.path.dirname(__file__)
# DATETIME_OFFSET_SEC = 2*3600 if time.daylight else 3*3600

CONFIG = configparser.ConfigParser()
CONFIG.read('server.ini')



def get_utc_timestamp():
	return datetime.now(timezone.utc)


def fetch_latest_positions():
	sql  = "SELECT * FROM AIS_latest_positions ORDER BY ts DESC"

	try:
		# Connect to Database and get the latest positions
		settings = CONFIG['datastories.org']
		conn = psycopg2.connect(host=settings['host'], port=int(settings['port']), dbname=settings['dbname'], 
								user=settings['user'], password=settings['pass'], connect_timeout=3)
								
		print(f'[{get_utc_timestamp().strftime("%Y-%m-%d %H:%M:%S")} UTC] [INFO] Connected to Database!', flush=True)
		result = pd.read_sql_query(sql, conn)
		conn.close()

	except Exception:
		print(f'[{get_utc_timestamp().strftime("%Y-%m-%d %H:%M:%S")} UTC] [ERROR] Connection to Database Timed Out!', flush=True)
		# if it fails (for some reason) return an empty DataFrame
		result = pd.DataFrame(data=[], columns=['mmsi', 'ts', 'lon', 'lat', 'moving', 'speed', 'heading', 'vessel_name', 'flag', 'vessel_type'])
	
	return result


def get_data():
	## Use Python PostgreSQL Driver
	df = fetch_latest_positions()
	
	## Meteorological Discipline
	df.loc[:, 'TRCMP'] = - df.heading
	df.loc[:, 'DSCMP'] = 270 - df.heading
	df.sort_values('ts', ascending=False, inplace=True)

	return df.reset_index(drop=True)


def track_selected(df, mmsis):
	return df.loc[df.mmsi.isin(mmsis)].index.values.tolist()


def toggle_renderers(viz):
	for renderer in viz.renderers: 
		renderer.visible = not renderer.visible
