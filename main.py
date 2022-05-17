#!/usr/bin/env python

"""Streaming AIS Positions from Vessels near Piraeus
   Contributors: Andreas Tritsarolis, Yannis Kontoulis and Yannis Theodoridis
   Data Science Laboratory, University of Piraeus
"""


import urllib
import time
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import geopandas as gpd

import bokeh
import bokeh.models as bokeh_models
import bokeh.layouts as bokeh_layouts
import bokeh.io as bokeh_io
from bokeh.driving import linear


import sys, os
sys.path.append(os.path.join('.', 'st_visions'))

from st_visualizer import st_visualizer
import express as viz_express
import geom_helper as viz_helper 

from vessel_positions_json import get_data, track_selected, toggle_renderers, get_utc_timestamp, APP_ROOT


def main():
	# Get Current Timestamp
	utc_time = get_utc_timestamp()

	# Plot General Settings
	title = 'Univ. Piraeus AIS Stream Visualization @{0} UTC'
	x_range = (2603068.0, 2642460.0)
	y_range = (4533512.0, 4584541.0)

	sizing_mode='stretch_both'
	plot_width=250
	plot_height=250
	limit=10000

	datetime_strfmt = '%Y-%m-%d %H:%M:%S'
	numeric_strfmt = "0.000"

	tooltips = [('Vessel MMSI','@mmsi'), ('Vessel Name','@vessel_name'), ('Vessel Type','@vessel_type'), ('Timestamp','@ts{%Y-%m-%d %H:%M:%S}'), 
				('Location (lon., lat.)','(@lon{0.00}, @lat{0.00})'), ('Heading (deg.)', '@heading'), ('Vessel is Moving', '@moving')]
	


	@linear()
	def update(step):
		# Getting current UTC timestamp
		utc_time = get_utc_timestamp()

		# Flusing old Data
		st_viz.figure.title.text = 'Refreshing...'

		# Track Selected Objects #1 - Fetch at Start in order to Avoid Inconsistencies during the Update Sequence
		tracked = st_viz.source.selected.indices

		# Fetching New Data
		df = get_data()
		st_viz.canvas_data = viz_helper.getGeoDataFrame_v2(df, coordinate_columns=st_viz.sp_columns, crs='epsg:4326').to_crs(epsg=3857)
		st_viz.canvas_data = st_viz.prepare_data(st_viz.canvas_data).drop(st_viz.canvas_data.geometry.name, axis=1)


		# Track Selected Objects #2 - Find Selected Objects in the Updated Dataset
		if tracked:
			mmsis = np.array(st_viz.source.data['mmsi'])[tracked]
			tracked = track_selected(df, mmsis)
				
		# Visualize Data
		## Hide Objects and Replace Tracked Indices behind-the-scenes (Fix Flickering)
		toggle_renderers(st_viz)
		st_viz.source.selected.indices = tracked

		## Prepare Data for Visualization		
		st_viz.source.data = st_viz.canvas_data.to_dict(orient="list")

		## Show Objects (Fix Flickering)
		toggle_renderers(st_viz)

		## Flush Buffers
		st_viz.canvas_data = None
		st_viz.data = None
		
		## Update Title
		st_viz.figure.title.text = title.format(datetime.strftime(utc_time, datetime_strfmt))
		

		
	# Create ST_Visions Instance
	st_viz = st_visualizer(limit=limit)
	
	# Add Data
	df = get_data()
	st_viz.set_data(df.copy())
	


	# Create Canvas
	basic_tools = "tap,pan,wheel_zoom,save,reset" 
	st_viz.create_canvas(x_range=x_range, y_range=y_range, title=title.format(pd.to_datetime(utc_time).strftime(datetime_strfmt)), sizing_mode=sizing_mode, 
						 plot_width=plot_width, plot_height=plot_height, height_policy='max', tools=basic_tools, output_backend='webgl')

	# Add Tooltips & Map Layer
	st_viz.add_hover_tooltips(tooltips=tooltips, formatters={'@ts': 'datetime'}, mode="mouse", muted_policy='ignore')
	st_viz.add_map_tile('CARTODBPOSITRON')
	
	# Define Date and Time Formatters 
	datefmt = bokeh.models.DateFormatter(format=datetime_strfmt)
	hfmt = bokeh.models.NumberFormatter(format=numeric_strfmt)



	# Add a tabular view for the data
	columns = [
		bokeh_models.TableColumn(field="mmsi", title="MMSI", default_sort='ascending', width=110),
		bokeh_models.TableColumn(field="ts", title="Timestamp", default_sort='descending', formatter=datefmt, width=140),
		bokeh_models.TableColumn(field="lon", title="Longitude", sortable=False, formatter=hfmt, width=90),
		bokeh_models.TableColumn(field="lat", title="Latitude", sortable=False, formatter=hfmt, width=90),
		bokeh_models.TableColumn(field="heading", title="Heading", sortable=False, width=90),
		bokeh_models.TableColumn(field="moving", title="Moving", width=90),
		bokeh_models.TableColumn(field="vessel_name", title="Vessel Name", width=130),
		bokeh_models.TableColumn(field="vessel_type", title="Vessel Type", width=130),   
	]
	data_table = bokeh_models.DataTable(source=st_viz.source, columns=columns, height_policy='max', width_policy='max', height=plot_height, width=plot_width, css_classes=['selected'], autosize_mode='none')
	
	# Add Application (inc. DataTable) CSS 
	header = bokeh_models.Div(text=f"<link rel='stylesheet' type='text/css' href='{os.path.basename(APP_ROOT)}/static/css/styles.css'>")
	data_table = bokeh_layouts.column(header, data_table)



	# Add (Different) Glyphs for Moving and Statinonary Vessels
	ves_moving = bokeh_models.CDSView(source=st_viz.source, filters=[bokeh_models.GroupFilter(column_name='moving', group='Y')])
	ves_stat = bokeh_models.CDSView(source=st_viz.source, filters=[bokeh_models.GroupFilter(column_name='moving', group='N')])

	## Green Arrow for Moving Vessels
	_ = st_viz.add_glyph(glyph_type='triangle', size=13, angle='TRCMP', angle_units='deg', color='forestgreen', alpha=1, nonselection_alpha=0, fill_alpha=0.5, muted_alpha=0, legend_label='Moving', view=ves_moving)
	_ = st_viz.add_glyph(glyph_type='dash', size=13, angle='DSCMP', angle_units='deg', color='forestgreen', alpha=1, nonselection_alpha=0, line_width=3, line_cap='round', muted_alpha=0, legend_label='Moving', view=ves_moving)
	
	## Red Circle for Stationary Vessels
	_ = st_viz.add_glyph(glyph_type='circle', size=7, color='orangered', alpha=1, nonselection_alpha=0, fill_alpha=0.5, muted_alpha=0, legend_label='Stationary', view=ves_stat)


	# Remove grid lines from Figure
	st_viz.figure.xgrid.grid_line_color = None
	st_viz.figure.ygrid.grid_line_color = None

	# Customize Plot Legend
	st_viz.figure.legend.title = 'Movement Status'
	st_viz.figure.legend.title_text_font = 'Arial'
	st_viz.figure.legend.title_text_font_style = 'bold'
	st_viz.figure.legend.location = "top_left"
	st_viz.figure.legend.click_policy = "mute"

	# Customize Plot Toolbar
	st_viz.figure.match_aspect = True
	st_viz.figure.add_tools(bokeh_models.LassoSelectTool(select_every_mousemove=False))
	st_viz.figure.add_tools(bokeh_models.BoxSelectTool(select_every_mousemove=False))
	st_viz.figure.add_tools(bokeh_models.BoxZoomTool(match_aspect=True))

	# Set Active Toolkits
	st_viz.figure.toolbar.active_scroll = st_viz.figure.select_one(bokeh_models.WheelZoomTool)
	st_viz.figure.toolbar.active_tap = st_viz.figure.select_one(bokeh_models.TapTool)
	
	# Create Page Application
	doc = bokeh_io.curdoc()
	doc.title = 'Univ. Piraeus AIS Stream Visualization'

	# Add DataStories Logo 
	logo_height = 27
	url = os.path.join(os.path.basename(APP_ROOT), 'static', 'logo.png')
	app_logo = bokeh_models.Div(text=f'''<a href="http://www.datastories.org/" target="_blank"> <img src={url} height={logo_height}> </a>''', height_policy='min', height=logo_height, margin=(-5, 0, -5, 0))    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.



	# Render Canvas and Instantiate Recurrent Function
	st_viz.show_figures([[app_logo], [st_viz.figure, data_table]], notebook=False, toolbar_options=dict(logo=None), sizing_mode=sizing_mode, doc=doc, toolbar_location='right')
	doc.add_periodic_callback(update, 5000) #period in ms


main()
