#!/usr/bin/env python3

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
import threading

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

from vessel_positions_json import load_from_cache, data_thread, get_utc_timestamp, APP_ROOT


def main():

    moving_vessel_ttl = 720_000
    stationary_vessel_ttl = 1_800_000
    # Get Current Timestamp
    utc_time = get_utc_timestamp()
    sp_columns_xy = { 'x': 'lon', 'y': 'lat' }
    mercator_column_suffix = '_merc'

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
    

    def update_page_time():
        utc_time = get_utc_timestamp()
        st_viz.figure.title.text = title.format(datetime.strftime(utc_time, datetime_strfmt))

    def purge_expired():
        with mmsi_index_lock:
            data = st_viz.source.data
            now_ms = int(time.time_ns() // 1_000_000)
            keep = []
            for ts,mov in zip(data['ts'],data['moving']):
                if mov == 'Y':
                    keep.append((now_ms - ts) <= moving_vessel_ttl)
                else:
                    keep.append((now_ms - ts) <= stationary_vessel_ttl)
            if not all(keep):
                tracked = st_viz.source.selected.indices
                new_data = {
                    col: [vals[i] for i, ok in enumerate(keep) if ok]
                    for col, vals in data.items()
                }
                st_viz.source.data = new_data

                mmsi_index.clear()
                for idx, m in enumerate(st_viz.source.data['mmsi']):
                    mmsi_index[m] = idx

                tracked_new = []
                for old_idx in tracked:
                    if 0 <= old_idx < len(keep) and keep[old_idx]:
                        new_idx = sum(keep[:old_idx + 1]) - 1
                        tracked_new.append(new_idx)
                st_viz.source.selected.indices = tracked_new
        

    def on_session_kill(session_context):
        thread_stop_event.set()

    # Create ST_Visions Instance
    st_viz = st_visualizer(limit=limit)

    st_viz.set_source(source=bokeh_models.ColumnDataSource(data={'mmsi':[], 'ts':[], f'{sp_columns_xy["x"]}':[], f'{sp_columns_xy["y"]}':[], 'moving':[], 'heading':[], 'vessel_name':[], 'vessel_type':[], 'TRCMP':[], 'DSCMP':[], f'{sp_columns_xy["x"]}{mercator_column_suffix}':[], f'{sp_columns_xy["y"]}{mercator_column_suffix}':[]}))
    st_viz.sp_columns = [sp_columns_xy["x"],sp_columns_xy["y"]]
    mmsi_index = {}
    ais_type_code_mappings = {}
    mmsi_index_lock  = threading.Lock()
    thread_stop_event = threading.Event()

    load_from_cache(source=st_viz.source, record_index=mmsi_index, index_lock=mmsi_index_lock, code_mappings=ais_type_code_mappings,sp_cols=sp_columns_xy, mercator_suffix=mercator_column_suffix)

    # Create Canvas
    basic_tools = "tap,pan,wheel_zoom,save,reset" 
    st_viz.create_canvas(using_dataframes=False, suffix=mercator_column_suffix, x_range=x_range, y_range=y_range, title=title.format(pd.to_datetime(utc_time).strftime(datetime_strfmt)), sizing_mode=sizing_mode, plot_width=plot_width, plot_height=plot_height, height_policy='max', tools=basic_tools, output_backend='webgl')

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
    doc.add_periodic_callback(update_page_time, 1000) #period in ms
    doc.add_periodic_callback(purge_expired, 10000)
    doc.on_session_destroyed(on_session_kill)

    threading.Thread(target=data_thread, kwargs={'thread_stop': thread_stop_event, 'source': st_viz.source, 'record_index': mmsi_index, 'index_lock': mmsi_index_lock, 'code_mappings': ais_type_code_mappings, 'doc': doc, 'sp_cols': sp_columns_xy, 'mercator_suffix': mercator_column_suffix}, daemon=True).start()


main()
