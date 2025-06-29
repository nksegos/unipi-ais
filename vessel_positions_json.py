from datetime import datetime, timezone
import sys, os
import configparser
import time
import json

from redis import Redis, ConnectionError
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyproj import Transformer
from threading import Lock, Event

from bokeh.models import ColumnDataSource
from bokeh.document import Document
# import logging


# Define Global Variables
APP_ROOT = os.path.dirname(__file__)
# DATETIME_OFFSET_SEC = 2*3600 if time.daylight else 3*3600

CONFIG = configparser.ConfigParser()
CONFIG.read('server.ini')
settings = CONFIG['datastories.org']

coord_transformer = Transformer.from_crs(crs_from="EPSG:4326", crs_to="EPSG:3857", always_xy = True)

def get_utc_timestamp():
    return datetime.now(timezone.utc)

def load_from_cache(source: ColumnDataSource, record_index: dict, index_lock: Lock, code_mappings: dict, sp_cols: dict = {'x': 'lon', 'y': 'lat'}, mercator_suffix: str = '_merc'):
    
    redis_client = Redis(host=settings['redis_host'], port=settings['redis_port'], db=settings['redis_db'], decode_responses=True)
    try:
        _pong = redis_client.ping()
    except ConnectionError as e:
        print(f'Redis connection failed: {e}. Check config. Exiting...')
        return

    code_mappings.update(redis_client.hgetall('ais_code_descriptions'))

    for mmsi in redis_client.scan_iter(match='*', count=1000):
        if redis_client.type(mmsi) != 'hash':
            continue
        data = redis_client.hgetall(mmsi)
        if 'timestamp' in data:
            lon_merc, lat_merc = coord_transformer.transform(data['longitude'], data['latitude'])
            with index_lock:
                source.stream({
                    'mmsi' : [mmsi],
                    'ts': [int(data.get('timestamp'))],
                    f'{sp_cols["x"]}': [data.get('longitude')],
                    f'{sp_cols["y"]}': [data.get('latitude')],
                    'moving': [data.get('moving')],
                    'heading': [data.get('heading', "0")],
                    'vessel_name': [data.get('vessel_name', data.get('shipname', ''))],
                    'vessel_type': [data.get('vessel_type', code_mappings.get(data.get('shiptype', ''), '').split(',')[0])], 
                    'TRCMP': [-float(data.get('heading', 0))],
                    'DSCMP': [270 - float(data.get('heading', 0))],
                    f'{sp_cols["x"]}{mercator_suffix}': [lon_merc],
                    f'{sp_cols["y"]}{mercator_suffix}': [lat_merc]
                    })
                record_index[mmsi] = len(source.data['mmsi']) - 1

    redis_client.connection_pool.disconnect()

def on_record_arrival(record: dict, source: ColumnDataSource, record_index: dict, index_lock: Lock, code_mappings: dict, doc: Document, sp_cols: dict = {'x': 'lon', 'y': 'lat'}, mercator_suffix: str = '_merc'):
    
    record_type = 'kinematic' if len(record) > 4 else 'static'

    mmsi = record.get('mmsi')

    if record_type == 'kinematic':
        ts = int(record.get('timestamp'))
        lon = record.get('longitude')
        lat = record.get('latitude')
        moving = 'Y' if float(record.get('speed', 0)) > 0 else 'N'
        heading = record.get('heading', "0")
        TRCMP = -float(record.get('heading', 0)) # -0 is valid as far as Python is concerned
        DSCMP = 270 - float(record.get('heading', 0))
        lon_merc, lat_merc = coord_transformer.transform(lon, lat)
    else:
        vessel_type = code_mappings.get(str(record.get('shiptype', '')), '').split(',')[0]
        vessel_name = record.get('shipname', '')

    def update_source():
        with index_lock:
            if mmsi in record_index:
                idx = record_index[mmsi]
                if record_type == 'kinematic':
                    source.patch({
                        'ts': [(idx, ts)],
                        f'{sp_cols["x"]}': [(idx, lon)],
                        f'{sp_cols["y"]}': [(idx, lat)],
                        'moving': [(idx, moving)],
                        'heading': [(idx, heading)],
                        'TRCMP': [(idx, TRCMP)],
                        'DSCMP': [(idx, DSCMP)],
                        f'{sp_cols["x"]}{mercator_suffix}': [(idx, lon_merc)],
                        f'{sp_cols["y"]}{mercator_suffix}': [(idx, lat_merc)]
                        })
                else:
                    source.patch({
                        'vessel_type': [(idx, vessel_type)],
                        'vessel_name': [(idx, vessel_name)]
                        })
            else:
                if record_type == 'kinematic':
                    source.stream({
                        'mmsi': [mmsi],
                        'ts': [ts],
                        f'{sp_cols["x"]}': [lon],
                        f'{sp_cols["y"]}': [lat],
                        'moving': [moving],
                        'heading': [heading],
                        'TRCMP': [TRCMP],
                        'DSCMP': [DSCMP],
                        f'{sp_cols["x"]}{mercator_suffix}': [lon_merc],
                        f'{sp_cols["y"]}{mercator_suffix}': [lat_merc],
                        'vessel_name': [''],
                        'vessel_type': ['']
                        })
                    record_index[mmsi] = len(source.data['mmsi']) - 1

    doc.add_next_tick_callback(update_source)

def data_thread(thread_stop: Event, source: ColumnDataSource, record_index: dict, index_lock: Lock, code_mappings: dict, doc: Document, sp_cols: dict = {'x': 'lon', 'y': 'lat'}, mercator_suffix: str = '_merc'):

    print(f"Kafka thread starting for session '{doc.session_context.id}', subscribing to topics: {settings['kafka_topics'].split(',')}")
    conf = {
            'bootstrap.servers': settings['kafka_broker'],
            'group.id': doc.session_context.id,
            'auto.offset.reset': 'latest'
            }
    consumer = Consumer(conf)
    try:
        broker_reply = consumer.list_topics(timeout=5)
        for topic in settings['kafka_topics'].split(','):
            if topic not in broker_reply.topics:
                err = KafkaError(KafkaError._UNKNOWN_TOPIC_OR_PART, f"Topic not found or unavailable: {topic}")
                raise KafkaException(err)   
    except KafkaException as e:
        print(f'Broker connection failed: {e}. Check config. Exiting...')
        return

    consumer.subscribe(settings['kafka_topics'].split(','))

    try:
        while not thread_stop.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                time.sleep(0.1) # Very light throttling in case the msg queue is empty
                continue
            elif msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                print(f'Kafka error: {msg.error()}')
                time.sleep(0.1)
                continue
            elif msg.error():
                time.sleep(0.1) 
                continue

            record = json.loads(msg.value().decode('utf-8'))
            on_record_arrival(record=record['payload'], source=source, record_index=record_index, index_lock=index_lock, code_mappings=code_mappings, doc=doc)
    finally:
        consumer.close()


