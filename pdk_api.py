# pylint: disable=line-too-long
# -*- coding: utf-8 -*-

from __future__ import print_function

import json
import time

import arrow
import requests

from django.conf import settings
from django.contrib.gis.geos import Polygon

from passive_data_kit_external_sensors.models import SensorRegion

AIRBEAM_SENSOR_TYPES = (
    'AirBeam3-PM2.5',
    'AirBeam3-PM1',
    'AirBeam3-PM10',
    'AirBeam2-PM2.5',
    'AirBeam2-PM1',
    'AirBeam2-PM10',
    'AirBeam-PM',
    'OpenAQ-PM2.5',
)

def fetch_sensors(): # pylint: disable=too-many-locals
    sensors = []

    if hasattr(settings, 'PDK_EXTERNAL_SENSORS_AIRBEAM_URL'): # pylint: disable=too-many-nested-blocks
        valid_region = None

        for region in SensorRegion.objects.filter(include_sensors=True):
            if valid_region is None:
                valid_region = region.bounds
            else:
                valid_region = valid_region.union(region.bounds)

        start = int(arrow.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp)
        end = int(arrow.utcnow().shift(days=1).shift(seconds=-1).timestamp)

        # Chicago: 41.8781 N, 87.6298

        west = valid_region.extent[0]
        south = valid_region.extent[1]
        east = valid_region.extent[2]
        north = valid_region.extent[3]

        print('BOUNDS: NW(%f, %f) SE(%f, %f)' % (west, north, east, south))

        cell_size = 1.0

        latitude = south

        while latitude < north:
            longitude = west

            while longitude < east:
                cell_poly = Polygon((
                    (longitude, latitude),
                    (longitude, latitude + cell_size),
                    (longitude + cell_size, latitude + cell_size),
                    (longitude + cell_size, latitude),
                    (longitude, latitude),
                ))

                if cell_poly.overlaps(valid_region):
                    print('[{0}, {1}]'.format(latitude, longitude))

                    for sensor_type in AIRBEAM_SENSOR_TYPES:
                        params = {
                            'time_from': str(start),
                            'time_to': str(end),
                            'sensor_name': sensor_type,
                            'measurement_type': 'Particulate Matter',
                            'unit_symbol': 'µg/m³',
                            'tags': '',
                            'usernames': '',
                            'west': longitude,
                            'east': longitude + cell_size,
                            'south': latitude,
                            'north': latitude + cell_size,
                        }

                        response = requests.get(settings.PDK_EXTERNAL_SENSORS_AIRBEAM_URL + '/api/fixed/active/sessions.json', params={'q': json.dumps(params, ensure_ascii=False)})

                        if response.status_code == 200:
                            response_payload = response.json()

                            if response_payload is not None:
                                sessions = response_payload.get('sessions', [])

                                if len(sessions) >= 0:
                                    print(' {0:16} {1}'.format(sensor_type, len(sessions)))
                        else:
                            print('Unexpected HTTP status code for %s - %d: %s' % (response.url, response.status_code, response.text))

                        time.sleep(3)

                longitude += cell_size
            latitude += cell_size

    return sensors

def ingest_sensor_data(sensor_data): # pylint: disable=unused-argument
    pass

#    if 'pdk_identifier' in sensor_data:
#        identifier = sensor_data['pdk_identifier']
#
#        if identifier.startswith('purpleair-') and ('pdk_observed' in sensor_data) and \
#           ('Lat' in sensor_data) and ('Lon' in sensor_data):
#            model = None
#
#            if 'Type' in sensor_data:
#                model = SensorModel.objects.filter(identifier=slugify(sensor_data['Type'])).first()
#
#                if model is None:
#                    model = SensorModel(identifier=slugify(sensor_data['Type']), \
#                            name=sensor_data['Type'])
#                    model.manufacturer = 'Unknown (via Purple Air)'
#                    model.save()
#
#            sensor = Sensor.objects.filter(identifier=identifier).first()
#
#            now = timezone.now()
#
#            if sensor is None:
#                sensor = Sensor(identifier=identifier)
#
#                if 'Label' in sensor_data:
#                    sensor.name = sensor_data['Label'].strip()
#                else:
#                    sensor.name = identifier
#
#                sensor.added = now
#                sensor.model = model
#
#                sensor.save()
#
#            sensor.last_checked = now
#            sensor.save()
#
#            payload_when = sensor_data['pdk_observed']
#
#            del sensor_data['pdk_observed']
#
#            sensor_location = GEOSGeometry('POINT(%f %f)' % (sensor_data['Lon'], \
#                              sensor_data['Lat'],))
#
#            last_location = sensor.locations.all().order_by('-last_observed').first()
#
#            if last_location is None or last_location.location.distance(sensor_location) > 0.00001:
#                last_location = SensorLocation.objects.create(sensor=sensor, first_observed=now, \
#                                last_observed=now, location=sensor_location)
#            else:
#                if last_location.last_observed != payload_when:
#                    last_location.last_observed = payload_when
#                    last_location.save()
#
#            last_payload = sensor.data_payloads.filter(observed__gte=payload_when).first()
#
#            if last_payload is None:
#                data_payload = SensorDataPayload(sensor=sensor, observed=payload_when, \
#                               location=last_location)
#                data_payload.definition = sensor_data
#                data_payload.save()
#
