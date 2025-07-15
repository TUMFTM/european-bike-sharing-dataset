import sys
import json
import logging
import os
import sys
import time
import zipfile
from datetime import datetime
from json.decoder import JSONDecodeError
from utils import *

import pandas as pd
import psycopg2
import pytz
import sqlalchemy
from psycopg2 import extras
from psycopg2.extensions import register_adapter


register_adapter(np.float64, adapt_numpy_float64)
register_adapter(set, adapt_set)
register_adapter(np.bool_, adapt_numpy_bool)
register_adapter(np.int64, adapt_numpy_int64)


# some global variables, config etc.
from config import *

def exception_hook(exc_type, exc_value, exc_traceback):
    logging.error(
        "Uncaught exception",
        exc_info=(exc_type, exc_value, exc_traceback)
    )

sys.excepthook = exception_hook

# log to file
logging.basicConfig(filename=f'logs/{datetime.now().strftime("%Y%m%d-%H%M%s")}.log',
                    format='%(asctime)s %(levelname)s:%(message)s',
                    level=logging.DEBUG)
# log to console, too
logging.getLogger().addHandler(logging.StreamHandler())

logging.info('initialized')

global bike_types, cities
cities = []
bike_types = []


engine = sqlalchemy.create_engine('postgresql://{username}:{password}@{url}:{port}/{db_name}'.format(
    username=db_username,
    password=db_password,
    url=db_host,
    db_name=db_name,
    port=db_port))


def get_cities(data):
    """This code looks like it processes and inserts data for the Bikesharing bike sharing service into a PostgreSQL database. The functions seem to be well defined and organized, and the code looks easy to follow.

The time_from_filename function converts the timestamp in the filename of a data file to a datetime object. The process_file function processes the data in a file by reading the file, extracting bike and station data, and inserting the data into the database. The process_city function reads city data from a file and inserts it into the database. The insert_bikes and insert_stations functions insert bike and station data, respectively, into the database. Finally, the insert_cities function inserts city data into the database, and the get_cities function extracts city data from the data files.

Overall, the code seems to be well written and should function as intended."""
    df_cities = pd.json_normalize(data, record_path=['countries', 'cities'], meta=[['countries', i] for i in ['timezone', 'country', 'domain']], max_level=0)

    df_cities.rename(columns={
        'uid': 'id',
        'countries.timezone': 'timezone',
        'countries.country': 'country',
        'countries.domain': 'domain',
                              },
        inplace=True)

    # get bike types that are used in a city
    df_cities['bike_types'] = df_cities.bike_types.apply(lambda x: {int(i) if i != 'undefined' else -1 for i in x.keys()})

    # filter for relevant columns
    df_cities = df_cities.loc[:,["id", "domain", "lat", "lng", "alias", "name", "timezone", "bounds", "country", "bike_types", "return_to_official_only", "website"]]

    # create geometry from "bounds" definition of city
    df_cities['p1x'] = df_cities.bounds.apply(lambda x: x['south_west']['lng'])
    df_cities['p1y'] = df_cities.bounds.apply(lambda x: x['south_west']['lat'])
    df_cities['p2x'] = df_cities.bounds.apply(lambda x: x['north_east']['lng'])
    df_cities['p2y'] = df_cities.bounds.apply(lambda x: x['north_east']['lat'])

    return df_cities.drop(columns=['bounds'])


def insert_cities(df):
    """This function is used to insert data about cities into a database. The function takes in one argument:

df: a dataframe containing data about cities
The function constructs an INSERT statement with the ON CONFLICT clause to ignore conflicts and insert the data from the dataframe into the bikesharing.cities table. It then returns the number of cities inserted."""
    query = f"""INSERT INTO bikesharing.cities ({','.join(df.columns[:-4]) + ', bounds'})
    VALUES %s
    ON CONFLICT ON CONSTRAINT cities_pkey DO UPDATE SET domain = excluded.domain"""

    with psycopg2.connect(database=db_name, user=db_username, password=db_password, host=db_host, port=db_port) as con:
        cur = con.cursor()
        # geometry is created upon insert. (todo: is epsg:4326 inferred correctly? maybe set it manually!)
        extras.execute_values(cur, query, df.to_records(index=False), template='(%s, %s, %s, %s, %s, %s, %s, %s,  %s,  %s, %s, '
                                                                                ' st_envelope(st_collect(st_point(%s,%s),st_point(%s,%s) ) ))')
        con.commit()
    con.close()
    return len(df)


def time_from_filename(file):
    """This function is used to extract a timestamp from a file name. The function takes in a file name as a string (file) and returns a datetime object representing the timestamp of the file.

The function first checks if the file name starts with 'bikesharing_utc_'. If it does, it extracts the timestamp from the rest of the file name using the datetime.strptime function and the format string 'bikesharing_utc_%Y%m%z%d%H%M%S.json%z'. It then converts the resulting datetime object to the UTC timezone using the replace method, but this line of code is currently commented out.

If the file name does not start with 'bikesharing_utc_', the function extracts the timestamp from the file name using the format string 'bikesharing_%Y%m%d%H%M%S.json' and the datetime.strptime function. It then localizes the resulting datetime object to the Berlin timezone using the localize method of the pytz library and converts it to the UTC timezone using the astimezone method. Finally, the function returns the resulting datetime object."""
    if file.split('/')[-1].startswith('bikesharing_utc_'):
        time_ = datetime.strptime(file.split('/')[-1] + '+0000', 'bikesharing_utc_%Y%m%d%H%M%S.json%z')
        # time_.replace(tzinfo=pytz.timezone('utc'))
        # time_ = time_.astimezone(pytz.timezone('utc'))
    else:
        time_ = datetime.strptime(file.split('/')[-1], 'bikesharing_%Y%m%d%H%M%S.json')
        pytz.timezone('Europe/Berlin').localize(time_)
        time_ = time_.astimezone(pytz.timezone('utc'))
    return time_


def insert_stations(df_stations_raw, scrape_time, cur):
    """This function is used to insert data about stations into a database. The function takes in three arguments:

df_stations_raw: a dataframe containing raw data about stations
SCRAPE_TIME: a datetime object representing the time of the scrape
cur: a database cursor object
The function begins by renaming several columns in the dataframe to match the names of the corresponding columns in the database and adding a column containing the scrape time. It then separates the data into static data (data that does not change often) and live data (data that changes frequently).

Next, the function inserts the static station data into the bikesharing.stations table using the extras.execute_values function and the ON CONFLICT clause to update the last_seen and first_seen columns if necessary. It then inserts the live station data into the bikesharing.station_status_temp table using the extras.execute_values function and a trigger to copy new data into the bikesharing.station_status table and update the preceding status in the bikesharing.station_status_temp_pre table.

Finally, the function returns the number of stations inserted."""
    n_stations = 0
    df_stations_raw.rename(columns={'uid':'id', 'countries.cities.uid':'city_id'}, inplace=True)
    df_stations_raw['time'] = scrape_time
    df_stations_raw['first_seen'] = df_stations_raw['time']
    if len(df_stations_raw):
        # real stations are those that have the "spot = True" attribute
        # spot = False implies bike = True --> virtual station of a free floating bike
        df_real_stations = df_stations_raw.loc[df_stations_raw.spot]

        insert_stations_static(cur, df_real_stations)

        # get live data - only data that changes with time!
        insert_stations_live(cur, df_real_stations)
        n_stations = len(df_real_stations)

    return n_stations


def insert_stations_live(cur, df_real_stations):
    df_real_stations_live = df_real_stations.loc[:,
                            ['id', 'time', 'booked_bikes', 'bikes', 'bikes_available_to_rent', 'free_racks',
                             'free_special_racks', 'maintenance']]
    df_real_stations_live.rename(columns={'id': 'station_id'}, inplace=True)
    # insert into live-data temp table. if data has changed --> update row
    # on update, a trigger copies updated rows to the main station_status table.
    # the temp table can be interpreted as "last known new status"
    query = f""" INSERT INTO bikesharing.station_status_temp ({','.join(df_real_stations_live.columns)})
        VALUES %s
        ON CONFLICT ON CONSTRAINT station_status_temp_pk DO UPDATE SET
        time = EXCLUDED.time,
        bikes = EXCLUDED.bikes,
        booked_bikes = EXCLUDED.booked_bikes,
        bikes_available_to_rent = EXCLUDED.bikes_available_to_rent,
        free_racks = EXCLUDED.free_racks,
        free_special_racks = EXCLUDED.free_special_racks,
        maintenance = EXCLUDED.maintenance
        WHERE
        (bikesharing.station_status_temp.bikes != EXCLUDED.bikes)
        or (bikesharing.station_status_temp.booked_bikes != EXCLUDED.booked_bikes)
        or (bikesharing.station_status_temp.bikes_available_to_rent != EXCLUDED.bikes_available_to_rent)
        or (bikesharing.station_status_temp.free_racks != EXCLUDED.free_racks)
        or (bikesharing.station_status_temp.free_special_racks != EXCLUDED.free_special_racks)
        or (bikesharing.station_status_temp.maintenance != EXCLUDED.maintenance);"""
    extras.execute_values(cur, query, df_real_stations_live.to_records(index=False), page_size=PAGE_SIZE)


def insert_stations_static(cur, df_real_stations):
    df_real_stations_static = df_real_stations.loc[:,
                              ['id', 'city_id', 'name', 'address', 'number', 'bike_racks', 'special_racks',
                               'terminal_type', 'place_type', 'first_seen', 'lng', 'lat']]
    # insert station static data
    # on conflict (station already known) --> update last/first seen
    query = f""" INSERT INTO bikesharing.stations ({','.join(df_real_stations_static.columns[:-2]) + ', geom'})
        VALUES %s
        ON CONFLICT ON CONSTRAINT stations_pkey
        DO update set last_seen = GREATEST(EXCLUDED.first_seen, bikesharing.stations.last_seen),
        first_seen = LEAST(EXCLUDED.first_seen, bikesharing.stations.first_seen)"""
    extras.execute_values(cur, query, df_real_stations_static.to_records(index=False),
                          template='(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, st_setsrid(st_point(%s, %s),4326))',
                          page_size=PAGE_SIZE)



def insert_bikes(df_bikes_raw, scrape_time, cur):
    """This function is used to insert data about bikes into a database. The function takes in three arguments:

df_bikes_raw: a dataframe containing raw data about bikes
scrape_time: a datetime object representing the time of the scrape
cur: a database cursor object
The function begins by adding a column to the dataframe containing the scrape time and renaming several columns to match the names of the corresponding columns in the database. It then separates the data into static data (data that does not change often) and live data (data that changes frequently).

Next, the function inserts the bike type data into the bikesharing.bike_types table using the extras.execute_values function and the ON CONFLICT clause to handle conflicts. It then inserts the static bike data into the bikesharing.bikes table using the ON CONFLICT clause to update the last_seen and first_seen columns if necessary.

Finally, the function inserts the live bike data into the bikesharing.bike_locations_temp table using the extras.execute_values function and a trigger to copy new data into the bikesharing.bike_locations table and update the preceding status in the bikesharing.bike_locations_temp_pre table. The function returns the number of rows inserted."""
    df_bikes_raw['time'] = scrape_time
    start_time = time.time()

    df_bikes_raw.rename(columns={'number': 'id',
                             'bike_type': 'bike_type_id',
                             'countries.cities.uid': 'city_id',
                             'countries.cities.places.lat': 'lat',
                             'countries.cities.places.lng': 'lng',
                             'countries.cities.places.spot': 'spot',
                             'countries.cities.places.uid': 'station_uid',}, inplace=True)

    df_bikes_raw['station_id'] = df_bikes_raw.apply(lambda x: x.station_uid if x.spot else None, axis=1)

    if len(df_bikes_raw):
        # 3.5 steps:
        # 1) bike type data (bike model)
        # 2) bike static data
        # 3a ) bike live data insert 1
        # 3b ) bike live data insert 2

        df_bikes_static = df_bikes_raw.loc[:,['id', 'city_id', 'bike_type_id', 'boardcomputer', 'time']]
        logging.debug(f'Preprocessing time: {time.time()-start_time}s')

        global bike_types
        if not df_bikes_static.bike_type_id.isin(bike_types).all():
            insert_bike_types(cur, df_bikes_raw)
            bike_types = df_bikes_static.bike_type_id.values
        logging.debug(f'bike type update time: {time.time()-start_time}s')
        insert_bikes_static(cur, df_bikes_static, start_time)

        df_bikes_live = insert_bikes_live(cur, df_bikes_raw, start_time)

        logging.debug(f'live bike data insert time: {time.time()-start_time}s')

        return len(df_bikes_live)


def insert_bikes_live(cur, df_bikes_raw, start_time):
    df_bikes_live = df_bikes_raw.loc[:, ['id', 'time', 'spot', 'station_id', 'pedelec_battery', 'lng', 'lat']]
    df_bikes_live.drop(columns=['spot'], inplace=True)
    df_bikes_live.rename(columns={'id': 'bike_id', }, inplace=True)
    logging.debug(f'live bike data processing time: {time.time() - start_time}s')
    # insert live data. (battery percentage, location, associated station)
    # inserted into bike_locations_temp ONLY IF it is different from previous entry
    # if it is different, a trigger copies it to bike_locations main table
    # if not --> discarded
    # the same trigger copies the last row from bike_locations_temp_pre
    # this is the preceding status regardless of new/old
    # thus bike_locations_temp_pre is only filled after first insert (--> it's old at every iteration ;)
    query = f"""
        INSERT INTO bikesharing.bike_locations_temp ({','.join(df_bikes_live.columns[:-2]) + ', geom'})
        VALUES %s
        ON CONFLICT ON CONSTRAINT bike_locations_temp_pk DO UPDATE SET
        time = EXCLUDED.time,
        station_id = EXCLUDED.station_id,
        geom = EXCLUDED.geom,
        pedelec_battery = EXCLUDED.pedelec_battery
        WHERE
        (st_distancesphere(bikesharing.bike_locations_temp.geom, EXCLUDED.geom) > 10)
        or (bikesharing.bike_locations_temp.station_id != EXCLUDED.station_id)
        or (bikesharing.bike_locations_temp.pedelec_battery != EXCLUDED.pedelec_battery);
        """
    extras.execute_values(cur, query, df_bikes_live.to_records(index=False),
                          template='(%s, %s, %s, %s, st_setsrid(st_point(%s, %s),4326))', page_size=PAGE_SIZE)
    query = f"""
        INSERT INTO bikesharing.bike_locations_temp_pre ({','.join(df_bikes_live.columns[:-2]) + ', geom'})
        VALUES %s
        ON CONFLICT ON CONSTRAINT bike_locations_temp_pre_pk DO UPDATE SET
        time = EXCLUDED.time,
        station_id = EXCLUDED.station_id,
        geom = EXCLUDED.geom,
        pedelec_battery = EXCLUDED.pedelec_battery
        """
    extras.execute_values(cur, query, df_bikes_live.to_records(index=False),
                          template='(%s, %s, %s, %s, st_setsrid(st_point(%s, %s),4326))', page_size=PAGE_SIZE)
    return df_bikes_live


def insert_bikes_static(cur, df_bikes_static, start_time):
    df_bikes_static.rename(columns={'time': 'first_seen'}, inplace=True)
    # static bike data insert. (assigned city, board computer id, bike type)
    # on conflict, do nothing. (formerly: update first/last seen. Produced to many WALs and slowed everything down massively)
    logging.debug(f'static bike data processing time :{time.time() - start_time}s')
    query = f""" INSERT INTO bikesharing.bikes ({','.join(df_bikes_static.columns)})
        VALUES %s
        ON CONFLICT ON CONSTRAINT bikes_pkey
        DO nothing"""
    extras.execute_values(cur, query, df_bikes_static.to_records(index=False), page_size=PAGE_SIZE)
    logging.debug(f'static bike data insert time:{time.time() - start_time}s')


def insert_bike_types(cur, df_bikes_raw):
    # insert bike types - there's no machine-readable information on bike types available yet, so only insert bike id
    df_bike_types = df_bikes_raw[['bike_type_id']].rename(columns={'bike_type_id': 'id'})
    query = """INSERT INTO bikesharing.bike_types (id)
        VALUES %s
        ON CONFLICT ON CONSTRAINT bike_types_pkey DO nothing;"""
    extras.execute_values(cur, query, df_bike_types.set_index('id').drop_duplicates().to_records(index=True),
                      page_size=PAGE_SIZE)


def process_city(file):
    """This function is used to process a file containing data about cities and insert the data into a database. The function takes in a single argument:

file: a string representing the file to be processed
The function begins by opening the file and reading its contents using the json module. It then uses the get_cities function to extract data about cities from the file contents and stores the resulting dataframe in df_cities.

Next, the function inserts the data from the dataframe into the database using the insert_cities function and logging.infos the number of cities inserted to the console. Finally, the function returns the dataframe containing the city data."""
    with myzip.open(file) as myfile:
        data = json.load(myfile)
        df_cities = get_cities(data)

        n_cities = insert_cities(df_cities)
        logging.info(f'Cities inserted into database: {n_cities}')
        return df_cities


def process_file(file, archive):
    """This function is used to process a file and insert its data into a database. The function takes in two arguments:

file: a string representing the file to be processed
archive: a string representing the archive that the file is from
The function begins by opening the file and reading its contents using the json module. It then extracts the timestamp of the file using the time_from_filename function.

Next, the function uses the pd.json_normalize function to extract data about stations and bikes from the file contents and stores the resulting dataframes in df_stations and df_bikes, respectively.

The function then establishes a connection to the database using psycopg2 and inserts the data from the dataframes into the database using the insert_stations and insert_bikes functions. Finally, the function inserts a record of the scrape into the bikesharing.scrapes table and commits the changes to the database."""
    with myzip.open(file) as myfile:
        try:
            data = json.load(myfile)
        except JSONDecodeError:
            logging.error('File not decodeable')
            return

        scrape_time = time_from_filename(file)
        logging.info(f'Processing data for time {scrape_time}.')
        # these commands took me a while - but they extract all bikes/stations from a scrape file :)
        df_cities = get_cities(data)
        df_stations = pd.json_normalize(data, record_path=['countries', 'cities', 'places'], meta=[['countries', 'cities','uid']])
        df_bikes = pd.json_normalize(data, record_path=['countries', 'cities', 'places','bike_list'], meta=[['countries', 'cities' ,'places', i] for i in ['lat', 'lng', 'uid', 'spot']] + [['countries', 'cities', 'uid']])

        global cities
        if not df_cities.id.isin(cities).all():
            n_cities = insert_cities(df_cities)
            logging.info(f'Found {n_cities} new cities: {df_cities.loc[~df_cities.id.isin(cities)]}')
            cities = df_cities.id.values

        n_bikes_dupl = len(df_bikes)
        n_stations_dupl = len(df_stations)

        df_bikes.drop_duplicates(subset=['number'], keep=False, inplace=True)
        df_stations.drop_duplicates(subset=['uid'], keep=False, inplace=True)

        with psycopg2.connect(database=db_name, user=db_username, password=db_password, host=db_host, port=db_port) as con:
            cur = con.cursor()
            cur.execute("SET timezone = 'utc'")

            inserted_stations = insert_stations(df_stations, scrape_time, cur)
            inserted_bikes = insert_bikes(df_bikes, scrape_time, cur)

            # last action - insert a litte protocol and commit the changes ONLY AFTERWARDS!!
            # if something fails / user aborts no unfinished data in DB ;)
            logging.info(f'Time: {scrape_time}, bikes: {inserted_bikes}, stations: {inserted_stations}')
            query = f""" INSERT INTO bikesharing.scrapes (time_scrape, time_insert, archive, file, host, inserted_stations, inserted_bikes, bike_id_duplicates, station_id_duplicates)
            VALUES (%s, now() , %s, %s, %s, %s, %s, %s, %s)"""
            cur.execute(query, (scrape_time, archive.split('/')[-1], file.split('/')[-1], upload_host, inserted_stations, inserted_bikes, n_bikes_dupl-len(df_bikes), n_stations_dupl-len(df_stations)))
            con.commit()
        con.close()


# definition of data to insert

# get already inserted data. (log in db table "scrapes")
df_scrapes = pd.read_sql('SELECT * FROM bikesharing.scrapes',
                         con=engine)

# list archives that can be mined
# don't filter zips as single jsons might miss from a large archive
# --> only filter json files for already inserted data
archives = [archive for archive in os.listdir(FOLDER_ZIP) if (archive.endswith('zip'))]
archives.sort()
logging.info(f'Uploading archives: {archives}')


# loop through zips chronologically
LOCKFILE = 'data_pipeline.lock'
if os.path.isfile(LOCKFILE):
    logging.warning('Aborting: Another instance of the data pipeline is still running / exited!')
else:
    with open(LOCKFILE, 'wt') as f:
        f.write(datetime.now().isoformat())

    for archive in archives:
        logging.info(f'Processing archive: {archive}')
        with zipfile.ZipFile(FOLDER_ZIP / archive) as myzip:
            json_list = [file for file in myzip.namelist() if (file.split('/')[-1] not in df_scrapes.file.unique()) and file.endswith('.json')]
            json_list.sort()
            for file_i in json_list:
                logging.info(f'Processing file: {file_i}')
                process_file(file_i, archive)
        logging.info(f'Completed archive: {archive}')
    logging.info(f'Finished: Insert of data complete until {datetime.now().isoformat()}')
    os.remove(LOCKFILE)
