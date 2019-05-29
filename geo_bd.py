import psycopg2


def insert_station(name_channel, name_network, name_station, x=None, y=None, z=None, latitude=None, longitude=None,
                   dt=None, sens=None, freq_range=None, type='',
                   comment='NULL'):
    id_location = None
    id_property = None
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    cursor = conn.cursor()
    conn.autocommit = True
    cursor.execute(f'SELECT name_station , station_key '
                   f'FROM station '
                   f'WHERE to_tsvector(\'english\', name_station) @@ to_tsquery(\'english\', \'{name_station}\') and '
                   f'to_tsvector(\'english\', channel) @@ to_tsquery(\'english\', \'{name_channel}\') and '
                   f'to_tsvector(\'english\', network) @@ to_tsquery(\'english\', \'{name_network}\');')
    res = cursor.fetchall()
    if len(res) != 0:
        print(res)
        return res[0][1]
    if x and y and z and latitude and longitude:
        cursor.execute(f'insert into location (x,y,z,latitude,longitude)'
                       f'values ({x},{y},{z},{latitude}, {longitude});'
                       f'SELECT currval(pg_get_serial_sequence(\'location\',\'location_key\'));')
        id_location = int(cursor.fetchall()[0])
    if dt and sens and freq_range and type != 'NULL':
        cursor.execute(f'insert into property (dt, sens, freq_range, type)'
                       f'values ({dt},{sens}, {freq_range}, \'{type}\');'
                       f'SELECT currval(pg_get_serial_sequence(\'property\',\'property_key\'));')
        id_property = int(cursor.fetchall()[0])

    cursor.execute(
        f'INSERT INTO station (network, channel, name_station, property_key, location_key,setup_date,update_time, comment) '
        f'VALUES (\'{name_network}\', \'{name_channel}\', \'{name_station}\', {convert_value(id_property)}, '
        f'{convert_value(id_location)}, NULL, now(),\'{comment}\');'
        f'SELECT currval(pg_get_serial_sequence(\'station\',\'station_key\'));')
    res = cursor.fetchall()[0]
    cursor.close()
    print(res[0])

    return res[0]


def convert_value(value):
    if value is None or value == "NULL" or value == "?":
        return 'NULL'
    else:
        return value


def insert_earthquake(name_channel, name_network, name_station, arrival_time_value='NULL',
                      azimuth_value=None, distance_value=None, sign_value=None, level_value=None,
                      magnitude_value=None, seismic_date_value='NULL', intens_value=None, quality_value='NULL',
                      x=None, y=None, z=None, latitude=None, longitude=None, dt=None, sens=None, freq_range=None,
                      type='NULL', comment='NULL', phase_value='NULL'):
    station_key = insert_station(name_channel, name_network, name_station, x, y, z, latitude, longitude,
                                 dt, sens, freq_range, type, comment)
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    cursor = conn.cursor()
    conn.autocommit = True

    arrival_time_key = insert_table_arrival_time(station_key, arrival_time_value, cursor)
    azimuth_key = insert_table_azimuth(station_key, azimuth_value, cursor)
    distance_key = insert_table_distance(station_key, distance_value, cursor)
    intens_key = insert_table_intens(station_key, intens_value, cursor)
    level_key = insert_table_level(station_key, level_value, cursor)
    magnitude_key = insert_table_magnitude(station_key, magnitude_value, cursor)
    quality_key = insert_table_quality(quality_value, cursor)
    sign_key = insert_table_sign(station_key, sign_value, cursor)
    seismic_date_key = insert_table_seismic_date(station_key, seismic_date_value, cursor)
    phase_key = insert_table_phase(phase_value, cursor)

    cursor.execute(f'insert into earthquake (okigin_time, arrival_time_key,distance_key, azimuth_key,sign_key,'
                   f'level_key, magnitude_key, seismic_data_key, intens_key, quality_key, phase_key)'
                   f'values (NULL,{convert_value(arrival_time_key)},{convert_value(distance_key)},{convert_value(azimuth_key)},{convert_value(sign_key)},'
                   f'{convert_value(level_key)}, {convert_value(magnitude_key)},{convert_value(seismic_date_key)}, '
                   f'{convert_value(intens_key)}, {convert_value(quality_key)}, {convert_value(phase_key)});')
    cursor.close()


def update_location_in_station(name_station, x=None, y=None, z=None, latitude=None, longitude=None, comment='NULL'):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    cursor = conn.cursor()

    if x and y and z and latitude and longitude:
        cursor.execute(f'insert into location (x,y,z,latitude,longitude)'
                       f'values ({x},{y},{z},{latitude}, {longitude});'
                       f'SELECT currval(pg_get_serial_sequence(\'location\',\'location_key\'));')
        id_location = int(cursor.fetchall()[0])
    else:
        cursor.close()
        return
    cursor.execute(f'UPDATE station '
                   f'SET '
                   f'location_key = {id_location},'
                   f'update_time = now() '
                   f'FROM station '
                   f'WHERE'
                   f' station.name_station = \'{name_station}\';')
    cursor.close()


def clear_db():
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(f'TRUNCATE station CASCADE;')
        cursor.execute(f'TRUNCATE earthquake CASCADE;')
        cursor.execute(f'TRUNCATE phase CASCADE;')
        cursor.execute(f'TRUNCATE quality CASCADE;')


def find_key(name_table, name_value, name_key, value):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')

    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(f'SELECT {name_key} '
                       f'FROM {name_table} '
                       f'WHERE {name_value} = {value};')
        return cursor.fetchall()


def insert_table_arrival_time(station_key, arrival_time_value, cursor):
    arrival_time_key = None
    if arrival_time_value != 'NULL':
        cursor.execute(f'insert into arrival_time (value, station_key)'
                       f'values (\'{arrival_time_value}\',{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'arrival_time\',\'arrival_time_key\'));')
        arrival_time_key = cursor.fetchall()[0][0]
    return arrival_time_key


def insert_table_azimuth(station_key, azimuth_value, cursor):
    if azimuth_value is None:
        return None
    key = find_key('azimuth', 'value', 'azimuth_key', azimuth_value)
    if len(key) != 0:
        return key[0][0]
    cursor.execute(f'insert into azimuth (value, station_key)'
                       f'values ({convert_value(azimuth_value)},{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'azimuth\',\'azimuth_key\'));')
    azimuth_key = cursor.fetchall()[0][0]
    return azimuth_key


def insert_table_distance(station_key, distance_value, cursor):
    if distance_value is None:
        return None
    key = find_key('distance', 'value', 'distance_key', distance_value)
    if len(key) != 0:
        return key[0][0]
    distance_key = 0
    cursor.execute(f'insert into distance (value, station_key)'
                       f'values ({convert_value(distance_value)},{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'distance\',\'distance_key\'));')
    distance_key = cursor.fetchall()[0][0]
    return distance_key


def insert_table_intens(station_key, intens_value, cursor):
    if intens_value is None:
        return None
    key = find_key('intens', 'value', 'intens_key', intens_value)
    if len(key) != 0:
        return key[0][0]

    cursor.execute(f'insert into intens (value, station_key)'
                    f'values ({convert_value(intens_value)},{convert_value(station_key)});'
                    f'SELECT currval(pg_get_serial_sequence(\'intens\',\'intens_key\'));')
    key = cursor.fetchall()[0][0]
    return key


def insert_table_level(station_key, level_value, cursor):
    if level_value is None:
        return None
    key = find_key('level', 'value', 'level_key', level_value)
    if len(key) != 0:
        return key[0][0]
    cursor.execute(f'insert into level (value, station_key)'
                       f'values ({convert_value(level_value)},{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'level\',\'level_key\'));')
    key = cursor.fetchall()[0][0]
    return key


def insert_table_magnitude(station_key, magnitude_value, cursor):
    if magnitude_value is None:
        return None
    key = find_key('magnitude', 'value', 'magnitude_key', magnitude_value)
    if len(key) != 0:
        return key[0][0]

    cursor.execute(f'insert into magnitude (value, station_key)'
                       f'values ({convert_value(magnitude_value)},{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'magnitude\',\'magnitude_key\'));')
    key = cursor.fetchall[0][0]
    return key


def insert_table_quality(quality_value, cursor):
    if quality_value == 'NULL':
        return None

    key = find_key('quality', 'value', 'quality_key', f'\'quality_value\'')
    if len(key) != 0:
        return key[0][0]
    cursor.execute(f'insert into quality (value)'
                       f'values (\'{convert_value(quality_value)}\');'
                       f'SELECT currval(pg_get_serial_sequence(\'quality\',\'quality_key\'));')
    key = cursor.fetchall()[0][0]
    return key


def insert_table_sign(station_key, sign_value, cursor):
    if sign_value is None or '?':
        return None
    key = find_key('sign', 'value', 'sign_key', sign_value)
    if len(key) != 0:
        return key[0][0]
    cursor.execute(f'insert into sign (value, station_key)'
                       f'values ({convert_value(sign_value)},{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'sign\',\'sign_key\'));')
    key = cursor.fetchall()[0][0]
    return key


def insert_table_phase(phase_value, cursor):
    if phase_value is None:
        return None
    key = find_key('phase', 'value', 'phase_key', f'\'phase_value\'')
    if len(key) != 0:
        return key[0][0]
    cursor.execute(f'insert into phase (value)'
                       f'values (\'{convert_value(phase_value)}\');'
                       f'SELECT currval(pg_get_serial_sequence(\'phase\',\'phase_key\'));')
    key = cursor.fetchall()[0][0]
    return key


def insert_table_seismic_date(station_key, seismic_date_value, cursor):
    if seismic_date_value == 'NULL':
        return None
    key = find_key('seismic_date', 'value', 'seismic_date_key', seismic_date_value)
    if len(key) != 0:
        return key[0][0]

    cursor.execute(f'insert into seismic_date (value, station_key)'
                       f'values (\'{seismic_date_value}\',{convert_value(station_key)});'
                       f'SELECT currval(pg_get_serial_sequence(\'seismic_date\',\'seismic_date_key\'));')
    key = cursor.fetchall()[0][0]
    return key


def search_channel(name_channel):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select* '
                     f'from station '
                     f'WHERE to_tsvector(\'english\', channel) @@ to_tsquery(\'english\', \'{name_channel}\');')
        res = curs.fetchall()
    print(res)
    return res


def search_network(name_network):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select* '
                     f'from station '
                     f'WHERE to_tsvector(\'english\', network) @@ to_tsquery(\'english\', \'{name_network}\');')
        res = curs.fetchall()
    print(res)
    return res


def search_phase(phase_value):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select phase_value, arrival_value, level_value, quality_value,distance_value, azimuth_value '
                     f'from (select phase.value phase_value,arrival_time.value arrival_value,level.value level_value,'
                     f'q.value quality_value, d.value distance_value, a.value azimuth_value '
                     f'from earthquake join phase on '
                     f'earthquake.phase_key = phase.phase_key join arrival_time on '
                     f' earthquake.arrival_time_key = arrival_time.arrival_time_key '
                     f'join level on earthquake.level_key = level.level_key join quality q on earthquake.quality_key = q.quality_key '
                     f'join distance d on earthquake.distance_key = d.distance_key join azimuth a on earthquake.azimuth_key = a.azimuth_key ) new_table '
                     f'WHERE to_tsvector(\'english\', new_table.phase_value) @@ to_tsquery(\'english\', \'{phase_value}\');')
        res = curs.fetchall()
    print(res)
    return res


def search_date_interval(date1,date2):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select phase_value, arrival_value, level_value, quality_value,distance_value, azimuth_value '
                     f'from (select phase.value phase_value,arrival_time.value arrival_value,level.value level_value,'
                     f'q.value quality_value, d.value distance_value, a.value azimuth_value '
                     f'from earthquake join phase on '
                     f'earthquake.phase_key = phase.phase_key join arrival_time on '
                     f' earthquake.arrival_time_key = arrival_time.arrival_time_key '
                     f'join level on earthquake.level_key = level.level_key join quality q on earthquake.quality_key = q.quality_key '
                     f'join distance d on earthquake.distance_key = d.distance_key join azimuth a on earthquake.azimuth_key = a.azimuth_key ) new_table '
                     f'WHERE new_table.arrival_value BETWEEN \'{date1}\' AND \'{date2}\';')
        res = curs.fetchall()
    return res


def search_level_interval(val1, val2):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select phase_value, arrival_value, level_value, quality_value,distance_value, azimuth_value '
                     f'from (select phase.value phase_value,arrival_time.value arrival_value,level.value level_value,'
                     f'q.value quality_value, d.value distance_value, a.value azimuth_value '
                     f'from earthquake join phase on '
                     f'earthquake.phase_key = phase.phase_key join arrival_time on '
                     f' earthquake.arrival_time_key = arrival_time.arrival_time_key '
                     f'join level on earthquake.level_key = level.level_key join quality q on earthquake.quality_key = q.quality_key '
                     f'join distance d on earthquake.distance_key = d.distance_key join azimuth a on earthquake.azimuth_key = a.azimuth_key ) new_table '
                     f'WHERE new_table.level_value BETWEEN {val1} AND {val2};')
        res = curs.fetchall()
    return res


def search_distance_interval(val1, val2):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select phase_value, arrival_value, level_value, quality_value,distance_value, azimuth_value '
                     f'from (select phase.value phase_value,arrival_time.value arrival_value,level.value level_value,'
                     f'q.value quality_value, d.value distance_value, a.value azimuth_value '
                     f'from earthquake join phase on '
                     f'earthquake.phase_key = phase.phase_key join arrival_time on '
                     f' earthquake.arrival_time_key = arrival_time.arrival_time_key '
                     f'join level on earthquake.level_key = level.level_key join quality q on earthquake.quality_key = q.quality_key '
                     f'join distance d on earthquake.distance_key = d.distance_key join azimuth a on earthquake.azimuth_key = a.azimuth_key ) new_table '
                     f'WHERE new_table.distance_value BETWEEN {val1} AND {val2};')
        res = curs.fetchall()
    return res


def search_azimuth_interval(val1, val2):
    conn = psycopg2.connect(dbname='postgres', user='geo_user',
                            password='123', host='localhost,192.168.0.1', port='5432')
    with conn.cursor() as curs:
        conn.autocommit = True
        curs.execute(f'select phase_value, arrival_value, level_value, quality_value,distance_value, azimuth_value '
                     f'from (select phase.value phase_value,arrival_time.value arrival_value,level.value level_value,'
                     f'q.value quality_value, d.value distance_value, a.value azimuth_value '
                     f'from earthquake join phase on '
                     f'earthquake.phase_key = phase.phase_key join arrival_time on '
                     f' earthquake.arrival_time_key = arrival_time.arrival_time_key '
                     f'join level on earthquake.level_key = level.level_key join quality q on earthquake.quality_key = q.quality_key '
                     f'join distance d on earthquake.distance_key = d.distance_key join azimuth a on earthquake.azimuth_key = a.azimuth_key ) new_table '
                     f'WHERE new_table.azimuth_value BETWEEN {val1} AND {val2};')
        res = curs.fetchall()
    return res