__author__ = 'yzhu7'

import json
import psycopg2
from postgis import register, LineString
import os
import time
import datetime
import csv
import threading
import sys
import geojson


NUM_OF_SERVER = 7
Num_Of_threads = 50
CAR_NUM = 25000

def Run_Multi_Threading(file_path, PSQL_URI, table_name):
    threads = []
    for i in range(Num_Of_threads):
        conn = psycopg2.connect(PSQL_URI)
        cur = conn.cursor()
        new_thread = myThread(i, range(i*CAR_NUM/Num_Of_threads,(i+1)*CAR_NUM/Num_Of_threads),file_path, conn, cur, table_name)
        threads.append(new_thread)
    for th in threads:
        th.start()
    for t in threads:
        t.join()

class myThread(threading.Thread):
    def __init__(self, threadID, car_list, file_path, conn, cur, table_name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.car_list = car_list
        self.file_path = file_path
        self.conn = conn
        self.cur = cur
        self.table_name = table_name

    def run(self):
        print "my_thread_id:",self.threadID
        #conn = psycopg2.connect(PSQL_URI)
        #cur = conn.cursor()
        register(self.cur)
        for car_id in self.car_list:
            file_name = self.file_path + 'one_car_traj_' + str(car_id) + '.geojson'
            if os.path.exists(file_name):
                if car_id % 100 == 0:
                    print "car_id:", car_id,","
                with open(file_name) as f:
                    data = json.load(f)
                insert_data = []
                insert_string_block = "INSERT INTO " + table_name + \
                                "(unix_time, car_id, time_string, osm_id, gid, percent, unix_time_pre, gid_pre, pick_or_drop, speed,date, the_geom) " + \
                                            "VALUES "
                for feature in data['features']:
                    if len(feature['geometry']['coordinates']) > 1:
                        try:
                            dist = feature['properties']['Dist']
                        except:
                            continue
                        time_current = feature['properties']['ABSTIME']
                        time_current_string = feature['properties']['TIME']
                        osm_id = feature['properties']['OSM_ID']
                        gid_current = feature['properties']['GID']
                        percent_current = feature['properties']['Percent']
                        #percent_pre = feature['properties']['Percent_pre']
                        gid_pre = feature['properties']['GID_pre']
                        pick_or_drop = feature['properties']['Pick_or_drop']
                        time_pre = feature['properties']["ABS_TIME_PRE"]
                        coors = feature['geometry']['coordinates']
                        line_seg = []
                        for item in coors:
                            line_seg.append(tuple(item))

                        insert_string = "INSERT INTO " + table_name + \
                                "(unix_time, car_id, time_string, osm_id, gid, percent, unix_time_pre, gid_pre, pick_or_drop, speed,date, the_geom) " + \
                                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"


                        """
                        insert_data = (time_current,car_id,time_current_string,osm_id,gid_current,
                                       percent_current,time_pre,gid_pre,pick_or_drop,dist/(time_current - time_pre),date,
                                       LineString(line_seg, srid=4326))
                                       """
                        insert_data.append((time_current,car_id,time_current_string,osm_id,gid_current,
                                       percent_current,time_pre,gid_pre,pick_or_drop,dist/(time_current - time_pre),date,
                                       LineString(line_seg, srid=4326)))
                if len(insert_data) > 0:
                    try:
                        """
                        self.cur.execute(insert_string,insert_data)
                        self.conn.commit()
                        """
                        args_str = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in insert_data)
                        self.cur.execute(insert_string_block + args_str)
                        self.conn.commit()
                    except:
                        # Rollback in case there is any error
                        e, t, tb = sys.exc_info()
                        print "caught", e, t, tb
                        self.conn.rollback()

        print "Insert done! Thread id = ",self.threadID
        self.cur.close()
        self.conn.close()


def Get_PSQL_URI(server_id, db_name):
    if not (server_id < NUM_OF_SERVER and server_id >=0):
        print "server id error: pls input server_id = 0~6!"
    PSQL_URI = {}
    for i in range(0,NUM_OF_SERVER):
        if i == 6:
            PSQL_URI[i] = 'host=10.127.1.192 port=5432 dbname=' + db_name + ' user=yzhu7 password=zhuyixuan1234A'
        elif i == 5:
            PSQL_URI[i] = 'host=10.127.1.101 port=5432 dbname=' + db_name + ' user=yzhu7 password=zhuyixuan1234A'
        else:
            PSQL_URI[i] = 'host=10.127.1.16'+ str(i + 1) + ' port=5432 dbname=' + db_name + ' user=julie00' + str(i + 1) + ' password=zhuyixuan1234A'

    return PSQL_URI[server_id]

def Make_Table(PSQL_URI, table_name):
    conn = psycopg2.connect(PSQL_URI)
    cur = conn.cursor()
    create_table_string  = "CREATE TABLE " + table_name +"(" + \
                "unix_time BIGINT NOT NULL," + \
                "car_id INTEGER NOT NULL," + \
                "time_string TEXT, " + \
                "osm_id INTEGER, " + \
                "gid INTEGER, " + \
                "percent double precision, " + \
                "unix_time_pre BIGINT," + \
                "gid_pre INTEGER, " + \
                "percent_pre double precision, " + \
                "pick_or_drop INTEGER, " + \
                "speed double precision, " + \
                "date text, " + \
                "the_geom geometry(LineString,4326)) " + \
                ";"

    cur.execute(create_table_string)
    cur.close()
    conn.commit()
    conn.close()

def Drop_Table(PSQL_URI, table_name):
    conn = psycopg2.connect(PSQL_URI)
    cur = conn.cursor()
    drop_table_string  = "DROP TABLE if EXISTS " + table_name +";"
    cur.execute(drop_table_string)
    cur.close()
    conn.commit()
    conn.close()


def Create_Geo_Index(PSQL_URI, table_name):
    conn = psycopg2.connect(PSQL_URI)
    cur = conn.cursor()
    drop_pkey_string = "ALTER TABLE " + table_name + " DROP CONSTRAINT if exists " + table_name + "_pkey;"
    add_pkey_string = "ALTER TABLE " + table_name + " ADD PRIMARY KEY(unix_time, car_id);"
    drop_index_string = "DROP INDEX if exists idx_gist_" + table_name;
    create_index_string= "CREATE INDEX idx_gist_" + table_name + " ON " + table_name + " USING gist (the_geom);"
    cur.execute(drop_pkey_string);
    cur.execute(add_pkey_string);
    cur.execute(drop_index_string)
    cur.execute(create_index_string)
    cur.close()
    conn.commit()
    conn.close()

def Select_Region(PSQL_URI, table_name, gps_center, radius, ts_for_vis, time_window):
    conn = psycopg2.connect(PSQL_URI)
    cur = conn.cursor()
    select_string = "SELECT unix_time, car_id, osm_id, gid, unix_time_pre, gid_pre, pick_or_drop, speed, st_astext(the_geom) FROM " + \
                    table_name + \
                    " WHERE ST_DWithin(ST_GeomFromText('POINT(" + str(gps_center[0]) + " " + str(gps_center[1]) + \
                    ")',4326), the_geom ," + str(radius*1000) + "e-5) and (((unix_time_pre - " + str(ts_for_vis) + \
                    ") between 0 and " + str(time_window) + \
                    ") or ((unix_time  - " + str(ts_for_vis) +") between 0 and " + str(time_window) +")) order by unix_time_pre;"

    cur.execute(select_string)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

def Write_Geojson(data, output_file,CAR_NUM):
    #headers = ['unix_time', 'car_id', 'osm_id', 'gid', 'unix_time_pre', 'gid_pre', 'pick_or_drop', 'speed', 'linestring']
    features = {}
    car_id_tag = [0]*(CAR_NUM + 1)
    for item in data:
        car_id = item[1]
        if car_id_tag[car_id] == 0:
            features[car_id] = []
        car_id_tag[car_id] = 1
        points_in_line = []
        line = item[8]
        line = line.split('(')[1]
        line = line.split(')')[0]
        lst = line.split(',')
        for i in range(0,len(lst)):
            tmp = lst[i].split()
            points_in_line.append([float(tmp[0]),float(tmp[1])])
        time_string = datetime.datetime.utcfromtimestamp(item[0] + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
        features[car_id].append(geojson.Feature(geometry=geojson.LineString(points_in_line),
                                           properties={'ABSTIME':item[0],'TIME':time_string,'CAR_ID':item[1],
                                                       'OSM_ID': item[2], 'GID': item[3],
                                                       'GID_pre': item[5],
                                                       'Pick_or_drop':item[6], "ABS_TIME_PRE":item[4],"Speed":item[7]}))
    geom_in_geojson = geojson.FeatureCollection(features)
    with open(output_file, 'w') as o:
        geojson.dump(geom_in_geojson, o)

def Write_Geojson_Pre(data, output_file):
    #headers = ['unix_time', 'car_id', 'osm_id', 'gid', 'unix_time_pre', 'gid_pre', 'pick_or_drop', 'speed', 'linestring']
    features = []
    for item in data:
        points_in_line = []
        line = item[8]
        line = line.split('(')[1]
        line = line.split(')')[0]
        lst = line.split(',')
        for i in range(0,len(lst)):
            tmp = lst[i].split()
            points_in_line.append([float(tmp[0]),float(tmp[1])])
        time_string = datetime.datetime.utcfromtimestamp(item[0] + 3600*8).strftime('%Y-%m-%d %H:%M:%S')

        features.append(geojson.Feature(geometry=geojson.LineString(points_in_line),
                                           properties={'ABSTIME':item[0],'TIME':time_string,'CAR_ID':item[1],
                                                       'OSM_ID': item[2], 'GID': item[3],
                                                       'GID_pre': item[5],
                                                       'Pick_or_drop':item[6], "ABS_TIME_PRE":item[4],"Speed":item[7]}))
    geom_in_geojson = geojson.FeatureCollection(features)
    with open(output_file, 'w') as o:
        geojson.dump(geom_in_geojson, o)

def Write_Geojson_Crop_Pre(data, output_file, ts_for_vis, time_window):
    #headers = ['unix_time', 'car_id', 'osm_id', 'gid', 'unix_time_pre', 'gid_pre', 'pick_or_drop', 'speed', 'linestring']
    features = []
    for item in data:
        points_in_line = []
        time_pre = item[4]
        time_current = item[0]
        if time_current == time_pre:
            continue
        line = item[8]
        line = line.split('(')[1]
        line = line.split(')')[0]
        lst = line.split(',')
        for i in range(0,len(lst)):
            tmp = lst[i].split()
            points_in_line.append([float(tmp[0]),float(tmp[1])])
        start_index = 0
        end_index = len(points_in_line)
        if time_pre < ts_for_vis and time_current <= ts_for_vis + time_window:
            start_index = int((1 - (time_current - ts_for_vis) / float(time_current - time_pre))*end_index)
            time_pre = ts_for_vis
        elif time_pre >= ts_for_vis and time_current > ts_for_vis + time_window:
            end_index = int((ts_for_vis + time_window - time_pre) / float(time_current - time_pre)*end_index)
            time_current = ts_for_vis + time_window
        if len(range(start_index,end_index)) < 2:
            continue
        time_string = datetime.datetime.utcfromtimestamp(time_current + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
        features.append(geojson.Feature(geometry=geojson.LineString(points_in_line[start_index:end_index]),
                                           properties={'ABSTIME':time_current,'TIME':time_string,'CAR_ID':item[1],
                                                       'OSM_ID': item[2], 'GID': item[3],
                                                       'GID_pre': item[5],
                                                       'Pick_or_drop':item[6], "ABS_TIME_PRE":time_pre,"Speed":item[7]}))
    geom_in_geojson = geojson.FeatureCollection(features)
    with open(output_file, 'w') as o:
        geojson.dump(geom_in_geojson, o)


def Write_Geojson_Crop(data, output_file, CAR_NUM, ts_for_vis, time_window):
    #headers = ['unix_time', 'car_id', 'osm_id', 'gid', 'unix_time_pre', 'gid_pre', 'pick_or_drop', 'speed', 'linestring']
    features = {}
    car_id_tag = [0]*(CAR_NUM + 1)
    for item in data:
        points_in_line = []
        time_pre = item[4]
        time_current = item[0]
        if time_current == time_pre:
            continue
        line = item[8]
        line = line.split('(')[1]
        line = line.split(')')[0]
        lst = line.split(',')
        for i in range(0,len(lst)):
            tmp = lst[i].split()
            points_in_line.append([float(tmp[0]),float(tmp[1])])
        start_index = 0
        end_index = len(points_in_line)
        if time_pre < ts_for_vis and time_current <= ts_for_vis + time_window:
            start_index = int((1 - (time_current - ts_for_vis) / float(time_current - time_pre))*end_index)
            time_pre = ts_for_vis
            #end_index = int((1 - (time_current - ts_for_vis) / float(time_current - time_pre))*end_index)
            #time_current = ts_for_vis
        elif time_pre >= ts_for_vis and time_current > ts_for_vis + time_window:
            end_index = int((ts_for_vis + time_window - time_pre) / float(time_current - time_pre)*end_index)
            time_current = ts_for_vis + time_window
            #start_index = int((ts_for_vis + time_window - time_pre) / float(time_current - time_pre)*end_index)
            #time_pre = ts_for_vis + time_window

        if len(range(start_index,end_index)) < 2:
            continue
        car_id = item[1]
        if car_id_tag[car_id] == 0:
            features[car_id] = []
        car_id_tag[car_id] = 1
        time_string = datetime.datetime.utcfromtimestamp(time_current + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
        features[car_id].append(geojson.Feature(geometry=geojson.LineString(points_in_line[start_index:end_index]),
                                           properties={'ABSTIME':time_current,'TIME':time_string,'CAR_ID':item[1],
                                                       'OSM_ID': item[2], 'GID': item[3],
                                                       'GID_pre': item[5],
                                                       'Pick_or_drop':item[6], "ABS_TIME_PRE":time_pre,"Speed":item[7]}))
    geom_in_geojson = geojson.FeatureCollection(features)
    with open(output_file, 'w') as o:
        geojson.dump(geom_in_geojson, o)

if __name__ == "__main__":

    for day in range(1,2):
        date = '2016-06-' + str(day).zfill(2)
        file_path = './GPS_' + date[:4] +'_' + date[5:7] + '_' + date[-2:] + '/geojson_traj/'
        print file_path
        # select server_id
        server_id = 6
        table_name = "car_trajs_" + date[:4] +'_' + date[5:7] + '_' + date[-2:]
        #table_name = "car_trajs_test"
        PSQL_URI = Get_PSQL_URI(server_id, "demodb")
        #Drop_Table(PSQL_URI, table_name)
        #Make_Table(PSQL_URI, table_name)
        #Run_Multi_Threading(file_path, PSQL_URI, table_name)
        Create_Geo_Index(PSQL_URI, table_name)

    """
    gps_center = (114.099693, 22.567223)
    radius = 1 #unit: km
    hour = 9
    minute  = 20
    time_window = 20*60
    ts_for_vis = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())) + 3600*hour + minute*60
    start = time.time()
    data = Select_Region(PSQL_URI, table_name, gps_center, radius, ts_for_vis, time_window)
    end = time.time()
    print "time for data selection:", end - start
    if not os.path.exists('./region_files/'):
        os.makedirs('./region_files/')
    output_file = './region_files/' + date + '_traj_region_center_' + str(gps_center[0])\
                  + '_' + str(gps_center[1]) + '_' + str(radius) + 'km_' + str(hour) +'_' + str(minute) +'_' + str(time_window) + 's.geojson'
    start = time.time()
    #Write_Geojson(data, output_file, CAR_NUM)
    #Write_Geojson_Pre(data, output_file)
    Write_Geojson_Crop_Pre(data, output_file, ts_for_vis, time_window)
    #Write_Geojson_Crop(data, output_file, CAR_NUM, ts_for_vis, time_window)
    end = time.time()
    print "output time", end - start
    print " num results: ", len(data)
    """






