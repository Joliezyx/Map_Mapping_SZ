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
from datetime import timedelta


NUM_OF_SERVER = 7
Num_Of_threads = 36
TIME_WINDOW = 20
ROADNET_PATH = "./Roadnets/"
FILE_NAME_ROADNETS_SEG = ROADNET_PATH + 'roadnet_segments.csv'
FILE_NAME_ROADNETS = ROADNET_PATH +'roadnet1.csv'
ADJ_MATRIX = ROADNET_PATH +'adjacency_matrix1.txt'

def Run_Multi_Threading(file_path, PSQL_URI, table_name, zero_oclock):
    threads = []
    for i in range(Num_Of_threads):
        conn = psycopg2.connect(PSQL_URI)
        cur = conn.cursor()
        new_thread = myThread(i, range(i*TIME_WINDOW*2 + 1,(i+1)*TIME_WINDOW*2 + 1,20),file_path, conn, cur, table_name, zero_oclock)
        threads.append(new_thread)
    for th in threads:
        th.start()
    for t in threads:
        t.join()

class myThread(threading.Thread):
    def __init__(self, threadID, time_start_list, file_path, conn, cur, table_name, zero_oclock):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.time_start_list = time_start_list
        self.file_path = file_path
        self.conn = conn
        self.cur = cur
        self.table_name = table_name
        self.zero_oclock = zero_oclock

    def run(self):
        print "my_thread_id:",self.threadID
        #conn = psycopg2.connect(PSQL_URI)
        #cur = conn.cursor()
        register(self.cur)
        for time_start in self.time_start_list:
            file_name = self.file_path + 'minute_' + str(time_start) + '.geojson'
            if os.path.exists(file_name):
                with open(file_name) as f:
                    data = json.load(f)
                insert_data = []
                insert_string_block = "INSERT INTO " + table_name + \
                                "(gid, unix_time_start, interval_in_second, speed_median, speed_mean, car_cnt_20mins, class) " + \
                                            "VALUES "
                uinx_time_start = zero_oclock + (time_start - 1)*60
                for feature in data['features']:
                    if len(feature['geometry']['coordinates']) > 1:
                        gid = feature['properties']['gid']
                        speed_median = feature['properties']['median_speed']
                        speed_mean = feature['properties']['mean_speed']
                        car_cnt = feature['properties']['car_cnt']
                        class_osm = int(float((feature['properties']['class'])))

                        insert_data.append((gid, uinx_time_start, 60*TIME_WINDOW, speed_median, speed_mean, car_cnt, class_osm))
                if len(insert_data) > 0:
                    try:
                        """
                        self.cur.execute(insert_string,insert_data)
                        self.conn.commit()
                        """
                        args_str = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x) for x in insert_data)
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
        print "server id error: pls input server_id = 0~5!"
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
                "gid INTEGER NOT NULL, " + \
                "unix_time_start BIGINT NOT NULL," + \
                "interval_in_second INTEGER NOT NULL, " + \
                "speed_mean double precision, " + \
                "speed_median double precision, " + \
                "car_cnt_20mins INTEGER, " + \
                "class INTEGER, " + \
                "PRIMARY KEY (gid, unix_time_start, interval_in_second), " + \
                "FOREIGN KEY (gid) REFERENCES ways (gid))" + \
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
    #add_pkey_string = "ALTER TABLE " + table_name + " ADD PRIMARY KEY(unix_time, car_id);"
    drop_index_string = "DROP INDEX if exists idx_gist_" + table_name;
    create_index_string= "CREATE INDEX idx_gid_" + table_name + " ON " + table_name + " USING btree (gid);" + \
        "CREATE INDEX idx_unix_time_start_" + table_name + " ON " + table_name + " USING btree (unix_time_start);" + \
        "CREATE INDEX idx_interval_in_second_" + table_name + " ON " + table_name + " USING btree (interval_in_second);"
    cur.execute(drop_pkey_string);
   # cur.execute(add_pkey_string);
    cur.execute(drop_index_string)
    cur.execute(create_index_string)
    cur.close()
    conn.commit()
    conn.close()

def Select_Region(PSQL_URI, table_name, gps_center, radius, ts_for_vis, time_window):
    conn = psycopg2.connect(PSQL_URI)
    cur = conn.cursor()
    select_string = " select a.*, b.priority, st_astext(b.the_geom) from " + table_name + \
                    " a INNER JOIN ways as b on a.gid = b.gid " + \
                    " WHERE ST_DWithin(ST_GeomFromText('POINT(" + str(gps_center[0]) + " " + str(gps_center[1]) + \
                    ")',4326), b.the_geom ," + str(radius*1000) + "e-5) and a.unix_time_start = " + str(ts_for_vis) + \
                    " and a.interval_in_second = " + str(time_window*60) +";"

    cur.execute(select_string)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

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
        time_string = datetime.datetime.utcfromtimestamp(item[1] + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
        if item[4] > 6: # or mid == 0 or isnan(mid):
            stroke = '#00FF00'
        elif item[4] > 3:
            stroke = '#FFFF00'
        elif item[4] > 0:
            stroke = '#FF0000'
        features.append(geojson.Feature(geometry=geojson.LineString(points_in_line),
                                           properties={'TIME':time_string,'CAR_CNT':item[5],
                                                       'Priority': item[7],'Gid': item[0],'stroke':stroke,
                                                       'Class':item[6], "Speed":item[4]}))
    geom_in_geojson = geojson.FeatureCollection(features)
    with open(output_file, 'w') as o:
        geojson.dump(geom_in_geojson, o)

if __name__ == "__main__":
    # select server_id
    server_id = 6
    table_name = "traffic_speed"
    PSQL_URI = Get_PSQL_URI(server_id, "demodb")
    """
    Drop_Table(PSQL_URI, table_name)
    Make_Table(PSQL_URI, table_name)
    for month in range(6,0,-1):
        start_date = '2016-0' + str(month) + '-01'
        start_day = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        cmds = []
        for i in range(31):
            current_day = start_day + timedelta(days=i)
            file_path = "./GPS_"+ (start_day + timedelta(days=i)).strftime("%Y_%m_%d") + '/geojson/'
            print file_path
            zero_oclock = int(time.mktime(current_day.timetuple()))
            Run_Multi_Threading(file_path, PSQL_URI, table_name, zero_oclock)
        Create_Geo_Index(PSQL_URI, table_name)
    """
    """
    gps_center = (114.099693, 22.567223)
    radius = 1 #unit: km
    hour = 9
    minute  = 20
    time_window = 20
    ts_for_vis = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())) + 3600*hour + minute*60
    start = time.time()
    data = Select_Region(PSQL_URI, table_name, gps_center, radius, ts_for_vis, time_window)
    end = time.time()
    print "time for data selection:", end - start
    if not os.path.exists('./region_files/'):
        os.makedirs('./region_files/')
    output_file = './region_files/' + date + '_speed_region_center_' + str(gps_center[0])\
                  + '_' + str(gps_center[1]) + '_' + str(radius) + 'km_' + str(hour) +'_' + str(minute) +'_' + str(time_window) + 's.geojson'
    start = time.time()
    Write_Geojson_Pre(data, output_file)
    end = time.time()
    print "output time", end - start
    print " num results: ", len(data)


    """





