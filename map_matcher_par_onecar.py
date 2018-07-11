__auther__ = 'Julie, the big circle'


import sys
import psycopg2
import csv
import threading
import time
import numpy as np
import timeit

Num_Of_threads = 50
#ROAD_TABLE_NAME2 = 'shenzhen_main1234_roads_new'
ROAD_TABLE_NAME = 'ways'
SEARCH_RADIUS = 25
DIFF = 1
DEGREE_DIFF = '70'

#define multi-thread for database query
class myThread(threading.Thread):

    def __init__(self, conn, cur, threadID, data_to_deal, res):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conn = conn
        self.cur = cur
        self.data_to_deal = data_to_deal
        self.res = res

    def run(self):        # add your sql
        for record in self.data_to_deal:
            LAT = record[2]
            LONG = record[1]
            LAT_PRE = record[6]
            LONG_PRE = record[5]
            #osm_id, lat, long, dist = select_nearest_roads(PSQL_URI,ROAD_TABLE_NAME, SEARCH_RADIUS,record[2],record[1])
            # prepare a cursor object using cursor() method
            # psql string
            sql_string3 = "SELECT gid, ST_distance(ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326), ST_LineInterpolatePoint(ST_Transform(the_geom,4326)" + \
                    ", ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326)))) AS distance, ST_AsText(ST_LineInterpolatePoint(ST_Transform(the_geom,4326), ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT(" + str(LONG) + " " + str(LAT) + \
                    ")',4326)))) AS NewCoor," + \
                    "ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326)) as percent FROM " + \
                    ROAD_TABLE_NAME + \
                    " WHERE ST_DWithin(ST_GeomFromText('POINT(" + str(LONG) + " " + str(LAT) + \
                    ")',4326), ST_LineInterpolatePoint(ST_Transform(the_geom,4326), " + \
                    "ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT("  + str(LONG) + " " + str(LAT) + \
                    ")',4326))) ," + str(SEARCH_RADIUS) + "e-5) Order by distance LIMIT 5;"
            sql_string2 = "SELECT gid,ST_distance(ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                ")',4326), ST_LineInterpolatePoint(ST_Transform(the_geom,4326)" + \
                    ", ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326)))) AS distance, ST_AsText(ST_LineInterpolatePoint(ST_Transform(the_geom,4326), " + \
                    "ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326)))) AS NewCoor," + \
                    " ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326)) as percent FROM " + \
                    ROAD_TABLE_NAME + \
                    " WHERE ST_DWithin(ST_GeomFromText('POINT(" + LONG + " " + LAT + \
                    ")',4326), ST_LineInterpolatePoint(ST_Transform(the_geom,4326), " + \
                    "ST_LineLocatePoint(ST_Transform(the_geom,4326), ST_GeomFromText('POINT("  + LONG + " " + LAT + \
                    ")',4326))) ," + str(SEARCH_RADIUS) + "e-5) and ((one_way = 2 and " + \
                    "(abs(abs(degrees(ST_Azimuth(ST_Point(x1, y1), ST_Point(x2,y2)))-degrees(ST_Azimuth(ST_Point(" + \
                    LONG_PRE + "," + LAT_PRE +"), ST_Point(" + LONG + "," + LAT +")))) - 180) < " + DEGREE_DIFF + ")) or " + \
                    "(abs(degrees(ST_Azimuth(ST_Point(x1, y1), ST_Point(x2,y2)))-degrees(ST_Azimuth(ST_Point(" + \
                    LONG_PRE + "," + LAT_PRE +"), ST_Point(" + LONG + "," + LAT +")))) < " + DEGREE_DIFF + ") or " + \
                    "(abs(abs(degrees(ST_Azimuth(ST_Point(x1, y1), ST_Point(x2,y2)))-degrees(ST_Azimuth(ST_Point(" + \
                    LONG_PRE + "," + LAT_PRE +"), ST_Point(" + LONG + "," + LAT +")))) - 360) < " + DEGREE_DIFF + \
                    ")) ORDER BY distance ASC LIMIT 5;"
            data = []
            time_diff = int(record[7])
            #match road segments
            if time_diff < 300 and time_diff > 0: #and (abs(float(LAT) - float(LAT_PRE)) + abs(float(LONG) - float(LONG_PRE))) > 15e-5:
                self.cur.execute(sql_string2)
                data = self.cur.fetchall()
            #just match points
            """
            if time_diff >= 60 or time_diff <= 0 or len(data) == 0:
                self.cur.execute(sql_string2)
                data = self.cur.fetchall()"""
            #print data

            #if len(data) == 0:
                #gid, osm_id, lat, long, dist, one_way , degree , degree_diff, percent, source, target \
                    #= None, None,None, None, None, None, None, None, None, None, None
            record = list(record)[0:5]
            for k in range(0,len(data)):
                coor = data[k][2].split(' ')
                long = coor[0].split('(')[1]
                lat = coor[1].split(')')[0]
                gid = data[k][0]
                percent = data[k][3]

                record.append(gid)
                record.append(long)
                record.append(lat)
                record.append(percent)
            self.res.append(record)
        # print record
        #print 'Thread_ID:',self.threadID,'Record:', self.data_to_deal[-1]


def parse_csv_gps(PSQL_URI, FILE_NAME):
    res = []
    #calculate the temporal difference
    data_list = calculate_gps_diff(FILE_NAME,DIFF)
    #reshape the data
    rows_num = int(data_list.shape[0]/Num_Of_threads)
    cols_num = data_list.shape[1]
    data_list = data_list[0:rows_num*Num_Of_threads][:].\
        reshape([Num_Of_threads, rows_num,cols_num])
    start = timeit.default_timer()
    threads = []
    for i in range(Num_Of_threads):
        conn = psycopg2.connect(PSQL_URI)
        cur = conn.cursor()
        new_thread = myThread(conn, cur,i, data_list[i][:][:],res)
        threads.append(new_thread)

    for th in threads:
        th.start()

    for t in threads:
        t.join()

    stop = timeit.default_timer()
    print "time cost:", stop - start
    return res

def calculate_gps_diff(FILE_NAME, DIFF):
    data_list = []
    with open(FILE_NAME) as fin:
        reader = csv.reader(fin)
        data_list = list(reader)
        data_list = np.array(data_list)

        # remove duplicate gps
        a = data_list[:,1:3]
        b = np.ascontiguousarray(a).view(np.dtype((np.void, a.dtype.itemsize * a.shape[1])))
        _, indices = np.unique(b, return_index=True)

        data_list = data_list[indices,:]
        data_list = data_list[np.argsort(data_list[:,0].astype(int)),:]
        data_list_diff = data_list[DIFF:,0].astype(int) - data_list[:-DIFF,0].astype(int)
        data_list = np.concatenate((data_list[DIFF:,:],data_list[:-DIFF,1:3],np.array([data_list_diff]).T),axis = 1)
        data_list = np.array(data_list)
        data_list = np.delete(data_list,np.where(data_list[:,-1].astype(int)==0)[0],0)
        print 'Data ready!','data_length:',len(data_list)#,'\n preview \n', data_list[0:10,:]
        return data_list

def main(argv):
    PSQL_URI = 'host=10.127.1.106 port=5432 dbname=testdb user=yzhu7 password=zhuyixuan1234A'
    FILE_NAME = argv #'./Car/Car_B948R7.txt'
    print "file name:", FILE_NAME
    res = parse_csv_gps(PSQL_URI, FILE_NAME)
    res = sorted(res)
    #headers = ['ABS_TIME', 'LONG', 'LAT', 'TM', 'Occupy', 'OSM_ID', 'LONG_interp', 'LAT_interp', 'Dist(GPS)']
    """headers = ['ABS_TIME', 'LONG', 'LAT', 'TM', 'Occupy', 'LONG_PRE', 'LAT_PRE', 'TM_DIFF',
               'Graph_ID','OSM_ID','Source','Target','Percent',
               'LONG_interp', 'LAT_interp', 'Dist(GPS)','One_way' , 'Degree' , 'Degree_diff']"""
    headers = ['ABS_TIME', 'LONG', 'LAT','TIME','Occupy','INFO(GID,LONG,LAT,Percent)--top 5 ordered by distance' ]
    with open('./Car/NEWGPS_Mapmatching_'+FILE_NAME[-5]+'.txt','wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(headers)
        wr.writerows(res)


#if __name__ == '__main__':
    #sys.exit(main(sys.argv[1:]))

if __name__ == '__main__':
    #main('./Car-day1/Car_B550VX.txt')
    for i in range(1,6):
        filename = './Car/Car_' + str(i) + '.txt'
        main(filename)

