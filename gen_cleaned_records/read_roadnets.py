#__author__ = 'yzhu7, Julie the big circle'

import numpy as np
import networkx as nx

def read_roadnets_from_file(filename1, filename2, filename3):
    mydata = np.loadtxt(filename1, delimiter=',',skiprows=1, usecols=(0,1,3,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,22))
    #print mydata[:10]
    #mydata: mydata[gid - 1][2] length_m; mydata[gid - 1][3] source; mydata[gid - 1][4] target; mydata[gid - 1][13] one_way;
    #mydata[gid - 1][16] osm_id

    mydata = mydata[np.argsort(mydata[:,0]),:]
    mydata_linestring = np.genfromtxt(filename2, delimiter= '(', skip_header=1, usecols=(1,),dtype='str')
    mydata_linestring_gid = np.genfromtxt(filename2, delimiter= ',',skip_header=1, usecols=(0,))
    mydata_linestring = mydata_linestring[np.argsort(mydata_linestring_gid)]

    # reverse one_way == -1
    for iter in range(1,len(mydata)):
        if mydata[iter][13] == -1:
            temp = mydata[iter][4]
            mydata[iter][4] = mydata[iter][3]
            mydata[iter][3] = temp

    # create graph for edges
    edges = np.loadtxt(filename3, delimiter = ',',dtype = np.int)
    gr = nx.DiGraph()
    cnt = 0
    for item in edges:
        if cnt % 10000 == 0:
            print item
            #print cnt
        gr.add_edge(str(item[0] + 1),str(item[1] + 1), weight= mydata[item[0]][2] + mydata[item[1]][2])
        cnt = cnt + 1
    return gr, mydata, mydata_linestring

# get the array of each road (gid 1 ~ 78296)
def get_roads_string(roadnet_data, roadnet_string):

    roads = {}
    cnt = 0
    for line in roadnet_string:
        roads[cnt] = []
        line = line.split(')')[0]
        lst = line.split(',')
        if roadnet_data[cnt][13] == -1:
            lst = lst[::-1]
        for i in range(0,len(lst)):
            tmp = lst[i].split()
            roads[cnt].append([float(tmp[0]),float(tmp[1])])
        cnt = cnt + 1
    print "Got roads string, done!"
    return roads

def write_matrix(filename1):
    mydata = np.loadtxt(filename1, delimiter=',',skiprows=1, usecols=(0,1,3,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,22))
    mydata = mydata[np.argsort(mydata[:,0]),:]

    # reverse oneway == -1
    for iter in range(1,len(mydata)):
        if mydata[iter][13] == -1:
            print iter + 1
            temp = mydata[iter][4]
            mydata[iter][4] = mydata[iter][3]
            mydata[iter][3] = temp


    fout = open('adjacency_matrix.txt',"wb")
    for i in range(0, len(mydata)):
        # mydata[i][3]: source, mydata[i][4]: target
        # for each road segment, calculate its next segment connected to the "target"
        target = mydata[i][4]
        temp_edges = np.delete(mydata[:,[0,3,4,13]],i,0)
        indices1 = np.where(temp_edges[:,1] == target)
        indices2 = np.where(temp_edges[:,3] == 2)
        indices3 = np.where(temp_edges[indices2,2][0] ==  target)
        if len(indices3[0]) == 0:
            indices = indices1[0]
        else:
            indices = np.unique(np.append(indices1,indices2[0][indices3][0]))
        #print indices
        for item in temp_edges[indices,0]:
            fout.write(str(i)+","+str(int(item - 1)) + "\n")
        # one_way ==  2, calculate the next segment connected to the "source"
        if mydata[i][13] ==  2:
            target = mydata[i][3]
            indices1 = np.where(temp_edges[:,1] == target)
            indices2 = np.where(temp_edges[:,3] == 2)
            indices3 = np.where(temp_edges[indices2,2][0] ==  target)
            if len(indices3[0]) == 0:
                indices = indices1[0]
            else:
                indices = np.unique(np.append(indices1,indices2[0][indices3][0]))
            for item in temp_edges[indices,0]:
                fout.write(str(i)+","+str(int(item - 1)) + "\n")
            #print indices
        if i%10000 == 0:
            print i, int(item - 1)
    fout.close()