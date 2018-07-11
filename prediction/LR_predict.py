__author__ = 'yzhu7, Julie the big circle'

import numpy as np
import os
import datetime
from sklearn import linear_model
from collections import defaultdict
import math
from datetime import timedelta

def train_LR_all(num_of_gids, selected_gids):
    hist_neighbor = []
    hist_history = []
    hist_test_neighbor = []
    hist_test_history = []
    lablefile = './prediction/trainY.csv'
    train_Y_all = np.loadtxt(lablefile,delimiter=',')
    for i in range(0,num_of_gids):
        if selected_gids[i] > 0:
            trainfile = './prediction/trainX/trainX_' + str(i) + '.csv'
            train_X_all = np.loadtxt(trainfile,delimiter=',')
            trainX = train_X_all
            if len(trainX) == 0:
                continue
            trainY = train_Y_all[i,3:]
            trainZ = np.concatenate((np.array([train_Y_all[i,2:-1]]).T,np.array([train_Y_all[i,1:-2]]).T,np.array([train_Y_all[i,:-3]]).T),axis=1)
            #print trainZ
            #print trainZ.shape
            print "-------------graph id:", i+1, "-----------------"
            print "---------  train by neighborhood -------"
            hist, hist_test = train_LR(trainX, trainY)
            if len(hist) == 1:
                continue


            hist_neighbor.append(hist.tolist())
            hist_test_neighbor.append(hist_test.tolist())
            print "---------  train by history -------"
            hist, hist_test = train_LR(trainZ, trainY)

            if len(hist) == 1:
                continue
            hist_history.append(hist.tolist())
            hist_test_history.append(hist_test.tolist())
        else:
            print "no data for this gid!"

    print "probability for absolute testing error in range [0,1),[1,2),[2,3),[3,4),[4,infinity):"
    print "---------  training histogram by neighbor -------"
    #print hist_neighbor
    print np.mean(np.array(hist_neighbor),axis = 0)
    print "---------  training histogram by history -------"
    #print hist_history
    print np.mean(np.array(hist_history),axis = 0)

    print "---------  testing histogram by neighbor -------"
    #print hist_neighbor
    print np.mean(np.array(hist_test_neighbor),axis = 0)
    print "---------  testing histogram by history -------"
    #print hist_history
    print np.mean(np.array(hist_test_history),axis = 0)


def train_LR(trainX, trainY):
    reg = linear_model.Ridge(alpha = .5)
    #trainX = trainX[:, (trainX != 0).sum(axis=0) >= 10] #remove columns with mostly zeros
    data_list = np.concatenate((trainX,np.array([trainY]).T),axis = 1)
    #data_list = np.array(data_list)
    data_list = data_list[~np.any(data_list == 0,axis=1)]
    if len(data_list) < 20:
        print "No enough data for training!"
        return [0],[0]
    print "data length for training and testing:", len(data_list)
    thresh = int(0.2*len(data_list))
    trainX = data_list[:0-thresh,:-1]
    trainY = data_list[:0-thresh,-1]
    testX = data_list[0-thresh:,:-1]
    testY = data_list[0-thresh:,-1]
    #print trainX[:10,:]
    #print trainY[:10]
    reg.fit(trainX,trainY)
    error = abs(reg.predict(trainX) - trainY)/trainY
    #print [error,trainY]
    error_test = abs(reg.predict(testX) - testY)/testY
    print "max train residuals:",sorted(error)[-10:]
    print "max test residuals:",sorted(error_test)[-10:]

    print "averaged training error:", np.mean(error)
    print "averaged testing error:", np.mean(error_test)
    #print "standard deviation:", np.sqrt(np.mean((reg.predict(trainX) - trainY)**2))
    print "averaged absolute error:", np.mean(abs(reg.predict(trainX) - trainY))
    print "averaged speed:", np.mean(trainY)
    hist, bin_edges = np.histogram(abs(reg.predict(trainX) - trainY), bins=[0,1,2,3,4,9999])
    hist_test, bin_edges = np.histogram(abs(reg.predict(testX) - testY), bins=[0,1,2,3,4,9999])
    print "probability for absolute training error in range [0,1),[1,2),[2,3),[3,4),[4,infinity):", hist/float(len(error))
    print "probability for absolute testing error in range [0,1),[1,2),[2,3),[3,4),[4,infinity):", hist_test/float(len(error_test))
    #print reg.coef_
    return hist/float(len(error)),hist_test/float(len(error_test))


def prepare_datasets(output_path, time_window,graph, num_of_roads):
    #gids = np.loadtxt(selected_gid,usecols=(0,),delimiter=' ')
    traffic_week = np.zeros((1440*7/time_window, num_of_roads)) #7 days a week
    traffic_week_count = np.zeros((1440*7/time_window, num_of_roads))

    train_X = defaultdict(list)
    train_Y = defaultdict(list)

    cnt = 0
    start_day = datetime.datetime.strptime('2016-06-01', "%Y-%m-%d")
    for day_delta in range(30):
        current_day = start_day + timedelta(days=day_delta)
        target_file = "GPS_"+ current_day.strftime("%Y_%m_%d")
        weekday =  datetime.date(current_day.year, current_day.month, current_day.day).weekday()
        print current_day.year, current_day.month, current_day.day, weekday
        speed_path = output_path + target_file +"/speed/"
        for t in range(1,1441, time_window):
            #merge data in the time window
            v = {}
            mid = [0] * num_of_roads
            mean = [0] * num_of_roads
            for i in range(0,num_of_roads):
                v[i] = []

            for delta in range(0,time_window):
                filename = speed_path + "minute_"+str(t + delta)+".txt"
                if os.path.exists(filename) and os.path.getsize(filename) > 0:
                    with open(filename) as speed_data:
                        for item in speed_data:
                            item = item.split(' ')
                            gid = int(item[0])
                            for j in range(0,int(item[1])):
                                v[gid - 1].append(float(item[2 + j]))

            for i in range(0,num_of_roads):
                #print V[i]
                if len(v[i]) > 0:
                    lst = sorted(v[i])
                    mid[i] = lst[int((len(lst)-2)/2)]
                    mean[i] = sum(lst)/len(lst)
                    traffic_week[int((t - 1)/time_window) + weekday * 1440/time_window,i] += mean[i]
                    traffic_week_count[int((t - 1)/time_window) + weekday * 1440/time_window,i] += 1
                train_Y[i].append(mean[i])
            #print traffic_week[int((t - 1)/time_window) - 1 + (weekday - 1) * 1440/time_window,:100]
            cnt += 1
            print int((t - 1)/time_window)  + weekday * 1440/time_window, cnt
    np.savetxt('./prediction/trainY_mean.csv',np.array(train_Y.values()),fmt='%.4f',delimiter=',')
    traffic_week_count[np.where(traffic_week_count == 0)] = -1
    final_traffic_week = traffic_week/traffic_week_count
    final_traffic_week[np.where(final_traffic_week < 0)] = 0
    np.savetxt('./prediction/traffic_week_mean.csv',np.transpose(final_traffic_week),fmt='%.4f',delimiter=',')

    #train_Y = np.loadtxt('./prediction/trainY.csv',delimiter=',')
    #cnt = len(train_Y[0])
    #train_X = defaultdict(list)
    """
    for i in range(0, num_of_roads):
        try:
            neighbors = graph.reverse().neighbors(str(i + 1))
            neighbors = [neighbors, str(i + 1)]
        except:
            neighbors = [str(i + 1)]
        for cnt_new in range(3,cnt):
            print i, cnt_new
            if gids[i] > 0:
                if len(neighbors) == 0:
                    continue
                else:
                    train_item = []
                    for n in neighbors:
                        if gids[int(n) - 1] > 0:
                            for j in range(1,4):
                                item = train_Y[int(n) - 1][cnt_new - j]
                                train_item.append(item)
                    train_X[i].append(train_item)
        np.savetxt('./prediction/trainX/trainX_' + str(i) + '.csv',np.array(train_X[i]),fmt='%.4f',delimiter=',')
        print './prediction/trainX/trainX_' + str(i) + '.csv'
    """
    #return train_X, train_Y, traffic_week/traffic_week_count



def filter_selected_gids():
    gids = np.loadtxt('./Roadnets/selected_gids2.csv',delimiter=',')
    u, indices = np.unique(gids[:,2], return_index=True)
    indices = sorted(indices)
    for i in range(0, len(indices) - 1):
        print i, indices[i], indices[i + 1]
        data = gids[indices[i]:indices[i+1],0]
        #print data
        if any(x > 0 for x in data):
            gids[indices[i]:indices[i+1],0] = [1] *(indices[i+1] - indices[i])
        #print gids[indices[i]:indices[i+1],0]

    np.savetxt('./Roadnets/selected_gids3.csv',gids,delimiter=',',fmt = '%d %d %d')