import pymysql
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import date, datetime, timedelta
from math import floor


def PoincarePlot(time, data):
    data = np.array(data)
    # data = data[data < 400]
    # data = data[data > 0]
    data_i = data[0:len(data) - 1]
    data_j = data[1:len(data)]
    plt.plot(data_i, data_j, 'o', markerfacecolor='k', markeredgecolor='k', markersize=3,
             )
    plt.title(time)
    # plt.xlabel('pRRx', fontsize=15)
    # plt.ylabel('pRRx+1', fontsize=15)
    plt.axis("equal")
    plt.xlim(0, 2500)
    plt.ylim(0, 2500)
    # plt.plot(data,'o', markerfacecolor='k', markeredgecolor='k', markersize=3)
    # plt.ylim(0.3, 2)
    plt.show()


def data_partition(data_group, min=5):
    for level, subsetdf in data_group:
        print(level)
        cishu_perID_list = subsetdf['CiShu'].unique()
        i = 0
        while i < len(cishu_perID_list):
            cishu_tmp = cishu_perID_list[i]
            tianjia_percishu_list = subsetdf[subsetdf.CiShu == cishu_tmp]['TianJianShiJian'].unique()
            ##tianjia_percishu_list虽然得到的是对应某一次数的添加时间列表，但是列表的时间间隔并不一定是1min左右，有的相差好几分钟。
            if len(tianjia_percishu_list) < min:
                i += 1
                continue
            else:
                j = 0
                length = len(tianjia_percishu_list)
                threshold = int(length / min) * min  ##阈值写法要注意，否则数据分割得不准确
                while j < threshold:
                    rr_list = []
                    time_now = tianjia_percishu_list[j]  ##取5min开始的时间
                    for num in range(min):
                        # print(type(tianjia_percishu_list[0]))
                        # print(type(subsetdf.TianJianShiJian[0]))
                        # print(subsetdf[subsetdf.TianJianShiJian == tianjia_percishu_list[0]])
                        data = subsetdf[subsetdf.TianJianShiJian == pd.Timestamp(tianjia_percishu_list[j + num])]['RR']  ##这里得到的数据是个list
                        for k in data:  ## 循环取数，使得到的RR_list不含list
                            rr_list.append(k)
                    # 计算每5min的pRRx序列
                    min_pRR = 0
                    max_pRR = 100
                    ## 阶梯
                    step = 1
                    pRRx_list_step = [0]*(floor(max_pRR/step)+1)
                    ## 连续
                    pRRx_list = [0]*(max_pRR + 1)
                    for index in range(len(rr_list)-1):
                        delta = int(abs(rr_list[index+1]-rr_list[index]))
                        if delta >= max_pRR:
                            delta = max_pRR
                        pRRx_list_step[floor(delta / step)]+=1
                        for index2 in range(1,delta):
                            pRRx_list[index2]+=1
                    time = pd.to_datetime(time_now) # time_now是np.datetime64格式，没有strftime属性。还可以用TimeStamp转化
                    time = time.strftime('%Y-%m-%d-%H-%M-%S')
                    # rr_df = pd.DataFrame(rr_list)
                    # rr_df.to_csv('cls-%s.csv' % time, mode='w', index=False, encoding='utf-8', header=None)
                    #先看阶梯状
                    PoincarePlot(time,pRRx_list)
                    j += min
                i += 1
startTime =  datetime(2018, 1, 3, 8, 00, 00)
endTime = datetime(2018, 1, 3, 22, 59, 59)
userName = '阎主任'
#Connect to the database
connection = pymysql.connect(host='120.77.210.240',
                             port=3306,
                             user='pku',
                             passwd='pkuims',
                             db='pku_heal',
                             charset = 'utf8mb4',
                             cursorclass = pymysql.cursors.DictCursor)



# 执行sql语句
try:
    with connection.cursor() as cursor:
        # 执行sql语句，插入记录
        sql = "SELECT A.YongHuID,B.XingMing,A.CiShu,A.TianJianShiJian,C.RR\
                FROM (heal_app_data as A left JOIN heal_user as B ON A.YongHuID=B.YongHuID )\
                RIGHT OUTER JOIN heal_app_additional_data  as C on A.ID=C.FuID\
                WHERE B.XingMing LIKE %s AND A.TianJianShiJian BETWEEN %s AND %s "
        cursor.execute(sql, (userName,startTime, endTime))
        # 获取查询结果
        result = cursor.fetchall()
        data_frame = pd.DataFrame(result, columns=['YongHuID', 'XingMing', 'CiShu', 'TianJianShiJian', 'RR'])
        data_group = data_frame.groupby(['YongHuID'])['CiShu', 'TianJianShiJian', 'RR']
        data_partition(data_group, min=30)
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()

finally:
    connection.close()