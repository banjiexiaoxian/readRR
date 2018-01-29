# -*- coding: UTF-8 -*-
import pymysql
import numpy as np
import matplotlib.pyplot as plt
from datetime import date, datetime, timedelta
import pandas as pd



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
                    # 每5min数据写入到csv文件中
                    time = pd.to_datetime(time_now) # time_now是np.datetime64格式，没有strftime属性。还可以用TimeStamp转化
                    time = time.strftime('%Y-%m-%d-%H-%M-%S')
                    rr_df = pd.DataFrame(rr_list)
                    rr_df.to_csv('cls-%s.csv' % time, mode='w', index=False, encoding='utf-8', header=None)
                    j += min
                i += 1






# 连接设置
connection = pymysql.connect(host='120.77.210.240',
                             port=3306,
                             user='pku',
                             passwd='pkuims',
                             db='pku_heal',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

userName = '刘言灵'

# 时间段设置
hour_start = 13
min_start = 00

startTime = datetime(2018, 1, 17, hour_start, min_start, 00)
endTime = datetime(2018, 1, 17, 20, 00, 00)


# 执行sql语句
try:
    with connection.cursor() as cursor:
        # 执行sql语句，插入记录
        sql = "SELECT A.YongHuID,B.XingMing,A.CiShu,A.TianJianShiJian,C.RR\
                FROM (heal_app_data as A left JOIN heal_user as B ON A.YongHuID=B.YongHuID )\
                RIGHT OUTER JOIN heal_app_additional_data  as C on A.ID=C.FuID\
                WHERE B.XingMing LIKE %s AND A.TianJianShiJian BETWEEN %s AND %s "
        cursor.execute(sql, (userName, startTime, endTime))
        # 获取查询结果
        result = cursor.fetchall()
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    connection.commit()
    data_frame = pd.DataFrame(result, columns=['YongHuID', 'XingMing', 'CiShu', 'TianJianShiJian', 'RR'])
    data_group = data_frame.groupby(['YongHuID'])['CiShu', 'TianJianShiJian', 'RR']
    data_partition(data_group, min=5)

finally:
    connection.close()