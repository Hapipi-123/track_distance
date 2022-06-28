import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import numpy as np
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import datetime
spark = SparkSession.builder \
    .appName("mirror_corder_day_all") \
    .config("hive.metastore.uris","thrift://hive-meta-marketth.hive.svc.datacloud.17usoft.com:9083") \
    .config("hive.metastore.local", "false") \
    .config("spark.io.compression.codec", "snappy") \
    .config("spark.sql.execution.arrow.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()



#stop_order不等于0的时候表示站点，stoporder的值为站点值，stoporder=0的时候表示不在站点。例：4~6之间的stoporder为4,0,0,0,0,0,0,5,0,0,0,0,0,6；把4~5之间stoporder=0的轨迹点之间的距离相加就是4~5之间的轨迹距离
#把dframe中所有stoporder不等于0的之间的距离相加
def station_step_track_distance(data):
    #补全最大站
    data_max_station = pd.DataFrame(data[-1:])
    data_max_station = data_max_station.reset_index(drop=True)
    data_max_station.loc[0,'order_no'] = max(data['order_no'].unique()) + 1
    data = data.append(data_max_station)
    sum0 = 0
    sum_distance = []
    #sum的值为toporder在4~5之间的stoporder = 0的和，每次加完要归0
    for p in range(0,data.shape[0]):
        if data['order_no'].iloc[p] == 0:
            sum0 = sum0 + data['distance'].iloc[p]
            sum_distance.append(sum0)
        else:
            sum0 = sum0 + data['distance'].iloc[p]
            sum_distance.append(sum0)
            sum0 = 0 
    data.insert(loc=len(data.columns), column='sumdistance', value=sum_distance)
    #去掉stoporder=0的 形式类似station表
    data = data[data['order_no'] > 0]
    data = data.drop(['distance'],axis = 1)
#     print(data)
#     print(data.shape)
#     print(type(data))
    return data



#读取维表数据 根据id号排序 不然会错位 2在20下面这种情况 导致异常;track没有nme，从station表里面获取
data_sql = '''
         SELECT  id ,city_code
             ,line_code AS line_id
             ,direction
             ,stop_order AS order_no
             ,distance 
         FROM base_tesubwayrealtime.t_station_track
         ORDER BY id
 '''
dframe = spark .sql(data_sql).toPandas()

station_step_distance = pd.DataFrame()

citylist = dframe['city_code'].unique()
for m in range(citylist.shape[0]):
    data_city = dframe[dframe['city_code'] == citylist[m]]
    linelist = data_city['line_id'].unique()
    for n in range(linelist.shape[0]):
        data_city_line_code = data_city[data_city['line_id'] == linelist[n]]
        
        #方向0
        data_city_line_code_direction0 = data_city_line_code[data_city_line_code['direction']==0]
        if data_city_line_code_direction0.empty == 0:
            station_step_distance = station_step_distance.append(station_step_track_distance(data_city_line_code_direction0))


        #方向1
        data_city_line_code_direction1 = data_city_line_code[data_city_line_code['direction']==1]
        if data_city_line_code_direction1.empty == 0:
            station_step_distance = station_step_distance.append(station_step_track_distance(data_city_line_code_direction1))


station_step_distance.rename(columns={"sumdistance": "distance"},inplace=True)



# # # #写入hive表
spark.createDataFrame(station_step_distance).write.mode("overwrite").format("hive").saveAsTable('tmp_dm.tmp_xwj_bus_station_accumulate_distance_track_table')


        
  