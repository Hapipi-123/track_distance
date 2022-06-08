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

#读取维表数据 根据id号排序 不然会错位 2在20下面这种情况导致异常
data_sql = '''
SELECT  id,city_code,name,line_id
        ,direction,order_no,distance   
FROM tmp_dm.tmp_ybl_bus_station_eta_realtime_model_t_station
ORDER BY id
'''
dframe = spark .sql(data_sql).toPandas()

def stations_gap_time(dframe,station_num,resource_embeddings,direction):
    for k in range(1,max(dframe['order_no'])):
        stop_order_name = dframe['name'].iloc[k-1]
        for j in range(k+1,max(dframe['order_no'])+1):
            next_stop_order_name = dframe['name'].iloc[j-1]
            #会出现有些station缺失，range是假设1~最大每个都存在的情况,--考虑去掉
            if (k in station_num) and (j in station_num): 
                sum_1 = 0
                for p in range(k+1,j+1):
                    sum_1 = sum_1 + dframe['distance'].iloc[p-1]
                resource_embeddings.append([citylist[m],linelist[n],direction,k,stop_order_name
                                            ,j,next_stop_order_name,sum_1
                                            ,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    return resource_embeddings

resource_embeddings =[]
citylist = dframe['city_code'].unique()
for m in range(citylist.shape[0]):
    data_city = dframe[dframe['city_code'] == citylist[m]]
    linelist = data_city['line_id'].unique()
    for n in range(linelist.shape[0]):
        data_city_line_code = data_city[data_city['line_id'] == linelist[n]]
        
        #方向0
        data_city_line_code_direction0 = data_city_line_code[data_city_line_code['direction']==0]
        if data_city_line_code_direction0.empty == 0:
            list = data_city_line_code_direction0["order_no"].values.tolist()
            ##不是环线的 每个direction的order_no=1只出现一次，环线会出现2次
            if dict(zip(*np.unique(list, return_counts=True)))[1] == 2 :
                station_num = data_city_line_code_direction0['order_no'].unique()
                stations_gap_time(data_city_line_code_direction0
                                  ,station_num,resource_embeddings,direction=0)
                
            #环线direction都为0 ，以order_no为1划分 顺时针还是逆时针
            else:
                #索引重置
                data_city_line_code_direction0 = data_city_line_code_direction0.reset_index(drop=True)
                index = data_city_line_code_direction0[data_city_line_code_direction0['order_no'] == 1].index.tolist()  
                data_city_line_code_direction0_1 = data_city_line_code_direction0.iloc[index[0]:index[1]]
                data_city_line_code_direction0_2 = data_city_line_code_direction0.iloc[index[1]:]
                
                
                station_num = data_city_line_code_direction0_1['order_no'].unique()
                stations_gap_time(data_city_line_code_direction0_1
                                  ,station_num,resource_embeddings,direction=0)
                
                station_num = data_city_line_code_direction0_2['order_no'].unique()
                stations_gap_time(data_city_line_code_direction0_2
                                  ,station_num,resource_embeddings,direction=0)   
                
                
        #方向1
        data_city_line_code_direction1 = data_city_line_code[data_city_line_code['direction']==1]
        if data_city_line_code_direction1.empty == 0:
            list = data_city_line_code_direction1["order_no"].values.tolist()
            ##不是环线的 每个direction的order_no=1只出现一次，环线会出现2次
            if dict(zip(*np.unique(list, return_counts=True)))[1] == 1 :
                station_num = data_city_line_code_direction1['order_no'].unique()
                stations_gap_time(data_city_line_code_direction1
                                  ,station_num,resource_embeddings,direction=1)

                    
resource_embeddings_dataframe=pd.DataFrame(resource_embeddings,columns=('city_code','line_code'
                               ,'direction', 'stop_order' ,'stop_order_name','next_stop_order'
                               ,'next_stop_order_name','distance_m','updatatime'))



# # # #写入hive表
spark.createDataFrame(resource_embeddings_dataframe).write.mode("overwrite").format("hive").saveAsTable('tmp_dm.tmp_xwj_bus_station_track_distance')

