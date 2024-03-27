import re
import numpy as np
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
from nostril import generate_nonsense_detector
from prometheus_api_client.utils import parse_datetime, parse_timedelta
from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame, PrometheusApiClientException

def scrapping(url:str, start_time:str, end_time:str, chunk_size:str):
    print("===============================================================================")
    print("Prometheus metric data Scrapping start..")

    prom = PrometheusConnect(url = url,disable_ssl=True)
    start_time = parse_datetime(start_time)
    end_time = parse_datetime(end_time)
    chunk_size = parse_timedelta(end_time, chunk_size)
    f =  open("./error_variable.txt",'w')

    # import avaliable metric's name
    metrics = prom.all_metrics()
    cpu_memory=[]
    for metric in metrics:
        if 'cpu' in metric or 'memory' in metric:
            cpu_memory.append(metric)
    metric_list = []

    # call data from prometheus
    for name in cpu_memory:
        try:
            metric_data = prom.get_metric_range_data(
                metric_name=name,
                start_time=start_time,
                end_time=end_time,
                chunk_size=chunk_size)
            metric_df = MetricSnapshotDataFrame(metric_data)

            if 'pod' in metric_df.columns:
                var_name = re.sub('[-=+,#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·]', '_',name)
                metric_df = metric_df.rename(columns={'value':var_name})
                metric_df = metric_df.loc[:,['timestamp','pod',var_name]]
                metric_list.append(metric_df)

        except PrometheusApiClientException:
            f.write(name)
            pass
    f.close()
    print("Scrapping success!")
    print("===============================================================================")
    return metric_list

def preprocessing(metric_list:list, chunk_size:str , method:str):
    print("preprocessing start...")
    core_df = pd.concat(metric_list,axis=0)
            
    # timestamp to yy-mm-dd format
    core_df['timestamp']=pd.to_datetime(core_df['timestamp'],unit='s') 
    # round time by set unit
    core_df['timestamp'] = core_df['timestamp'].apply(lambda x: x.round(freq=chunk_size)) 
    # replace na value for groupby statistics
    core_df['pod'] = core_df['pod'].fillna(0) 
    # # remove metric that has no pod name
    core_df = core_df[core_df['pod']!=0] 

    # pod name integration
    core_df['label'] = core_df['pod'].apply(lambda x: 1 if x.split('-')[-1].isalpha() == False else 0)
    num_df = core_df[core_df['label']==1]
    alpha_df = core_df[core_df['label']==0]

    nonsense_detector = generate_nonsense_detector(min_length=4, min_score=4.2)
    num_df['pod'] = num_df.apply(lambda x: x['pod'].replace('-' + x['pod'].split('-')[-1],''), axis=1)        
    alpha_df['pod'] = alpha_df.apply(lambda x: x['pod'].replace('-' + x['pod'].split('-')[-1],'') if nonsense_detector(x['pod'].split('-')[-1]) else x['pod'], axis=1)    
    
    core_df = pd.concat([num_df,alpha_df],axis=0)

    # integration value at same time
    core_df_groupby = core_df.groupby(['timestamp','pod'])
    if method == 'sum':
        total_df = core_df_groupby[core_df.columns[2:]].sum().reset_index(drop=False)    
    elif method == 'mean':
        total_df = core_df_groupby[core_df.columns[2:]].mean().reset_index(drop=False)
    elif method == 'median':
        total_df = core_df_groupby[core_df.columns[2:]].median().reset_index(drop=False)

    print('preprocessing success!')
    print("===============================================================================")
    return total_df

def pod_separate(total_df:pd.DataFrame,conn:create_engine):
    pod_list = total_df['pod'].unique()
    print('pod separate start...')
    for pod in pod_list:
        tmp = total_df[total_df['pod']==pod]
        tmp = tmp.sort_values(by=['timestamp'],axis=0).reset_index(drop=True)
        tmp = tmp.drop(['pod'],axis=1)
        drop_list=[]
        for i in range(len(tmp.columns[1:])):
            if tmp.describe().iloc[-1,i]==np.nan:                
                drop_list.append(tmp.columns[i+1])
        drop_list = list(set(drop_list))
        tmp = tmp.drop(drop_list,axis=1)
        tmp = tmp.fillna(1e-9)

        # change columns for db policy
        columns = [re.sub('container','co',re.sub('namespace','ns',x)) for x in tmp.columns]        
        tmp.columns=columns
        
        # insert DB
        table_name = re.sub('[-=+,#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·]', '_',pod)
        tmp.to_sql(name=table_name, con=conn,if_exists='replace',index=False)
        print("Table " +table_name+" insert success.")
    print("===============================================================================")
    print("current table list: ")
    print(conn.engine.table_names())
