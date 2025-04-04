import re
import pymysql
import numpy as np
import pandas as pd

from datetime import timedelta
from typing import TYPE_CHECKING
from sqlalchemy import create_engine
from prometheus_api_client.utils import parse_datetime, parse_timedelta
from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame, PrometheusApiClientException

# temporary import DBConfig from scheduler.py
if TYPE_CHECKING:
    from scheduler import DBConfig


def scrapping(url:str, start_time:str, end_time:str, scrap_size:str):
    print("===============================================================================")
    print("Prometheus metric data Scrapping start..")

    prom = PrometheusConnect(url = url,disable_ssl=True)
    start_time = parse_datetime(start_time)
    end_time = parse_datetime(end_time)
    scrap_unit = parse_timedelta(end_time, scrap_size)
    f =  open("./error_variable.txt",'w')

    # import avaliable metric's name
    metric_label = 'kube_pod_labels'
    label_data= prom.get_metric_range_data(
        metric_name=metric_label,
        start_time=start_time,
        end_time=end_time,
        chunk_size=scrap_unit)
    label_df = MetricSnapshotDataFrame(label_data)
    ml_workload_df = label_df[label_df['label_ml_workload'].isnull()!=True][['label_ml_workload','pod']]
    grouped_df = ml_workload_df.groupby('label_ml_workload')
    each_label_data = []  # define each_label_data out of for loop
    f =  open("./error_variable.txt",'w')
    
    node_per_cpu = 'machine_cpu_cores'
    cpu_core_data = prom.get_metric_range_data(
        metric_name=node_per_cpu,
        start_time=start_time,
        end_time=end_time,
        chunk_size=timedelta(days=1))
    cpu_df = MetricSnapshotDataFrame(cpu_core_data)
    cpu_core_df = cpu_df[['node','value']].drop_duplicates().sort_values(by=['node'],axis=0)
    cpu_core_df.rename(columns = {'value' : 'cpu_core'}, inplace=True)
    
    for label, group in grouped_df:
        ml_pod_list = list(set(group['pod']))
        ml_namespace = "'argo-test'"

        db_dict = {"label": label}  # new dict generate for each label
        df_list = []  # new df_list generate for each label

        for ml_pod in ml_pod_list:
            ml_pod = f"'{ml_pod}'"
            # print(ml_pod)
            query_list = [
                # / 100, cpu unit : Core
                'sum(rate(container_cpu_usage_seconds_total{namespace='+ml_namespace+', pod=~'+ml_pod+'}['+scrap_size+'])) by (pod, node) / 100', 
                'sum(container_memory_usage_bytes{namespace='+ml_namespace+', pod=~'+ml_pod+'}) by (pod) / 1e+6', # memory unit : Megabytes
                'sum(rate(container_network_transmit_bytes_total{namespace='+ml_namespace+', pod=~'+ml_pod+'}['+scrap_size+'])) by (pod)',
                'sum(rate(container_network_receive_bytes_total{namespace='+ml_namespace+', pod=~'+ml_pod+'}['+scrap_size+'])) by (pod)',
                'sum(rate(container_fs_reads_bytes_total{namespace='+ml_namespace+', pod=~'+ml_pod+'}['+scrap_size+'])) by (pod)',
                'sum(rate(container_fs_writes_bytes_total{namespace='+ml_namespace+', pod=~'+ml_pod+'}['+scrap_size+'])) by (pod)'
            ]
            metric_list = ['cpu_usage', 'memory_usage', 'network_transmit', 'network_receive', 'file_system_reads', 'file_system_writes']


            for query in query_list:
                try:
                    # print("query : ", query)
                    query_data = prom.custom_query_range(query=query,
                                                        start_time=start_time,
                                                        end_time=end_time,
                                                        step=scrap_size)

                    if len(query_data) != 0:
                        var_name = metric_list[query_list.index(query)]                        
                        query_df = pd.DataFrame(query_data[0]['values'], columns=['timestamp', var_name])
                        query_df[var_name] = query_df[var_name].astype(float)
                        query_df['pod'] = ml_pod.replace("'",'')
                        if query.startswith('sum(rate(container_cpu_usage_seconds_total'):
                            node_to_cpu_core = dict(zip(cpu_core_df['node'], cpu_core_df['cpu_core']))
                            query_df['node'] = query_data[0].get('metric', {}).get('node', None)
                            query_df['cpu_core'] = query_df['node'].map(node_to_cpu_core)
                            query_df.dropna(subset=['cpu_core'], inplace=True)
                            query_df[var_name] = query_df[var_name] * query_df['cpu_core']
                            query_df.drop(columns=['cpu_core', 'node'], inplace=True)
                        df_list.append(query_df)
                except PrometheusApiClientException:
                    f.write(ml_pod)
                    pass
                
        if df_list:  # addict df to db_dict if df_list not empty
            db_dict["df_array"] = df_list
            each_label_data.append(db_dict)
    f.close()
    
    print("Scrapping success!")
    print("===============================================================================")
    return each_label_data

def remove_numbers_and_hashes(pod_name:str):
    cleaned_string = re.sub(r'\b(\d+|[a-zA-Z0-9]{5})\b', '', pod_name)
    cleaned_string = re.sub(r'-{2,}', '-', cleaned_string)  # 2개 이상의 하이픈을 하나로 줄임
    return cleaned_string.strip('-')  # 앞뒤에 남은 하이픈 제거
        
def preprocessing(each_label_data:list, method:str, chunk_size:str):
    print("preprocessing start...")
    
    for dict in each_label_data:
        core_df = pd.concat(dict["df_array"],axis=0)
        # timestamp to yy-mm-dd format
        core_df['timestamp']=pd.to_datetime(core_df['timestamp'],unit='s')
        # round time by set unit
        core_df['timestamp'] = core_df['timestamp'].apply(lambda x: x.round(freq=chunk_size))
        # # pod name preprocess
        core_df['pod'] = core_df.apply(lambda x: x['pod'].replace('-' + x['pod'].split('-')[-1],''), axis=1)
        core_df['pod'] = core_df['pod'].apply(lambda x: remove_numbers_and_hashes(x))
        
        core_df_groupby = core_df.groupby(['timestamp','pod'])
        metric_list = core_df.columns.difference(['timestamp','pod'])
        if method == 'sum':
            total_df = core_df_groupby[metric_list].sum().reset_index(drop=False)    
        elif method == 'mean':
            total_df = core_df_groupby[metric_list].mean().reset_index(drop=False)
        elif method == 'median':
            total_df = core_df_groupby[metric_list].median().reset_index(drop=False)
        dict["df_array"]=total_df
    print('preprocessing success!')
    print("===============================================================================")
    return each_label_data



def pod_separate(db_args: 'DBConfig', each_label_data:list):
    db_url, db_port, db_user, db_passwd = db_args.db_url, db_args.db_port, db_args.db_user, db_args.db_passwd
    
    for dict in each_label_data:
        """
        dict format : 
        {"label": "train", "df_array": total_df}
        type of total_df is pandas Dataframe, type of each_label_data is list so that extract from each_label_data to dict.
        
        each_label_data format : 
        [{"label": "preprocess", "df_array": total_df1}, {"label": "train", "df_array": total_df2}, {"label": "inference", "df_array": total_df3}]
        """
        # define db_name each ML Workload Label
        db_name = 'ml_workload_'+dict['label'].replace('-','_')
        
        #create connection with pymysql
        create_conn = pymysql.connect(host=db_args.db_url,port=db_args.db_port, user=db_args.db_user,password=db_args.db_passwd,charset='utf8')
        try:
            create_conn.cursor().execute('create database ' + db_name +';')
            print("Database '"+db_name+"' creation succeed.")
        except:
            pymysql.err.ProgrammingError
        db_connection_path = 'mysql+pymysql://root:'+db_args.db_passwd+'@'+db_args.db_url+':'+str(db_args.db_port)+'/'+db_name
        db_connection = create_engine(db_connection_path)
        conn = db_connection.connect()
        
        
        print('Pod separate start. [Current Database : ', db_name ,']')
        
        pod_list = dict['df_array']['pod'].unique()
        
        table_list = conn.engine.table_names()
        total_df = dict['df_array']
        for idx, pod in enumerate(pod_list):
            tmp = total_df[total_df['pod']==pod]
            tmp = tmp.sort_values(by=['timestamp'],axis=0).reset_index(drop=True)
            tmp = tmp.groupby(['timestamp', 'pod']).agg({
                  'cpu_usage': 'max',
                  'memory_usage': 'max',
                  'network_transmit': 'max',
                  'network_receive': 'max',
                  'file_system_reads': 'max',
                  'file_system_writes': 'max'
                  }).reset_index()
            tmp = tmp.drop(['pod'],axis=1)
            drop_list=[]
            for i in range(len(tmp.columns[1:])):
                if tmp.describe().iloc[-1,i]==np.nan:                
                    drop_list.append(tmp.columns[i+1])
            drop_list = list(set(drop_list))
            tmp = tmp.drop(drop_list,axis=1)
            tmp = tmp.fillna(1e-9)
            
            # insert DB
            table_name = re.sub('[-=+,#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·]', '_',pod)
            table_name = re.sub("'",'',table_name)
            if table_name in table_list:
                db_df = pd.read_sql(f"SELECT * FROM {table_name}", con=conn)
                missing_data_db_df = tmp[~tmp['timestamp'].isin(db_df['timestamp'])]
                print("additional "+table_name+"'s data : ")
                print(missing_data_db_df)
                merged_df = pd.concat([db_df,missing_data_db_df],ignore_index=True)
                merged_df.to_sql(name=table_name, con=conn,if_exists='replace',index=False)
            else:
                tmp.to_sql(name=table_name, con=conn,if_exists='replace',index=False)
            print("Table " +table_name+" insert success.")
            
            
        print("===============================================================================")
        print('Current Database : ', db_name)
        print("Current table list: ")
        print(conn.engine.table_names())
        print("===============================================================================")
