import re
import pymysql
import argparse
import schedule
from sqlalchemy import create_engine
from prometheus_scrapper import scrapping, preprocessing, pod_separate
parser = argparse.ArgumentParser(description='Process to set parameters.')
parser.add_argument('--prometheus_url', type=str, default='http://prometheus_url:30090/')
parser.add_argument('--db_url', type=str, default='db_url')
parser.add_argument('--db_port',type=int, default='30001')
parser.add_argument('--db_user', type=str, default='root')
parser.add_argument('--db_passwd', type=str, default='test123')
parser.add_argument('--db_name', type=str, default='Prometheus')
parser.add_argument('--start_time', type=str,default="2h")
parser.add_argument('--end_time', type=str,default='now')
parser.add_argument('--scrap_size', type=str,default='1h',help = "set scraping size. choose time unit[d:day,h:hour,m:minute,s:second]")
parser.add_argument('--chunk_size', type=str,default='1H', help="set chunk size fot rounding timestamp. 'D' means day, 'H' means hour, 'T' means minute 's' means seconds.")
parser.add_argument('--method', type=str,default='mean', help= "choose groupby method ex) mean, sum, median")
parser.add_argument('--schedule_unit', type=str,default='minute', help= "choose unit of schedule period ex) day, hour, minute, second..")
args = parser.parse_args()

def main(url:str, db_url:str, db_port:int, db_user:str, db_passwd:str, db_name:str, start_time:str, end_time:str, scrap_size:str, chunk_size:str, method:str):
    print("===============================================================================\n")
    print('scheduler is running..')    
    connection = pymysql.connect(host=db_url,port=db_port, user=db_user,password=db_passwd,charset='utf8')
    try:
        connection.cursor().execute('create database ' + db_name +';')
        print("Database '"+db_name+"' creation succeed.")
    except:
        pymysql.err.ProgrammingError

    db_connection_path = 'mysql+pymysql://root:'+db_passwd+'@'+db_url+':'+str(db_port)+'/'+db_name
    db_connection = create_engine(db_connection_path)
    insert_conn = db_connection.connect()

    data_list = scrapping(url,start_time,end_time,scrap_size)
    integeration = preprocessing(data_list, chunk_size, method)
    pod_separate(integeration,insert_conn)

def scheduler(args:parser):

    main(args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,args.start_time, args.end_time, args.scrap_size, args.chunk_size, args.method)
    time_unit = int(re.match(r'^(\d+)', args.scrap_size).group(1))
    if args.schedule_unit == 'day':
        schedule.every(time_unit).day.at("06:00").do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,'1d', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'hour':
        schedule.every(time_unit).hour.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,'1h', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'minute':
        schedule.every(time_unit).minutes.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,str(time_unit*30)+'m', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'second':
        schedule.every(time_unit).seconds.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,str(time_unit*1800) + 's', args.end_time, args.scrap_size, args.chunk_size, args.method)
    else:
        print("Schedule unit invalid. Please check your arguments.")
    while True:
        schedule.run_pending()

if __name__ == '__main__':
    scheduler(args)