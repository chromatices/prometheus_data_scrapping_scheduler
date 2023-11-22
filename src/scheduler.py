import pymysql
import argparse
import schedule
from sqlalchemy import create_engine
from prometheus_scrapper import scrapping, preprocessing, pod_separate
parser = argparse.ArgumentParser(description='Process to set parameters.')
parser.add_argument('--prometheus_url', type=str, default='http://10.0.1.102:30090/')
parser.add_argument('--db_url', type=str, default='10.0.1.102')
parser.add_argument('--db_port',type=int, default='30091')
parser.add_argument('--db_user', type=str, default='root')
parser.add_argument('--db_passwd', type=str, default='ketilinux')
parser.add_argument('--db_name', type=str, default='Prometheus')
parser.add_argument('--start_time', type=str,default="2h")
parser.add_argument('--end_time', type=str,default='now')
parser.add_argument('--scrap_size', type=str,default='1h',help = "choose time unit[d:day,h:hour,m:minute,s:second]")
parser.add_argument('--chunk_size', type=str,default='1H', help="set chunk size fot rounding timestamp. 'D' means day, 'H' means hour, 'T' means minute 's' means seconds.")
parser.add_argument('--method', type=str,default='mean', help= "choose groupby method ex) mean, sum, median")
parser.add_argument('--schedule_unit', type=str,default='minute', help= "choose unit of schedule period ex) day, hour, minute, second..")
args = parser.parse_args()

def main(url:str, db_url:str, db_port:int, db_user:str, db_passwd:str, db_name:str, start_time:str, end_time:str, scrap_size:str, chunk_size:str, method:str):
    
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
    print("===============================================================================\n")
    print('scheduler is running..')
    time_unit = int(args.scrap_size)
    if args.schedule_unit == 'day':
        schedule.every().day.at("06:00").do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,'1d', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'hour':
        schedule.every(1).hour.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,'1h', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'minute':
        schedule.every(15).minutes.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,'15m', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'second':
        schedule.every(60).seconds.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.db_name,'1m', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'none':
        pass
    while True:
        schedule.run_pending()

if __name__ == '__main__':
    scheduler(args)