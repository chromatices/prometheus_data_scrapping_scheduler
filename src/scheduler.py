import re
import argparse
import schedule
import pymysql
from dataclasses import dataclass
from sqlalchemy import create_engine
from prometheus_scrapper import scrapping, preprocessing, pod_separate

parser = argparse.ArgumentParser(description='Process to set parameters.')
parser.add_argument('--prometheus_url', type=str, default='http://10.0.1.102:30090/')
parser.add_argument('--db_url', type=str, default='10.0.2.142')
parser.add_argument('--db_port',type=int, default='30091')
parser.add_argument('--db_user', type=str, default='root')
parser.add_argument('--db_passwd', type=str, default='ketilinux')
parser.add_argument('--start_time', type=str,default="5d")
parser.add_argument('--end_time', type=str,default='now')
parser.add_argument('--scrap_size', type=str,default='5m',help = "set scraping size. choose time unit[d:day,h:hour,m:minute,s:second]")
parser.add_argument('--chunk_size', type=str,default='5T', help="set chunk size fot rounding timestamp. 'D' means day, 'H' means hour, 'T' means minute 's' means seconds.")
parser.add_argument('--method', type=str,default='mean', help= "choose groupby method ex) mean, sum, median")
parser.add_argument('--schedule_unit', type=str,default='minute', help= "choose unit of schedule period ex) day, hour, minute, second..")
args = parser.parse_args()

@dataclass
class DBConfig:
    db_url: str
    db_port: int
    db_user: str
    db_passwd: str
    
def main(url:str, db_url:str, db_port:int, db_user:str, db_passwd:str, start_time:str, end_time:str, scrap_size:str, chunk_size:str, method:str):
    print("===============================================================================\n")
    print('scheduler is running..')
    
    args_db = DBConfig(db_url=db_url, db_port=db_port, db_user=db_user, db_passwd=db_passwd)
    data_list = scrapping(url,start_time,end_time,scrap_size)
    integeration_list = preprocessing(data_list,method, chunk_size)
    pod_separate(args_db, integeration_list)

def scheduler(args:argparse.Namespace):

    main(args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,args.start_time, args.end_time, args.scrap_size, args.chunk_size, args.method)
    time_unit = int(re.match(r'^(\d+)', args.scrap_size).group(1))
    if args.schedule_unit == 'day':
        schedule.every(time_unit).days.at("06:00").do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,'1d', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'hour':
        schedule.every(time_unit).hours.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,'1h', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'minute':
        schedule.every(time_unit).minutes.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,str(time_unit*30)+'m', args.end_time, args.scrap_size, args.chunk_size, args.method)
    elif args.schedule_unit == 'second':
        schedule.every(time_unit).seconds.do(main,args.prometheus_url,args.db_url,args.db_port,args.db_user,args.db_passwd,str(time_unit*1800) + 's', args.end_time, args.scrap_size, args.chunk_size, args.method)
    else:
        print("Schedule unit invalid. Please check your arguments.")
    while True:
        schedule.run_pending()

if __name__ == '__main__':
    scheduler(args)
