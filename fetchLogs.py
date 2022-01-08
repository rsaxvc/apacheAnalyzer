import argparse
from timeit import default_timer as timer
from pypika import functions as fn
from pypika import Query, Table, Field
import pypika
import sqlite3

parser = argparse.ArgumentParser(description='Query logs by client.')
parser.add_argument('--startTime', type=int, help='UTC UNIX seconds')
parser.add_argument('--stopTime', type=int, help='UTC UNIX seconds')
args = parser.parse_args()

conn = sqlite3.connect("logdb.db")
cur = conn.cursor()
cur.execute('pragma query_only = ON')

logs = Table('log_entries')
q = Query.from_(logs)

if args.startTime != None:
	q = q.where(logs.apachelog_request_time_unix >= args.startTime)
	
if args.stopTime != None:
	q = q.where(logs.apachelog_request_time_unix < args.stopTime)

q = q.select(logs.apachelog_request_line)

print(q)

for log in cur.execute(str(q)):
	print(log[0])
