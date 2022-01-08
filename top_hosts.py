import argparse
from timeit import default_timer as timer
from pypika import functions as fn
from pypika import Query, Table, Field, Order
import pypika
import sqlite3

parser = argparse.ArgumentParser(description='Query logs by client.')
parser.add_argument('--startTime', type=int, help='UTC UNIX seconds')
parser.add_argument('--stopTime', type=int, help='UTC UNIX seconds')
parser.add_argument('--minRequestsPerHost', type=int, help='hide hosts with fewer requests', default=20 )
parser.add_argument('--maxHosts', type=int, help='Fetch only top N hosts by request count', default=10 )
parser.add_argument('--outputFmt',
                    default='python',
                    const='python',
                    nargs='?',
                    choices=['python','json', 'csv', 'all'],
                    help='output format(default: %(default)s)')
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

q = q.groupby(logs.apachelog_remote_host) \
	.select(logs.apachelog_remote_host, fn.Count(logs.apachelog_remote_host).as_('RequestsPerHost') )

if args.minRequestsPerHost != None:
	q = q.having(fn.Count(logs.apachelog_remote_host) >= args.minRequestsPerHost )

q = q.orderby('RequestsPerHost', order=Order.desc)

if args.maxHosts != None:
	q = q.limit( args.maxHosts )

print(q)

rslt = cur.execute(str(q)).fetchall()
if args.outputFmt == 'python' or args.outputFmt == 'all':
	print(rslt)
if args.outputFmt == 'json' or args.outputFmt == 'all':
	import json
	print(json.dumps({k:v for k,v in rslt}))
if args.outputFmt == 'csv' or args.outputFmt == 'all':
	print('client,count')
	for row in rslt:
		print( str(row[0]) + ',' + str(row[1]) )