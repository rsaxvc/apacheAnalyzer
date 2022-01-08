import argparse
from pypika import functions as fn
from pypika import Query, Table, Field
import pypika
import sqlite3

parser = argparse.ArgumentParser(description='Query logs into a count per chunk over time.')
parser.add_argument('--chunkSeconds', type=int, help='seconds per chunk', default=60*60)
parser.add_argument('--startTime', type=int, help='UTC UNIX seconds')
parser.add_argument('--stopTime', type=int, help='UTC UNIX seconds')
parser.add_argument('--outputFmt',
                    default='python',
                    const='python',
                    nargs='?',
                    choices=['python','json', 'csv', 'all', 'matplotlib'],
                    help='output format(default: %(default)s)')
parser.add_argument('--maxChunks', type=int, help='Fetch only last N chunks' )
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

q = q.groupby(logs.apachelog_request_time_unix/args.chunkSeconds)
q = q.select(fn.Cast(logs.apachelog_request_time_unix - logs.apachelog_request_time_unix%args.chunkSeconds, 'INTEGER'), fn.Count(logs.apachelog_request_time_unix))
	
if args.maxChunks != None:
	q = q.limit(args.maxChunks)

print( str(q) )

rslt = cur.execute(str(q)).fetchall()
if args.outputFmt == 'python' or args.outputFmt == 'all':
	print(rslt)
if args.outputFmt == 'json' or args.outputFmt == 'all':
	import json
	print(json.dumps({k:v for k,v in rslt}))
if args.outputFmt == 'csv' or args.outputFmt == 'all':
	import csv
	print('timestamp,count')
	for row in rslt:
		print( str(row[0]) + ',' + str(row[1]) )
if args.outputFmt == 'matplotlib':
	import matplotlib.pyplot as plt
	x = [ i for i, j in rslt ]
	y = [ j for i, j in rslt ]
	plt.plot(x, y )
	plt.show()
