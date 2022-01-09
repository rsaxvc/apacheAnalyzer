import os
from pypika import functions as fn
from pypika import Query, Table, Field
import pypika
import sqlite3
import sys
from timeit import default_timer as timer

sys.stdout.reconfigure(encoding='utf-8')

if 'REQUEST_METHOD' in os.environ:
	import cgi
	args = cgi.FieldStorage()
	if "maxLogs" not in args:
		args["maxLogs"] = 100
	if "outputFmt" not in args:
		args["outputFmt"] = "html"
	if( args["outputFmt"] == "html" ):
		print( "Content-type: text/html\n\n" )
	elif( args["outputFmt"] == "json" ):
		print( "Content-type: application/json\n\n" )
	else:
		print( "Content-type: text/plain\n\n" )
	print()
else:
	import argparse
	parser = argparse.ArgumentParser(description='Query logs by client.')
	parser.add_argument('--maxLogs', type=int, help='maximum number of logs to fetch', default=100 )
	parser.add_argument('--startTime', type=int, help='UTC UNIX seconds')
	parser.add_argument('--stopTime', type=int, help='UTC UNIX seconds')
	parser.add_argument('--outputFmt',
					default='text',
					const='text',
					nargs='?',
					choices=['text','json', 'html', 'csv', 'all'],
					help='output format(default: %(default)s)')
	args = vars( parser.parse_args() )

conn = sqlite3.connect("logdb.db")
cur = conn.cursor()
cur.execute('pragma query_only = ON')

logs = Table('log_entries')
q = Query.from_(logs)

def present( args, key ):
	return key in args and args[key] != None

if present( args, "startTime"):
	q = q.where(logs.apachelog_request_time_unix >= args["startTime"])
	
if present( args, "stopTime"):
	q = q.where(logs.apachelog_request_time_unix < args["stopTime"])

q = q.select(logs.apachelog_request_time, logs.apachelog_remote_host, logs.apachelog_request_line)

if present( args, "maxLogs"):
	q = q.limit( args["maxLogs"] )

rslt = cur.execute(str(q)).fetchall()
if args["outputFmt"] == 'text' or args["outputFmt"] == 'all':
	print(rslt)
if args["outputFmt"] == 'json' or args["outputFmt"] == 'all':
	import json
	print(json.dumps({k:v for k,v in rslt}))
if args["outputFmt"] == 'csv' or args["outputFmt"] == 'all':
	import csv
	print('timestamp,remote host,request')
	for tup in rslt:
		print( ",".join( [str(t) for t in tup] ) )
if args["outputFmt"] == 'html' or args["outputFmt"] == 'all':
	print( "<html><head><title>Logs</title></head><body>" )
	print( "<table border=\"1\">" )
	print( "	<tr><th>BaseTime</th><th>Count</th><th>AvgRate</th></tr>")
	for tup in rslt:
		print("	<tr><td>" + "</td><td>".join( [ str(t) for t in tup ] ) + "</td></tr>")
	print( "</table>" )
	print( "</body></html>" )