#!/usr/bin/python3
import os
from pypika import functions as fn
from pypika import Query, Table, Field
import pypika
import sqlite3
import sys

if 'REQUEST_METHOD' in os.environ:
	import cgi
	args = dict(cgi.FieldStorage())
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
	parser = argparse.ArgumentParser(description='Query logs into a count per chunk over time.')
	parser.add_argument('--chunkSeconds', type=int, help='seconds per chunk', default=60*60)
	parser.add_argument('--startTime', type=int, help='UTC UNIX seconds')
	parser.add_argument('--stopTime', type=int, help='UTC UNIX seconds')
	parser.add_argument('--outputFmt',
						default='python',
						const='python',
						nargs='?',
						choices=['python','json', 'csv', 'html', 'all', 'matplotlib'],
						help='output format(default: %(default)s)')
	parser.add_argument('--maxChunks', type=int, help='Fetch only last N chunks' )
	args = vars(parser.parse_args())

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

if not present( args, "chunkSeconds" ):
	args["chunkSeconds"] = 60*60

q = q.groupby(logs.apachelog_request_time_unix/args["chunkSeconds"])
#q = q.select(fn.Cast(logs.apachelog_request_time_unix - logs.apachelog_request_time_unix % args["chunkSeconds"], 'INTEGER'), fn.Count(logs.apachelog_request_time_unix))
q = q.select(logs.apachelog_request_time_unix / args["chunkSeconds"] * args["chunkSeconds"], fn.Count(logs.apachelog_request_time_unix))

if present( args, "maxChunks"):
	q = q.limit(args.maxChunks)

rslt = cur.execute(str(q)).fetchall()
if args["outputFmt"] == 'python' or args["outputFmt"] == 'all':
	print(rslt)
if args["outputFmt"] == 'json' or args["outputFmt"] == 'all':
	import json
	print(json.dumps({k:v for k,v in rslt}))
if args["outputFmt"] == 'csv' or args["outputFmt"] == 'all':
	import csv
	print('timestamp,count')
	for tup in rslt:
		print( ",".join( [str(t) for t in tup] ) )
if args["outputFmt"] == 'matplotlib':
	import matplotlib.pyplot as plt
	x = [ i for i, j in rslt ]
	y = [ j for i, j in rslt ]
	plt.plot(x, y )
	plt.show()
if args["outputFmt"] == 'html' or args["outputFmt"] == 'all':
	print( "<html><head><title>Traffic Per Time</title></head><body>" )
	print( "<table border=\"1\">" )
	print( "	<tr><th>BaseTime</th><th>Count</th><th>Rate/Min</th></tr>")
	for tup in rslt:
		print("	<tr><td>"+str(tup[0]) + "</td><td>"+ str(tup[1])+"</td><td>" + "{:.2f}".format(60*tup[1]/args["chunkSeconds"]) + "</td></tr>")
	print( "</table>" )
	print( "</body></html>" )
