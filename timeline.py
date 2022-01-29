#!/usr/bin/python3
from collections.abc import Iterable
import os
from pypika import functions as fn
from pypika import Query, Table, Field, Parameter
import pypika
import sqlite3
import sys

if 'REQUEST_METHOD' in os.environ:
	import cgi
	args = dict()
	form = cgi.FieldStorage()
	for field in form.keys():
		args[field] = form.getfirst(field)
	if "outputFmt" not in args:
		args["outputFmt"] = "html"
	if( args["outputFmt"] == "html" ):
		print( "Content-type: text/html" )
	elif( args["outputFmt"] == "json" ):
		print( "Content-type: application/json" )
	else:
		print( "Content-type: text/plain" )
	print()
else:
	import argparse
	parser = argparse.ArgumentParser(description='Query logs into a count per chunk over time.')
	parser.add_argument('--chunkSeconds', type=int, help='seconds per chunk', default=60*60)
	parser.add_argument('--dumpSql', action='store_true')
	parser.add_argument('--remoteHost', type=str, action='append', help='Filter by remote IP addresses')
	parser.add_argument('--notRemoteHost', type=str, action='append', help='Filter by not remote IP addresses')
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
p = []

def present( args, key ):
	return key in args and args[key] != None

if present( args, "remoteHost"):
	if isinstance( args["remoteHost"], str) or not isinstance( args["remoteHost"], Iterable ):
		a = [args["remoteHost"]]
	else:
		a = list(args["remoteHost"])
	q = q.where(logs.apachelog_remote_host.isin([Parameter('?')] *len(a)))
	p.extend( a )

if present( args, "notRemoteHost"):
	if isinstance( args["notRemoteHost"], str) or not isinstance( args["notRemoteHost"], Iterable ):
		a = [args["notRemoteHost"]]
	else:
		a = list(args["notRemoteHost"])
	q = q.where(logs.apachelog_remote_host.notin([Parameter('?')] *len(a)))
	p.extend( a )

if present( args, "startTime"):
	q = q.where(logs.apachelog_request_time_unix >= Parameter('?'))
	p.append( args["startTime"] )

if present( args, "stopTime"):
	q = q.where(logs.apachelog_request_time_unix < Parameter('?'))
	p.append( args["stopTime"] )

if not present( args, "chunkSeconds" ):
	args["chunkSeconds"] = 60*60

q = q.groupby(logs.apachelog_request_time_unix / Parameter('?'))
p.append(args["chunkSeconds"])
q = q.select(logs.apachelog_request_time_unix / Parameter('?') * Parameter('?'), fn.Count(logs.apachelog_request_time_unix))
p.insert(0, args["chunkSeconds"])
p.insert(0, args["chunkSeconds"])

if present( args, "maxChunks"):
	q = q.limit(args.maxChunks)

if present( args, "dumpSql" ) and args["dumpSql"]:
	print(q)
	print(p)

rslt = cur.execute(str(q), p).fetchall()
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
