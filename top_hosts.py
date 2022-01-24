#!/usr/bin/python3
import os
import pypika
import sqlite3
import sys

from pypika import functions as fn
from pypika import Query, Table, Field, Order, Parameter

if 'REQUEST_METHOD' in os.environ:
	import cgi
	import cgitb
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
	cgitb.enable()
else:
	import argparse
	parser = argparse.ArgumentParser(description='Query logs by client.')
	parser.add_argument('--startTime', type=int, help='UTC UNIX seconds')
	parser.add_argument('--stopTime', type=int, help='UTC UNIX seconds')
	parser.add_argument('--minRequestsPerHost', type=int, help='hide hosts with fewer requests', default=20 )
	parser.add_argument('--maxHosts', type=int, help='Fetch only top N hosts by request count', default=10 )
	parser.add_argument('--outputFmt',
						default='python',
						const='python',
						nargs='?',
						choices=['python','json', 'csv', 'html', 'all'],
						help='output format(default: %(default)s)')
	args = vars(parser.parse_args())

conn = sqlite3.connect("logdb.db")
cur = conn.cursor()
cur.execute('pragma query_only = ON')

logs = Table('log_entries')
q = Query.from_(logs)
p = []
def present( args, key ):
	return key in args and args[key] != None

if not present( args, "minRequestsPerHost"):
	args["minRequestsPerHost"] = 20

if not present( args, "maxHosts"):
	args["maxHosts"] = 10

if present( args, "startTime"):
	q = q.where(logs.apachelog_request_time_unix >= Parameter('?'))
	p.append( args["startTime"] )

if present( args, "stopTime"):
	q = q.where(logs.apachelog_request_time_unix < Parameter('?'))
	p.append( args["startTime"] )

q = q.groupby(logs.apachelog_remote_host) \
	.select(logs.apachelog_remote_host, fn.Count(logs.apachelog_remote_host).as_('RequestsPerHost'), logs.geoip_full )

if present( args, "minRequestsPerHost"):
	q = q.having(fn.Count(logs["apachelog_remote_host"]) >= Parameter('?') )
	p.append( args["minRequestsPerHost"] )

q = q.orderby('RequestsPerHost', order=Order.desc)

if present( args, "maxHosts"):
	q = q.limit( Parameter('?') )
	p.append( args["maxHosts"] )

rslt = cur.execute(str(q), p).fetchall()
if args["outputFmt"] == 'python' or args["outputFmt"] == 'all':
	print(rslt)
if args["outputFmt"] == 'json' or args["outputFmt"] == 'all':
	import json
	print(json.dumps(rslt))
if args["outputFmt"] == 'csv' or args["outputFmt"] == 'all':
	print('client,count')
	for tup in rslt:
		print( ",".join( [str(t) for t in tup] ) )
if args["outputFmt"] == 'html' or args["outputFmt"] == 'all':
	print( "<html><head><title>Top Hosts</title></head><body>" )
	print( "<table border=\"1\">" )
	print( "	<tr><th>RemoteHost</th><th>Count</th><th>GeoIp</th></tr>")
	for tup in rslt:
		print("	<tr><td>"+"</td><td>".join([str(t) for t in tup]) +"</td></tr>")
	print( "</table>" )
	print( "</body></html>" )
