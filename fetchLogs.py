#!/usr/bin/python3
from collections.abc import Iterable
import html
import os
from pypika import functions as fn
from pypika import Query, Table, Field, Order, Parameter
import pypika
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

if 'REQUEST_METHOD' in os.environ:
	import cgi
	import cgitb

	args = dict()
	form = cgi.FieldStorage()
	for field in form.keys():
		args[field] = form.getfirst(field)
	if "maxLogs" not in args:
		args["maxLogs"] = 100
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
	parser.add_argument('--dumpSql', action='store_true')
	parser.add_argument('--maxLogs', type=int, help='maximum number of logs to fetch', default=100 )
	parser.add_argument('--startsWith', type=str, help='Log request starts with')
	parser.add_argument('--contains', type=str, help='Log request contains')
	parser.add_argument('--remoteHost', type=str, action='append', help='Filter by remote IP addresses')
	parser.add_argument('--notRemoteHost', type=str, action='append', help='Filter by not remote IP addresses')
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
	q = q.where(logs.apachelog_request_time_unix >= Parameter('?') )
	p.append( args["startTime"] )

if present( args, "stopTime"):
	q = q.where(logs.apachelog_request_time_unix < Parameter('?') )
	p.append( args["stopTime"] )

if present( args, "startsWith"):
	q = q.where(logs.apachelog_request_line.like(Parameter('?')))
	p.append( args["startsWith"] +'%' )

if present( args, "contains"):
	q = q.where(logs.apachelog_request_line.like(Parameter('?')))
	p.append( '%' + args["contains"] + '%')

q = q.select(logs.apachelog_request_time_unix, logs.apachelog_remote_host, logs.geoip_full, logs.apachelog_request_line)

q = q.orderby(logs.apachelog_request_time_unix, order=Order.desc)

if present( args, "maxLogs"):
	q = q.limit( Parameter('?') )
	p.append( args["maxLogs"] )

if present( args, "dumpSql" ) and args["dumpSql"]:
	print(q)
	print(p)
rslt = cur.execute(str(q),p).fetchall()
if args["outputFmt"] == 'text' or args["outputFmt"] == 'all':
	print(rslt)
if args["outputFmt"] == 'json' or args["outputFmt"] == 'all':
	import json
	print(json.dumps(rslt))
if args["outputFmt"] == 'csv' or args["outputFmt"] == 'all':
	import csv
	print('timestamp,remote host,geoip,request')
	for tup in rslt:
		print( ",".join( [str(t) for t in tup] ) )
if args["outputFmt"] == 'html' or args["outputFmt"] == 'all':
	print( "<html><head><title>Logs</title></head><body>" )
	print( "<table border=\"1\">" )
	print( "	<tr><th>BaseTime</th><th>Remotehost</th><th>GeoIp</th><th>LogEntry</th></tr>")
	for tup in rslt:
		print("	<tr><td>" + "</td><td>".join( [ html.escape(str(t)) for t in tup ] ) + "</td></tr>")
	print( "</table>" )
	print( "</body></html>" )
