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
	import cgitb
	args = dict()
	form = cgi.FieldStorage()
	for field in form.keys():
		if field in['remoteHost', 'notRemoteHost']:
			args[field] = form.getvalue(field)
		else:
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
	parser.add_argument('--dumpSql', action='store_true')
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
qmin = Query.from_(logs)
pmin = []
qmax = Query.from_(logs)
pmax = []

def present( args, key ):
	return key in args and args[key] != None

if present( args, "remoteHost"):
	if isinstance( args["remoteHost"], str) or not isinstance( args["remoteHost"], Iterable ):
		a = [args["remoteHost"]]
	else:
		a = list(args["remoteHost"])
	qmin = qmin.where(logs.apachelog_remote_host.isin([Parameter('?')] *len(a)))
	qmax = qmax.where(logs.apachelog_remote_host.isin([Parameter('?')] *len(a)))
	pmin.extend( a )
	pmax.extend( a )

if present( args, "notRemoteHost"):
	if isinstance( args["notRemoteHost"], str) or not isinstance( args["notRemoteHost"], Iterable ):
		a = [args["notRemoteHost"]]
	else:
		a = list(args["notRemoteHost"])
	qmin = qmin.where(logs.apachelog_remote_host.notin([Parameter('?')] *len(a)))
	qmax = qmax.where(logs.apachelog_remote_host.notin([Parameter('?')] *len(a)))
	pmin.extend( a )
	pmax.extend( a )

if present( args, "startTime"):
	qmin = qmin.where(logs.apachelog_request_time_unix >= Parameter('?'))
	qmax = qmax.where(logs.apachelog_request_time_unix >= Parameter('?'))
	pmin.append( args["startTime"] )
	pmax.append( args["startTime"] )

if present( args, "stopTime"):
	qmin = qmin.where(logs.apachelog_request_time_unix < Parameter('?'))
	qmax = qmax.where(logs.apachelog_request_time_unix < Parameter('?'))
	pmin.append( args["stopTime"] )
	pmax.append( args["stopTime"] )

qmin = qmin.select(fn.Min(logs.apachelog_request_time_unix))
qmax = qmax.select(fn.Max(logs.apachelog_request_time_unix))

if present( args, "dumpSql" ) and args["dumpSql"]:
	print(qmin)
	print(pmin)
	print(qmax)
	print(pmax)

rslt_min = cur.execute(str(qmin), pmin).fetchone()
rslt_max = cur.execute(str(qmax), pmax).fetchone()
if args["outputFmt"] == 'text' or args["outputFmt"] == 'all':
	print("oldest log:", rslt_min[0])
	print("newest log:", rslt_max[0])
if args["outputFmt"] == 'json' or args["outputFmt"] == 'all':
	import json
	print(json.dumps({"oldest":rslt_min[0], "newest":rslt_max[0]}))
if args["outputFmt"] == 'csv' or args["outputFmt"] == 'all':
	import csv
	print('oldest,newest')
	print(str(rslt_min[0]) + ',' + str(rslt_max[0]) )
if args["outputFmt"] == 'html' or args["outputFmt"] == 'all':
	print( "<html><head><title>Oldest and Newest</title></head><body>" )
	print( "<table border=\"1\">" )
	print( "	<tr><th>Oldest</th><th>Newest</th></tr>")
	print( "	<tr><td>" + str(rslt_min[0]) + "</td><td>" + str(rslt_max[0]) + "</td></tr>")
	print( "</table>" )
	print( "</body></html>" )
