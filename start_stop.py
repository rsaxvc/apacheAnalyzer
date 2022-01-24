#!/usr/bin/python3
import os
from pypika import functions as fn
from pypika import Query, Table, Field, Parameter
import pypika
import sqlite3
import sys
from timeit import default_timer as timer

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
