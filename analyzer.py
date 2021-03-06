#!/usr/bin/python3
from apachelogs import LogParser
import apachelogs
import argparse
import gzip
from timeit import default_timer as timer
import traceback
import os
import datetime
import sqlite3

parser = argparse.ArgumentParser(description='Parse apache log files.')
parser.add_argument('log_paths', type=str, nargs='+',
                    help='path to files or directories')
args = parser.parse_args()

GEODB = 'GeoLite2-City.mmdb'

georeader = None
try:
	import geoip2.database
	try:
		georeader = geoip2.database.Reader(GEODB)
	except:
		print("Warning! Unable to find: "+GEODB+", geoip resolution will be disabled")
except:
	print("Warning! Unable to load geoip2 module, geoip resolution will be disabled")

geoip_cache = dict()
conn = sqlite3.connect("logdb.db")
cur = conn.cursor()
if conn is not None:
	cur.execute(""" CREATE TABLE IF NOT EXISTS log_entries (
					apachelog_bytes_sent INTEGER,
					apachelog_final_status INTEGER,
					apachelog_headers_referer VARCHAR,
					apachelog_headers_useragent VARCHAR,
					apachelog_remote_host VARCHAR,
					apachelog_remote_logname VARCHAR,
					apachelog_remote_user VARCHAR,
					apachelog_request_line VARCHAR,
					apachelog_request_time VARCHAR,
					apachelog_request_time_unix INTEGER,
					apachelog_line INTEGER,
					geoip_city VARCHAR,
					geoip_country VARCHAR,
					geoip_full VARCHAR
                    ); """)

	cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS log_entries_idx_unique ON log_entries(apachelog_request_time_unix,apachelog_line,apachelog_remote_host,COALESCE(apachelog_request_line,''))""")

	cur.execute("""CREATE INDEX IF NOT EXISTS log_entries_idx_remote_host ON
					log_entries(apachelog_remote_host);""")

	cur.execute("""CREATE INDEX IF NOT EXISTS log_entries_idx_geoip_full ON
					log_entries(geoip_full);""")

def do_geoip( remote_host ):
	if georeader is None:
		return (None,None,None)
	elif remote_host in geoip_cache:
		return geoip_cache[remote_host]
	else:
		location = (None,None,None)
		try:
			lookup = georeader.city(remote_host)
			full = [ str(lookup.country.name) ]
			for subdivision in lookup.subdivisions:
				full.append( str(subdivision.name) )
			full.append( str(lookup.city.name) )
			location = ( str(lookup.country.name), str(lookup.city.name), '/'.join(full) )
			geoip_cache[remote_host] = location
			return location
		except geoip2.errors.AddressNotFoundError:
			geoip_cache[remote_host] = location
			return location

def autoGzOpen( filename ):
	r = open( filename, 'rb' )
	magic2 = r.read(2)
	r.close()
	if magic2 == b'\x1f\x8b':
		return gzip.open( filename, 'rt', encoding='utf8' )
	else:
		return open( filename )

def autoparse( filename ):
	logFormats=[
		"%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"", #Combined
		"%h %l %u %t \"%r\" %>s %b" #common
		]
	with autoGzOpen(filename) as fp:
		for lf in logFormats:
			try:
				entries=[]
				line_no = 0
				for log_entry in LogParser(lf).parse_lines(fp):
					line_no += 1
					entry = dict()
					entry["apachelog_line"] = line_no

					for k in vars(log_entry).keys():
						if not k.startswith('_') and k != "entry" and k != "format" and k != 'directives':
							entry["apachelog_"+k] = getattr(log_entry, k)

					if "apachelog_request_time_fields" in entry:
						del entry["apachelog_request_time_fields"]

					if "apachelog_request_time" in entry:
						entry["apachelog_request_time_unix"] = datetime.datetime.timestamp(entry["apachelog_request_time"])
						entry["apachelog_request_time"] = entry["apachelog_request_time"].isoformat()

					if "apachelog_headers_in" in entry:
						entry["apachelog_headers_referer"] = entry["apachelog_headers_in"]["Referer"]
						entry["apachelog_headers_useragent"] = entry["apachelog_headers_in"]["User-Agent"]
						del entry["apachelog_headers_in"]

					(entry['geoip_country'], entry['geoip_city'], entry['geoip_full'] ) = do_geoip( entry["apachelog_remote_host"] )

					q = "INSERT INTO log_entries"
					q += " (" + ",".join(entry.keys()) + ")"
					q += " VALUES (" + ",".join('?' * len(entry.keys())) + ")"
					q += " ON CONFLICT DO NOTHING"
					v = entry.values()
					#print(q,"<=",v)
					cur.execute(q, list(v))
				return line_no
			except apachelogs.errors.InvalidEntryError:
				continue
		return 0

starting_log_entries = cur.execute("SELECT COUNT(*) FROM log_entries").fetchone()[0]

count = 0
start = timer()
for path in args.log_paths:
	if os.path.isdir(path):
		for filename in os.scandir(path):
			count += autoparse( filename )
	elif os.path.isfile(path):
		count += autoparse( path )
	else:
		print("abnormal path:"+path)
end = timer()

ending_log_entries = cur.execute("SELECT COUNT(*) FROM log_entries").fetchone()[0]
added_count = ending_log_entries - starting_log_entries

dur = end-start
print("Took:", str(dur), "seconds" )
print("Parsed:", count, "records(" + str(count/dur) + "/second)" )
print("Ingressed:", added_count, "records(", str(added_count/dur), "/second)")

conn.commit()
