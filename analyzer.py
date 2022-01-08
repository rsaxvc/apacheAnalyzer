from apachelogs import LogParser
import apachelogs
from timeit import default_timer as timer
import traceback
import os
import sys
import sqlite3

has_geoip = False
try:
	import geoip2.database
	has_geoip = True
except:
	pass

reader = geoip2.database.Reader('GeoLite2-City.mmdb')
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
					apachelog_line INTEGER,
					geoip_city VARCHAR,
					geoip_country VARCHAR,
					geoip_subdivision_1 VARCHAR,
					geoip_subdivision_2 VARCHAR
                    ); """)

	cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS log_entries_idx_unique on log_entries(apachelog_line,apachelog_remote_host,COALESCE(apachelog_request_line,''))""")


	cur.execute("""CREATE INDEX IF NOT EXISTS log_entries_idx_remote_host ON
					log_entries(apachelog_remote_host);""")

	cur.execute("""CREATE INDEX IF NOT EXISTS log_entries_idx_geoip ON
					log_entries(geoip_country,geoip_subdivision_1,geoip_subdivision_2,geoip_city);""")

def do_geoip( remote_host ):
	location = (None,None,None,None)
	if remote_host in geoip_cache:
		return geoip_cache[remote_host]
	else:
		try:
			lookup = reader.city(remote_host)
			location = ( str(lookup.country.name), str(lookup.subdivision_1.name), str(lookup.subdivision_2.name), str(lookup.city.name) )
			geoip_cache[remote_host] = location
			return location
		except:
			location = (None,None,None,None)
			geoip_cache[remote_host] = location
			return location

def autoparse( filename ):
	logFormats=[
		"%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"", #Combined
		"%h %l %u %t \"%r\" %>s %b" #common
		]
	with open(filename) as fp:
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
						entry["apachelog_request_time"] = entry["apachelog_request_time"].isoformat()
						
					if "apachelog_headers_in" in entry:
						entry["apachelog_headers_referer"] = entry["apachelog_headers_in"]["Referer"]
						entry["apachelog_headers_useragent"] = entry["apachelog_headers_in"]["User-Agent"]
						del entry["apachelog_headers_in"]

					(entry['geoip_country'], entry['geoip_subdivision_1'], entry['geoip_subdivision_2'], entry['geoip_city'] ) = do_geoip( entry["apachelog_remote_host"] )

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
for filename in os.scandir('logs'):
	count += autoparse( filename )
end = timer()

ending_log_entries = cur.execute("SELECT COUNT(*) FROM log_entries").fetchone()[0]
added_count = ending_log_entries - starting_log_entries

dur = end-start
print("Took:", str(dur), "seconds" )
print("Parsed:", count, "records(" + str(count/dur) + "/second)" )
print("Ingressed:", added_count, "records(", str(added_count/dur), "/second)")

conn.commit()