# ApacheAnalyzer

ApacheAnalyzer is a set of Python tools for parsing apache logs into sqlite and generating queryable results.
It's designed for a small, single-server use-case.

## Installation

1) Place a copy of GeoLite2-City.mmdb in the project directory, analyzer.py will need it.
2) Run analyzer.py against a directory or individual log files to generate logdb.db
3) Run analysis tools(the other py files) against the logdb.db database

# Usage

## Ingress

```python3 analyzer.py [/path/to/logfile] [/path/to/log/dir] ```

This will parse a logfile or directory and generate or update the sqlite database logdb.db in the current working directory.

Both plain text and gzip'd logs are supported.

Currently only the [NCSA Common Log Format](https://en.wikipedia.org/wiki/Common_Log_Format) and Combined Log Format
are processed.

## Usage - Search and stuff

### The tools

* fetchLogs.py - fetch individual logs
* start_stop.py - emit start and stop of current available logs
* timeline.py - emit (unix, count) tuples suitable for plotting
* top_hosts.py - emit (hostname, count) tuples sorted by max(count)

## Usage - output formats

Most tools support at least a few of html, json, csv, python-text, all(for smoke testing), so give --outputFmt=? a try.

## Usage - CGI

Most tools are also CGI scripts! You can put them in an appropriately secured location to enable remote access(default in CGI mode is HTML output)

# Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

# Security

## Access Control
This tool does handle access control. That's on you.

Most webservers allow setting a password for a directory or requiring an SSL client certificate.

## SQL
Data backend is a SQLite database, so:

* Analysis tools set query_only = ON; before handling.
* While SQL queries are constructed at runtime, they are still parameterized, so no user-input is easily injectable.

## HTML
User-controlled content is escaped using the HTML module, we feel pretty good about this.

## Ingestion
Log parsing is done using apachelogs LogParser module - this vector hasn't been looked into yet.

