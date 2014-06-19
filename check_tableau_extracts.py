#!/usr/bin/python

import urllib, json
import os, sys
import psycopg2
import psycopg2.extras
from optparse import OptionParser
import datetime
import re

parser = OptionParser()
parser.usage = "usage: %prog [options] tableau_datasource_name"
parser.add_option( "-p", "--port", type="string",
                   help="port to connect to tableau postgresql, default is 8060",
                   dest="port",
                   default="8060" )
parser.add_option( "-H", "--host", type="string",
                   help="host to connect to tableau postgresql, default is localhost",
                   dest="host",
                   default="localhost" )
parser.add_option( "-D", "--database", type="string",
                   help="database name to connect to tableau postgresql, default is workgroup",
                   dest="database",
                   default="workgroup" )
parser.add_option( "-U", "--user", type="string",
                   help="user to connect to tableau postgresql, default is tableau",
                   dest="user",
                   default="tableau" )
parser.add_option( "-P", "--password", type="string",
                   help="password for user to connect to tableau postgresql, default is empty",
                   dest="password",
                   default="" )
parser.add_option( "-c", "--critical", type="int",
                   help="minutes since last occurance for CRITICAL, default is 1560",
                   dest="critical",
                   default=1560)
parser.add_option( "-w", "--warning", type="int",
                   help="minutes since last occurance for WARNING, default is 1440",
                   dest="warning",
                   default=1440)
parser.add_option( "-C", "--crit-rows", type="int",
                   help="number of rows updated to be considered critical, default is 0",
                   dest="crit_rows",
                   default=0)
parser.add_option( "-W", "--warn-rows", type="int",
                   help="number of rows updated to be considered warning, default is 0",
                   dest="warn_rows",
                   default=0)
parser.add_option( "-d", "--debug", action="store_true",
                   help="turn on some debugging",
                   default=0,
                   dest="debug")

options, arguments = parser.parse_args()

def get_events(cur, datasource, time):
    select = "SELECT created_at, details FROM historical_events INNER JOIN hist_datasources \
            ON historical_events.hist_datasource_id = hist_datasources.id WHERE hist_datasources.name \
            = '" + datasource + "' AND created_at > '" + \
            str(datetime.datetime.utcnow() - datetime.timedelta(minutes = time)) + "' \
            AND ( historical_event_type_id = 133 OR historical_event_type_id = 132 ) ORDER BY created_at DESC;" 
    cur.execute(select)
    data = cur.fetchall()
    return data

def get_rows_updated(data):
    for row in data:
        print "Timezone " + str(row[0].tzinfo) + "   TIME " + str(row[0])
        matched = re.match( r'.*Rows inserted: (\d+).*', row[1])
        if matched: return int(matched.group(1))
        break

def the_exit(datasource, rows, status):
    if status == "CRITICAL":
        print "CRITICAL: " + datasource + " hasn't completed in the past " + str(options.critical) + " minutes.  Rows " + str(rows)
        sys.exit(2)
    elif status == "WARNING":
        print "WARNING: " + datasource + " hasn't completed in the past " + str(options.warning) + " minutes.  Rows " + str(rows)
        sys.exit(1)
    elif status == "OK":
        print "OK: " + datasource + " has completed successfully recently.  Rows " + str(rows)
        sys.exit(0)

def row_exit(datasource, rows, status):
    if status == "CRITICAL":
        print "CRITICAL: " + datasource + " - Looking for " + str(options.crit_rows) + ", only saw " + str(rows)
        sys.exit(2)
    elif status == "WARNING":
        print "WARNING: " + datasource + " - Looking for  " + str(options.warn_rows) + ", only saw " + str(rows)
        sys.exit(1)


def main():

    tableau_datasource_name = arguments[0]

    try:
        conn = psycopg2.connect("dbname='" + options.database + "' user='" + options.user + "' host='" + options.host + "' password='" + options.password + "' port='" + options.port + "'")
    except:
        print "I am unable to connect to the database"
        sys.exit(256)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SET TIME ZONE 'America/Chicago';")
   
    select_warn = get_events(cur, tableau_datasource_name, options.warning)
    select_crit = get_events(cur, tableau_datasource_name, options.critical)

    rows = None

    if select_crit:
        rows = get_rows_updated(select_crit)

    if options.debug: print "Events before WARNING minutes - " + str(select_warn)
    if options.debug: print "Events before CRITICAL minutes - " + str(select_crit)
    if options.debug: print str(options.warn_rows) + "  " + str(options.crit_rows)
    warning = 0
    critical = 0
    if select_crit and not select_warn:
        warning = 1
    elif not select_crit:
        critical = 1

    if rows is not None:
        if rows <= options.warn_rows and rows > options.crit_rows:
            row_exit(tableau_datasource_name, rows, "WARNING")
        elif rows <= options.crit_rows:
            row_exit(tableau_datasource_name, rows, "CRITICAL")

    if critical == 1:
        the_exit(tableau_datasource_name, rows, "CRITICAL")
    elif warning == 1:
        the_exit(tableau_datasource_name, rows, "WARNING")
    else:
        the_exit(tableau_datasource_name, rows, "OK")

if len(arguments) > 0:
    main()
else:
    print "Tableau Datasource Name is required"
    sys.exit(256)