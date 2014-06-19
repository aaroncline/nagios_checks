#!/usr/bin/python

import urllib, json
import os, sys
from optparse import OptionParser

parser = OptionParser()
parser.usage = "usage: %prog [options] oozie_coordinator_id"
parser.add_option( "-p", "--port", type="string",
                   help="port to connect to oozie api, default is 11000",
                   dest="port",
                   default="11000" )
parser.add_option( "-H", "--host", type="string",
                   help="host to connect to oozie api, default is localhost",
                   dest="host",
                   default="localhost" )
parser.add_option( "-c", "--critical", type="int",
                   help="number of failures to be considered critical, default is 1",
                   dest="critical",
                   default=1)
parser.add_option( "-w", "--warning", type="int",
                   help="number of failures to be considered warning, default is 1",
                   dest="warning",
                   default=1)
parser.add_option( "-f", "--file", type="string",
                    help="file to store previous data, default is /tmp/check_oozie.data",
                    dest="file",
                    default='/tmp/check_oozie.data')
parser.add_option( "-d", "--debug", action="store_true",
                   help="turn on some debugging",
                   default=0,
                   dest="debug")

options, arguments = parser.parse_args()

def main():

    coordID = arguments[0]
    previous_data = []
    ok_status = [ "SUCCEEDED", "RUNNING" ]
    warn_status = [ "WAITING" ]

    
    # Read in previous data if exists
    if os.path.isfile(options.file):
        with open(options.file, 'r') as previous_file:
            previous_data = json.load(previous_file)

    # Find if there is existing data for the coordinator and pop it out so we can work with it
    existing_coord = {}
    index = 0
    for coord in previous_data:
        if coord['id'] == coordID:
            existing_coord = previous_data.pop(index)
            break
        else:
            index = index + 1

    # If the previous data was empty, grab all data
    # If there was previous data, let's just get the new data
    url = "http://" + options.host + ":" + options.port + "/oozie/v1/job/" + coordID + '?show=info'
    if existing_coord:
        url = "http://" + options.host + ":" + options.port + "/oozie/v1/job/" + coordID + '?show=info&offset=' + str(coord['lastActionNumber'])

    # Get JSON from Oozie
    response = urllib.urlopen(url)
    data = json.loads(response.read())

    # Grab the actionNumber of the last action taken by the coordinator
    if data:
        lastActionNumber = data['actions'][-1]['actionNumber']
    else:
        print "No DATA!!"
        sys.exit(256)

    # If there was previous data, only count a new action as bad.  Prevents constant alerts if last encountered action FAILED
    # Cycle through JSON looking for items that are NOT SUCCEEDED and count them
    warning = None
    ok = None
    last_action_status = data['actions'][-1]['status']
    if last_action_status in warn_status:
        warning = 1
    elif last_action_status in ok_status:
        ok = 1

    # Append data back into previous data
    previous_data.append({ 'id' : coordID, 'lastActionNumber' : lastActionNumber })

    # Write previous_data to file as JSON
    with open(options.file, 'w') as previous_file: 
        json.dump(previous_data, previous_file)

    # Exit as either CRITICAL, WARNING, or OK
    if warning == 1:
        print "WARNING: " + options.host + " last action completed with " + last_action_status
        sys.exit(1)
    elif ok == 1:
        print "OK: " + options.host + " last action completed with " + last_action_status
        sys.exit(0)
    else:
        print "CRITICAL: " + options.host + " last action completed with " + last_action_status
        sys.exit(2)

if len(arguments) > 0:
    main()
else:
    print "Oozie Coordinator ID is required."
    sys.exit(256)

