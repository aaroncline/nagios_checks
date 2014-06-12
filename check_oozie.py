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
    
    # Read in previous data if exists
    if os.path.isfile(options.file):
        with open(options.file, 'r') as previous_file:
            previous_data = json.load(previous_file)

    # If the previous data was empty, grab all data
    # Process list from previous data, If I have previously run this script, let's just check new actions that have run since then
    # Else, I will check them all
    url = "http://" + options.host + ":" + options.port + "/oozie/v1/job/" + coordID + '?show=info'
    for coord in previous_data:
        if coord['id'] == coordID:
            url = "http://" + options.host + ":" + options.port + "/oozie/v1/job/" + coordID + '?show=info&offset=' + str(coord['lastActionNumber'])

    # Get JSON from Oozie
    response = urllib.urlopen(url)
    data = json.loads(response.read())

    # Cycle through JSON looking for items that are NOT SUCCEEDED and count them
    bad_action = 0
    for action in data['actions']:
        if action['status'] != "SUCCEEDED":
            bad_action = bad_action + 1

    # Now find the last existing 'actionNumber' and add it to our previous_data
    # If the ID is new, append this new id and number to the previous_data
    lastActionNumber = data['actions'][-1]['actionNumber']
    for coord in previous_data:
        if coord['id'] == coordID:
            coord['lastActionNumber'] = lastActionNumber
            break
    else:
        previous_data.append({ 'id' : coordID, 'lastActionNumber' : lastActionNumber })

    # Write previous_data to file as JSON
    with open(options.file, 'w') as previous_file: 
        json.dump(previous_data, previous_file)

    # Exit as either CRITICAL, WARNING, or OK
    if options.critical <= bad_action:
        print "CRITICAL: " + options.host + " had " + str(bad_action) + " bad actions."
        sys.exit(2)
    elif options.warning <= bad_action:
        print "WARNING: " + options.host + " had " + str(bad_action) + " bad actions."
        sys.exit(1)
    else:
        print "OK: " + options.host + " had " + str(bad_action) + " bad actions."
        sys.exit(0)



if len(arguments) > 0:
    main()
else:
    print "Oozie Coordinator ID is required."
    sys.exit(256)

