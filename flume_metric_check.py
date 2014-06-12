#!/usr/bin/python

import urllib, json

import os, sys
from optparse import OptionParser



parser = OptionParser()
parser.usage = "usage: %prog [options] host1 host2 ..."
parser.add_option( "-p", "--port", type="string",
                   help="port to connect to flume metrics, default is 4140",
                   dest="port",
                   default="4140" )
parser.add_option( "-C", "--channels", type="string",
                   help="comma delimited list of channels to look for",
                   dest="unsplit_channels")
parser.add_option( "-c", "--critical", type="float",
                   help="percentage to be considered critical, default is 10",
                   dest="critical",
                   default=10.000)
parser.add_option( "-w", "--warning", type="float",
                   help="channel filled percentage to be considered warning, default is 5",
                   dest="warning",
                   default=5.000)
parser.add_option( "-d", "--debug", action="store_true",
                   help="turn on some debugging",
                   default=0,
                   dest="debug")

options, arguments = parser.parse_args()



def main():
  channels = options.unsplit_channels.split(',')
  critical_hosts = {}
  warning_hosts = {}

  for host in arguments:
    url = "http://" + host + ":" + options.port + "/metrics"
    response = urllib.urlopen(url)
    data = json.loads(response.read())
    found_channel = 0

    for channel in channels:
      
      if (channel in data):
	found_channel += 1
	if options.debug: print host + "  " + channel + "  " + str(float(data[channel]['ChannelFillPercentage']))
      if (channel in data) and (float(data[channel]['ChannelFillPercentage']) > options.critical):
	if not host in critical_hosts:
	  critical_hosts[host] = {}
	critical_hosts[host][channel] = float(data[channel]['ChannelFillPercentage'])
	if options.debug: print "Found a critical host " + host + "  " + str(float(data[channel]['ChannelFillPercentage']))
      elif (channel in data) and (float(data[channel]['ChannelFillPercentage']) > options.warning):
	if not host in warning_hosts:
	  warning_hosts[host] = {}
	warning_hosts[host][channel] = float(data[channel]['ChannelFillPercentage'])
	if options.debug: print "Found a warning host " + host + "  " + str(float(data[channel]['ChannelFillPercentage']))
    
    if found_channel == 0:
      print "Did not find any specified channels on this host " + host
      if not host in critical_hosts:
	critical_hosts[host] = {}
      critical_hosts[host]['UNAVAILABLE'] = "NA"

  if len(critical_hosts) > 0:
    host_string = ""
    for host in critical_hosts:
      for channel in critical_hosts[host]:
	host_string = host_string + host + ":" + channel + "--" + str(critical_hosts[host][channel]) + "%  "

    for host in warning_hosts:
      for channel in warning_hosts[host]:
	host_string = host_string + host + ":" + channel + "--" + str(warning_hosts[host][channel]) + "%  "
    
    print "CRITICAL - The following hosts showed critical or warning: " + host_string
    sys.exit(2)

  if len(warning_hosts) > 0:
    host_string = " "
    for host in warning_hosts:
      for channel in warning_hosts[host]:
	host_string = host_string + host + ":" + channel + "--" + str(warning_hosts[host][channel]) + "%  "

    print "WARNING - The following hosts show warning: " + host_string
    sys.exit(1)

  print "OK"
  sys.exit(0)






if options.unsplit_channels:
  main()
else:
  print "ERROR -- Expected Arguments not given!"
  parser.print_help()



