import os
import sys
import rpyc

try:
    conn = rpyc.connect("localhost", 18861, config = {"allow_public_attrs" : True})
except Exception as details:
    print "Exception caught"
    print details
    sys.exit(0)

server = conn.root
server.interruptTimer()
