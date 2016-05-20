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
server.exposed_method_for_client_to_call(param1="I am the client. here is the value",
                                                                   param2=10)
