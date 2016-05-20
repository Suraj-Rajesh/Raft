import rpyc
import sys
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
from persistence_manager import PersistenceManager

# Possible states of Raft node
LEADER = "leader"
FOLLOWER = "follower"
CANDIDATE = "candidate"


class RaftService(rpyc.Service):
    state = FOLLOWER
    electionTimer = 0
    config_reader = ConfigReader("../config/config.ini")
    persistence_manager = PersistenceManager("../persistence/persistence.ini")
    server_id = int(config_reader.getConfiguration("CurrentServer", "sid"))
    # Election timeout timer to be between, T to 2T (random)
    timeoutLower = int(config_reader.electionTimeoutPeriod())
    peers = list()

    def on_connect(self):
        # code that runs when a new connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when a connection closes
        # (to finalize the service, if needed)
        pass

    def exposed_get_id(self):  # this is an exposed method
        return RaftService.server_id

    def exposed_method_1(self, param1, param2):
        return_value = 'You are stinky pants :P #!~*&^'
        print "Welcome to Server %s" % RaftService.server_id
        print "Received parameters %s, %s" % (param1, param2)
        print "Responding to caller with value %s" % return_value
        return return_value

    def exposed_method_for_client_to_call(self, param1, param2):
        return_value = 'Client called me :P #!~*&^'
        print "Welcome to Server %s" % RaftService.server_id
        print "Received parameters %s, %s" % (param1, param2)

        print "Let me call another local method which makes RPC"
        RaftService.local_guy_who_makes_rpc()

        print "Responding to client with value %s" % return_value
        return return_value

    def local_guy_who_makes_rpc(self):

        try:
            conn = rpyc.connect("localhost", 18861, config={"allow_public_attrs": True})
            remote_guy = conn.root.exposed_method_1(param1="Whhatzaaaaa from leader",
                                                                   param2="Bow before your King")
            print remote_guy
        except Exception as details:
            print "Exception caught"
            print details
            sys.exit(0)

    # Use this method for debug log
    def print_data(self, msg, operation):
        print "**************** Operation***************\nLog: %s\n" % (RaftService.log)

    @staticmethod
    def startElectionTimer():
        # Election timeout to be a random value between T and 2T
        timeout = randint(RaftService.timeoutLower, 2 * RaftService.timeoutLower)
        RaftService.electionTimer = threading.Timer(timeout, RaftService.startElection)
        RaftService.electionTimer.start()

    @staticmethod
    def resetAndStartTimer():
        RaftService.electionTimer.cancel()
        #   RaftService.electionTimer.join()
        RaftService.startElectionTimer()

    # Once election timer times out, need to start the election
    @staticmethod
    def startElection():
        # Election conduction code follows
        print "Starting election now !!"

        # Once election done, reset Timer and start again
        RaftService.resetAndStartTimer()

    # Testing peers interrupting election timer
    def exposed_interruptTimer(self):
        RaftService.resetAndStartTimer()


if __name__ == "__main__":
    RaftService.startElectionTimer()
    t = ThreadedServer(RaftService, port=18861, protocol_config={"allow_public_attrs": True})
    t.start()
