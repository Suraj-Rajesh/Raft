import rpyc
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
from persistence_manager import PersistenceManager
import pickle

# Possible states of Raft node
LEADER    = "leader"
FOLLOWER  = "follower"
CANDIDATE = "candidate"


class RaftService(rpyc.Service):
    config_reader = ConfigReader("../config/config.ini")
    persistence_manager = PersistenceManager("../persistence/persistence.ini")

    state = FOLLOWER
    electionTimer = 0
    heartBeatTimer = 0
    server_id = int(config_reader.getConfiguration("CurrentServer", "sid"))
    ip_port = config_reader.get_ip_and_port_of_server("Server" + str(server_id))
    total_nodes = int(config_reader.getTotalNodes())
    timeoutLower = int(config_reader.electionTimeoutPeriod())  # Election timeout timer to be between, T to 2T (random)
    peers = config_reader.get_peers(server_id, total_nodes)
    connection = 0
    term = int(persistence_manager.getCurrentTerm())
    heartBeatInterval = config_reader.getHeartBeatInterval()
    majority_criteria = int(config_reader.get_majority_criteria())
    interrupt = False
    leader_id = -1
    voted_for = -1
    have_i_vote_this_term = False
    stable_log = list()  # (index, term, value)

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

    @staticmethod
    def startElectionTimer():
        # Election timeout to be a random value between T and 2T
        timeout = randint(RaftService.timeoutLower, 2 * RaftService.timeoutLower)
        RaftService.electionTimer = threading.Timer(timeout, RaftService.startElection)
        RaftService.electionTimer.start()

    @staticmethod
    def startHeartBeatTimer():
        # Once LEADER, start sending heartbeat messages(empty AppendRPC messages) to peers
        RaftService.heartBeatTimer = threading.Timer(RaftService.heartBeatInterval, RaftService.triggerNextHeartBeat)
        RaftService.heartBeatTimer.start()

    @staticmethod
    def triggerNextHeartBeat():

        if RaftService.state == LEADER:
            threading.Thread(target=RaftService.startHeartBeatTimer).start()

            RaftService.sendHeartBeat()

    @staticmethod
    def sendHeartBeat():
        for peer in RaftService.peers:
            peerConnection = RaftService.connect(peer)

            if peerConnection is not None:
                try:
                    peerConnection.heartBeat()
                except Exception as details:
                    print details

    def exposed_heartBeat(self):
        print "Received HeartBeat"
        RaftService.resetAndStartTimer()

    def exposed_appendRPC():
        # Implement AppendRPC here
        pass

    @staticmethod
    def resetAndStartTimer():
        RaftService.electionTimer.cancel()
        RaftService.startElectionTimer()

    def exposed_requestRPC(self, term, candidate_id, last_log_index, last_log_term):

        my_vote = False
        if RaftService.have_i_vote_this_term:
            print "Server %s has already vote this term (%s) to %s" % (
                RaftService.server_id, RaftService.term, RaftService.voted_for)
        elif term < RaftService.term:
            print "Stale Term of candidate %s" % candidate_id
        else:
            log_index, log_term = self.get_last_log_index_and_term()
            if last_log_term >= log_term and last_log_index >= log_index:
                my_vote = True
                print "Voting Yes to candidate %s" % candidate_id
                RaftService.voted_for = candidate_id
                # TODO Need Review on this
                RaftService.term = term
                self.persist_parameters()

        return my_vote

    @staticmethod
    def request_votes():

        total_votes = 0
        last_index, last_term = RaftService.get_last_log_index_and_term()

        # TODO Run this concurrently
        # Suggestion: Create a separate RPC call to handle response. This RPC only requests for vote.
        # For now we assume that the network wont fail
        for peer in RaftService.peers:

            if RaftService.interrupt:
                return -1

            try:
                peer_connection = RaftService.connect(peer)
                if peer_connection != None:
                    vote = peer_connection.requestRPC(term=RaftService.term,
                                                      candidate_id=RaftService.server_id,
                                                      last_log_index=last_index, last_log_term=last_term)
                    if vote:
                        total_votes = total_votes + 1

            except Exception as details:
                print details

        # +1 to account for self-vote
        return total_votes + 1

    # TODO: Review this method. Now, connecting to global "RaftService.connection"
    @staticmethod
    def connect(peer):
        print "Connecting to: " + peer[0] 
        try:
            ip_address = peer[0]
            port = peer[1]
            RaftService.connection = rpyc.connect(ip_address, port, config={"allow_public_attrs": True})
            peerConnection = RaftService.connection.root
            return peerConnection

        except Exception as details:
            print "Exception here"
            print details
            return None

    # Once election timer times out, need to start the election
    @staticmethod
    def startElection():

        print "Starting election for server %s" % (RaftService.server_id)
        RaftService.state = CANDIDATE
        RaftService.term = RaftService.term + 1
        RaftService.have_i_vote_this_term = True
        total_votes = RaftService.request_votes()

        # Check Majority
        if total_votes == -1:
            print "Voting was interrupted by external factor"
            RaftService.state = FOLLOWER
            RaftService.resetAndStartTimer()

        elif total_votes >= RaftService.majority_criteria:
            RaftService.leader_id = RaftService.server_id
            RaftService.state = LEADER
            # Send HeartBeat immediately and then setup regular heartbeats
            RaftService.startHeartBeatTimer()
            print "Successfully Elected New Leader %s " % RaftService.leader_id

        else:
            # Step Down
            RaftService.state = FOLLOWER
            RaftService.resetAndStartTimer()

    # Testing peers interrupting election timer
    # TODO Remove if not needed
    def exposed_interruptTimer(self):
        RaftService.resetAndStartTimer()

    @staticmethod
    def get_last_log_index_and_term():
        tuple = 0, 0, 0
        # If stabe_log is not empty
        if RaftService.stable_log:
            tuple = RaftService.stable_log[-1]

        return tuple[0], tuple[1]

    def persist_parameters(self):
        # TODO Optimize saving with and without log
        tuple = (RaftService.voted_for, RaftService.term, RaftService.stable_log)
        pickle.dump(tuple, open("persist_parameters.p", "wb"))

    def read_persisted_parameters(self):
        (RaftService.voted_for, RaftService.term, RaftService.stable_log) = pickle.load(
                open("persist_parameters.p", "rb"))


    def append_entries(self):
        #You are the leaeder asking peers to replicate information

        #1 Replicate the blog
        #2 Wait for majority
        #3 If successful,
            #3a Run it on state machine
            #3b Tell client replication is done
            #3c Ask peers to run it on their state machine
        #4 If failed, exit gracefully, tell the client
        #5 If you are sending heart beat just sent it with empty log
        pass

    def append_entriesRPC(self):

        #1 Perform Consistency Checks
            #a The term and log index
            #b
        #2
        pass


if __name__ == "__main__":
    print "Starting Server %d with Peers %s" % (RaftService.server_id, RaftService.peers)
    RaftService.startElectionTimer()
    t = ThreadedServer(RaftService, port = RaftService.ip_port[1], protocol_config={"allow_public_attrs": True})
    t.start()
