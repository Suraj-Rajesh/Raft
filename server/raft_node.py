import rpyc
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
from persistence_manager import PersistenceManager
import pickle

# States of Raft node
LEADER    = "LEADER"
FOLLOWER  = "FOLLOWER"
CANDIDATE = "CANDIDATE"

# AppendRPC return values
SUCCESS                  = "SUCCESS"
TERM_INCONSISTENCY       = "TERM_INCONSISTENCY"
NEXT_INDEX_INCONSISTENCY = "NEXT_INDEX_INCONSISTENCY"

class RaftService(rpyc.Service):

    config_reader = ConfigReader("../config/config.ini")
    persistence_manager = PersistenceManager("../persistence/persistence.ini")

    state = FOLLOWER
    electionTimer = 0
    heartBeatTimer = 0
    server_id = int(config_reader.getConfiguration("CurrentServer", "sid"))
    ip_port = config_reader.get_ip_and_port_of_server("Server" + str(server_id))
    total_nodes = int(config_reader.getTotalNodes())
    timeout_parameter = int(config_reader.electionTimeoutPeriod())
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
    def start_election_timer():
        # Election timeout to be a random value between T and 2T
        timeout = randint(RaftService.timeout_parameter, 2 * RaftService.timeout_parameter)
        RaftService.electionTimer = threading.Timer(timeout, RaftService.start_election)
        RaftService.electionTimer.start()

    @staticmethod
    def start_heartbeat_timer():
        # Once LEADER, start sending heartbeat messages(empty AppendRPC messages) to peers
        RaftService.heartBeatTimer = threading.Timer(RaftService.heartBeatInterval, RaftService.trigger_next_heartbeat)
        RaftService.heartBeatTimer.start()

    @staticmethod
    def trigger_next_heartbeat():

        if RaftService.state == LEADER:
            threading.Thread(target=RaftService.start_heartbeat_timer).start()

            RaftService.send_heartbeat()

    @staticmethod
    def send_heartbeat():
        for peer in RaftService.peers:
            peerConnection = RaftService.connect(peer)

            if peerConnection is not None:
                try:
                    peerConnection.heartBeat(RaftService.term)
                except Exception as details:
                    print details

    def exposed_heartbeat(self, leaders_term):
        print "Received HeartBeat"
        if leaders_term > RaftService.term:
            RaftService.term = leaders_term

        if RaftService.state == LEADER or RaftService.state == CANDIDATE:
            RaftService.state = FOLLOWER

        RaftService.reset_and_start_timer()

    @staticmethod
    def reset_and_start_timer():
        RaftService.electionTimer.cancel()
        RaftService.start_election_timer()

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
    def start_election():

        print "Starting election for server %s" % (RaftService.server_id)
        RaftService.state = CANDIDATE
        RaftService.term = RaftService.term + 1
        RaftService.have_i_vote_this_term = True
        #TODO You have to reset this to False when the term changes
        total_votes = RaftService.request_votes()

        # Check Majority
        if total_votes == -1:
            print "Voting was interrupted by external factor"
            RaftService.state = FOLLOWER
            RaftService.reset_and_start_timer()

        elif total_votes >= RaftService.majority_criteria:
            RaftService.leader_id = RaftService.server_id
            RaftService.state = LEADER
            # Send HeartBeat immediately and then setup regular heartbeats
            RaftService.start_heartbeat_timer()
            print "Successfully Elected New Leader %s " % RaftService.leader_id

        else:
            # Step Down
            RaftService.state = FOLLOWER
            RaftService.reset_and_start_timer()

    # Testing peers interrupting election timer
    # TODO Remove if not needed
    def exposed_interrupt_timer(self):
        RaftService.reset_and_start_timer()

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
        #This code is to be executed by the LEADER
        #The driver of this method is Client or Followers forwarding client requests

        if RaftService.state == LEADER:
            pass
        else:
            print "You are not the leader. Wrongly called this method!"

        #1 Replicate the blog
        #2 Wait for majority
        #3 If successful,
            #3a Run it on state machine
            #3b Tell client replication is done
            #3c Ask peers to run it on their state machine
        #4 If failed, exit gracefully, tell the client
        #5 If you are sending heart beat just sent it with empty log

    def exposed_appendentriesRPC(self, leader_term, leaders_id, leader_prev_log_index, leader_prev_log_term, entries, commit_index):

        # Get my last log index and term
        last_log_index_term = RaftService.get_last_log_index_and_term()

        # Check if next index matches. If not, send Inconsistency error and next index of the Follower
        if leader_prev_log_index != last_log_index_term[0]:
            return (NEXT_INDEX_INCONSISTENCY, last_log_index_term[0] + 1)

        # Check if previous log entry matches previous log term
        # If not, send Term Inconsistency error and next index of the Follower
        if leader_prev_log_term != last_log_index_term[1]:
            return (TERM_INCONSISTENCY, last_log_index_term[0] + 1)

        # Log consistency check successful. Append entries to log and send Success
        RaftService.stable_log.append(entries)
        # TODO: Store it on persistent disk
        return SUCCESS

if __name__ == "__main__":

    print "Starting Server %d with Peers %s" % (RaftService.server_id, RaftService.peers)
    RaftService.start_election_timer()
    t = ThreadedServer(RaftService, port = RaftService.ip_port[1], protocol_config={"allow_public_attrs": True})
    t.start()
