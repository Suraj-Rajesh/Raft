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

# AppendRPC return states
SUCCESS                  = "SUCCESS"
TERM_INCONSISTENCY       = "TERM_INCONSISTENCY"
NEXT_INDEX_INCONSISTENCY = "NEXT_INDEX_INCONSISTENCY"

class RaftService(rpyc.Service):
    config_reader = ConfigReader("../config/config.ini")
    persistence_manager = PersistenceManager("../persistence/persistence.ini")

    state = FOLLOWER
    electionTimer = 0
    heartBeatTimer = 0
    server_id = int(config_reader.get_configuration("CurrentServer", "sid"))
    ip_port = config_reader.get_server_parameters("Server" + str(server_id))
    total_nodes = int(config_reader.get_total_nodes())
    timeout_parameter = int(config_reader.get_election_timeout_period())
    peers = config_reader.get_peers(server_id, total_nodes)
    connection = 0
    term = int(persistence_manager.getCurrentTerm())
    heartBeatInterval = config_reader.get_heartbeat_interval()
    majority_criteria = int(config_reader.get_majority_criteria())
    interrupt = False
    leader_id = -1
    voted_for = -1
    have_i_vote_this_term = False
    stable_log = list()  # (index, term, value, commit_status)

    next_indices = dict()
    match_indices = dict()
    commit_index = -1
    blog = list()

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
        # Once he is a LEADER, start sending heartbeat messages to peers
        RaftService.heartBeatTimer = threading.Timer(RaftService.heartBeatInterval, RaftService.trigger_next_heartbeat)
        RaftService.heartBeatTimer.start()

    @staticmethod
    def trigger_next_heartbeat():
        if RaftService.state == LEADER:
            threading.Thread(target=RaftService.start_heartbeat_timer).start()
            RaftService.send_heartbeat()

    @staticmethod
    def send_heartbeat():
        # Connect to peers and send heartbeats
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

             if RaftService.state == LEADER:
                RaftService.heartBeatTimer.cancel()

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

            # TODO This may not work in multi threaded environment
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
        print "Connecting to: " + peer[1]
        try:
            ip_address = peer[1]
            port = peer[2]
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
        # TODO You have to reset this to False when the term changes
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
        tuple = 0, 0, 0, False
        # If stable_log is not empty
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

    def append_entries(self, blog, client_id):
        # This code is to be executed by the LEADER
        # The driver of this method is Client or Followers forwarding client requests

        entries = list()
        entries.append(blog)

        # 1 Replicate the blog
        # (index, term, value, commit_status)
        previous_log_index, previous_log_term = RaftService.get_last_log_index_and_term()
        RaftService.stable_log.append((previous_log_index + 1, RaftService.term, blog, False))

        # 2 Send RPCs and wait for majority
        if RaftService.state == LEADER:

            total_votes = RaftService.replicate_log(entries, previous_log_index,previous_log_term) + 1

            if total_votes >= RaftService.majority_criteria:
                print "Reached consensus to replicate %s, %s"%(previous_log_index+1, RaftService.term)
                RaftService.apply_log_on_state_machine(blog)
                RaftService.respond_to_client()
                RaftService.replicate_state_machine()

            else:
                print "Reached no majority"
                RaftService.respond_to_client()

        else:
            print "I aint no leader. Somebody called me by accident!"

    def replicate_log(self, entries, previous_log_index,previous_log_term):

        # TODO Redundant Code Ah Man!
        for peer in RaftService.peers:
            try:
                # TODO For now, as the dude doesnt fail, the entries are what client asks to replicate
                # TODO Remove this dangerous guy at once!
                # TODO Does it make sense to sleep for a while and try again network failure errors
                while True:
                    RaftService.update_indices_try_again()
                    peer_connection = RaftService.connect(peer)
                    if peer_connection != None:
                        term, status, next_index = peer_connection.append_entriesRPC(term=RaftService.term,
                                                                                     leader_id=RaftService.server_id,
                                                                                     previous_log_index=previous_log_index,
                                                                                     previous_log_term=previous_log_term,
                                                                                     entries=entries,
                                                                                     commit_index=RaftService.commit_index)
                    if status == "SUCCESS":
                        total_votes = total_votes + 1
                        # next_index = previous_log_index+1
                        break

            except Exception as details:
                print details

    def replicate_state_machine(self):
        pass

    def apply_log_on_state_machine(self, blog):
        RaftService.blog.append(blog)

    def respond_to_client(self):
        #TODO Respond actually
        print "Responded to client"

    def update_indices_try_again(self):
        #TODO Waiting on append entries RPC impl
        pass

    def exposed_append_entriesRPC(self, 
                                  leader_term, 
                                  leaders_id,
                                  leader_prev_log_index,
                                  leader_prev_log_term,
                                  entries,
                                  commit_index):

        # If my term is less than leader's, update my term
        if leaders_term > RaftService.term:
             RaftService.term = leaders_term
 
         if RaftService.state == LEADER or RaftService.state == CANDIDATE:

             # If I the LEADER, stop sending heartbeats, change state to successor
             if RaftService.state == LEADER:
                RaftService.heartBeatTimer.cancel()

             RaftService.state = FOLLOWER

        # Get my last log index and last log index term
         last_log_index_term = RaftService.get_last_log_index_and_term()
 
         # Check if next index matches. If not, send Inconsistency error and next index of the Follower
         if leader_prev_log_index != last_log_index_term[0]:
             return (RaftService.term, NEXT_INDEX_INCONSISTENCY, last_log_index_term[0] + 1)
 
         # Check if previous log entry matches previous log term
         # If not, send Term Inconsistency error and next index of the Follower
         if leader_prev_log_term != last_log_index_term[1]:
             return (RaftService.term, TERM_INCONSISTENCY, last_log_index_term[0] + 1)
  
         # Log consistency check successful. Append entries to log and send Success
         RaftService.stable_log.append(entries)
         # TODO: Store it on persistent disk
         return (RaftService.term, SUCCESS, last_log_index_term[0] + 1)


if __name__ == "__main__":
    print "Starting Server %d with Peers %s" % (RaftService.server_id, RaftService.peers)
    RaftService.start_election_timer()
    t = ThreadedServer(RaftService, port=RaftService.ip_port[1], protocol_config={"allow_public_attrs": True})
    t.start()
