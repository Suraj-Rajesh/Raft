import rpyc
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
import pickle
import os

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

    # Initializing TERM and VOTED_FOR parameters
    term = 0
    voted_for = -1

    # If persistence values already exists, load it
    if os.path.exists("../persistence/term_and_voted_for_parameters.p"):
        voted_for, term = pickle.load(open
                          ("../persistence/term_and_voted_for_parameters.p", "rb"))

    else:  # This is the first time this node is running
       pickle.dump((voted_for, term), open
                  ("../persistence/term_and_voted_for_parameters.p", "wb"))
        
    # Initializing STABLE LOG
    stable_log = list()  # (index, term, value, commit_status)

    # If persistent stable log already exists, load it
    if os.path.exists("../persistence/stable_log.p"):
        stable_log = pickle.load(open("../persistence/stable_log.p", "rb"))

    else:  # This is the first time this node is running, dump empty log
       pickle.dump(stable_log, open("../persistence/stable_log.p", "wb"))

    state = FOLLOWER
    electionTimer = 0
    heartBeatTimer = 0
    server_id = int(config_reader.get_configuration("CurrentServer", "sid"))
    ip_port = config_reader.get_server_parameters("Server" + str(server_id))
    total_nodes = int(config_reader.get_total_nodes())
    timeout_parameter = int(config_reader.get_election_timeout_period())
    peers = config_reader.get_peers(server_id, total_nodes)
    connection = 0
    heartBeatInterval = config_reader.get_heartbeat_interval()
    majority_criteria = int(config_reader.get_majority_criteria())
    interrupt = False
    leader_id = -1
    have_i_vote_this_term = False

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

    @staticmethod
    def start_election_timer():
        # Election timeout to be a random value between T and 2T
        timeout = randint(RaftService.timeout_parameter, 2 * RaftService.timeout_parameter)
        RaftService.electionTimer = threading.Timer(timeout, RaftService.start_election)
        RaftService.electionTimer.start()

    @staticmethod
    def start_heartbeat_timer():
        # Once I'm the LEADER, start sending heartbeat messages to peers
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
                    peerConneciton.append_entriesRPC(term = RaftService.term, 
                                                     entries = None, 
                                                     commit_index = RaftService.commmit_index) 
                except Exception as details:
                    print details

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
                RaftService.voted_for = candidate_id
                self.persist_vote_and_term()

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

    def persist_vote_and_term(self):
        tuple = (RaftService.voted_for, RaftService.term)
        pickle.dump(tuple, open("../persistence/term_and_voted_for_parameters.p", "wb"))

    def read_persisted_vote_and_term(self):
        (RaftService.voted_for, RaftService.term) = pickle.load(
                                open("../persistence/term_and_voted_for_parameters.p", "rb"))

    def persist_log(self):
        pickle.dump(RaftService.stable_log, open("../persistence/stable_log.p", "wb"))

    def read_persisted_log(self):
        RaftService.stable_log = pickle.load(open("../persistence/stable_log.p", "rb"))

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

        # AppendRPC received, need to reset my election timer
        RaftService.reset_and_start_timer()

        # If my term is less than leader's, update my term
        if leaders_term > RaftService.term:
             RaftService.term = leaders_term
 
        # If I think I am the LEADER/CANDIDATE, real LEADER sent me an appendRPC, step down
        if RaftService.state == LEADER or RaftService.state == CANDIDATE:
            # If I am the LEADER, stop sending heartbeats
            if RaftService.state == LEADER:
               RaftService.heartBeatTimer.cancel()
            # Finally, change the state to FOLLOWER
            RaftService.state = FOLLOWER

        # TODO Check commit_index and update state machine(blogs) accordingly

        
        if entries is not None:  # Not a heartbeat, entries to append

            # Get my last log index and last log index term
            my_prev_log_index, my_prev_log_entry_term = RaftService.get_last_log_index_and_term()
            my_next_index = my_prev_log_index + 1
 
            # Check if next index matches. If not, send Inconsistency error and next index of the Follower
            if leader_prev_log_index != my_prev_log_index:
                return (RaftService.term, NEXT_INDEX_INCONSISTENCY, my_next_index)
 
            # Check if previous log entry matches previous log term
            # If not, send Term Inconsistency error and next index of the Follower
            if leader_prev_log_term != my_prev_log_entry_term:
                return (RaftService.term, TERM_INCONSISTENCY, my_next_index)
  
            # Log consistency check successful. Append entries to log, persist on disk, send SUCCESS
            RaftService.stable_log.append(entries)
            self.persist_log()
            return (RaftService.term, SUCCESS, my_next_index)

        else:
            print "Received HeartBeat"


if __name__ == "__main__":
    print "Starting Server %d with Peers %s" % (RaftService.server_id, RaftService.peers)
    RaftService.start_election_timer()
    t = ThreadedServer(RaftService, port=RaftService.ip_port[1], protocol_config={"allow_public_attrs": True})
    t.start()
