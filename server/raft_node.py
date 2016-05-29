import rpyc
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
from node_dao import NodeDAO
import logging

# States of Raft node
LEADER = "LEADER"
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"

# AppendRPC return states
SUCCESS = "SUCCESS"
TERM_INCONSISTENCY = "TERM_INCONSISTENCY"
NEXT_INDEX_INCONSISTENCY = "NEXT_INDEX_INCONSISTENCY"


class RaftService(rpyc.Service):
    config_reader = ConfigReader("../config/config.ini")
    node_dao = NodeDAO()

    # stable log => (index, term, value, commit_status)
    term, voted_for, stable_log = node_dao.initialize_persistence_files(0, -1, list())

    # TODO @Suraj, Can you move this to a method?
    logger = logging.getLogger("raft_node")
    log_handler = logging.FileHandler("../log/raft_node.log")
    formatter = logging.Formatter("%(levelname)s %(message)s")
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    state = FOLLOWER
    electionTimer = 0
    heartBeatTimer = 0
    server_id = int(config_reader.get_configuration("CurrentServer", "sid"))
    id_ip_port = config_reader.get_server_parameters("Server" + str(server_id))
    total_nodes = int(config_reader.get_total_nodes())
    timeout_parameter = int(config_reader.get_election_timeout_period())
    peers = config_reader.get_peers(server_id, total_nodes)
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
        pass

    def on_disconnect(self):
        # code that runs when a connection closes
        pass

    @staticmethod
    def start_election_timer():
        # Election timeout to be a random value between T and 2T
        timeout = randint(RaftService.timeout_parameter, 2 * RaftService.timeout_parameter)
        RaftService.electionTimer = threading.Timer(timeout, RaftService.start_election)
        RaftService.electionTimer.start()

    @staticmethod
    def start_election():

        RaftService.logger.info("Starting election for server %s" % (RaftService.server_id))
        RaftService.state = CANDIDATE
        RaftService.term = RaftService.term + 1
        RaftService.have_i_vote_this_term = True
        # TODO You have to reset this to False when the term changes
        total_votes = RaftService.request_votes()

        # Check Majority
        if total_votes == -1:
            RaftService.logger.info("Voting was interrupted by external factor")
            RaftService.state = FOLLOWER
            RaftService.reset_and_start_timer()

        elif total_votes >= RaftService.majority_criteria:
            RaftService.leader_id = RaftService.server_id
            RaftService.state = LEADER
            # Send HeartBeat immediately and then setup regular heartbeats
            RaftService.start_heartbeat_timer()
            RaftService.logger.info("Successfully elected New Leader %s " % RaftService.leader_id)

        else:
            # Step Down
            RaftService.state = FOLLOWER
            RaftService.reset_and_start_timer()

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
            try:
                connection = rpyc.connect(peer[1], peer[2], config={"allow_public_attrs": True})
                connection.root.append_entriesRPC(leaders_term=RaftService.term,
                                                  leaders_id=RaftService.server_id,
                                                  previous_log_index=None,
                                                  previous_log_term=None,
                                                  entries=None,
                                                  commit_index=RaftService.commit_index)
            except Exception as details:
                print details
                RaftService.logger.info("send_heartbeat: Unable to connect to server %d" % peer[0])

    @staticmethod
    def reset_and_start_timer():
        RaftService.electionTimer.cancel()
        RaftService.start_election_timer()

    @staticmethod
    def request_votes():

        total_votes = 0
        last_index, last_term = RaftService.get_last_log_index_and_term()

        # TODO Run this concurrently
        # Suggestion: Create a separate RPC call to handle response. This RPC only requests for vote.
        # For now we assume that the network wont fail
        for peer in RaftService.peers:
            try:
                vote = False
                connection = rpyc.connect(peer[1], peer[2], config={"allow_public_attrs": True})
                vote = connection.root.requestRPC(term=RaftService.term,
                                                  candidate_id=RaftService.server_id,
                                                  last_log_index=last_index,
                                                  last_log_term=last_term)

                if vote:
                    RaftService.logger.info("Received vote from server %d for leader election, term %d"
                                            % (peer[0], RaftService.term))
                    total_votes = total_votes + 1

            except Exception as details:
                RaftService.logger.info("request_votes: Unable to connect to server %d" % peer[0])

        # +1 to account for self-vote
        return total_votes + 1

    def exposed_requestRPC(self, term, candidate_id, last_log_index, last_log_term):

        my_vote = False
        if RaftService.have_i_vote_this_term:
            RaftService.logger.info("Server %s has already vote this term (%s) to %s" % (
                RaftService.server_id, RaftService.term, RaftService.voted_for))
        elif term < RaftService.term:
            RaftService.logger.info("Stale term of candidate %s" % candidate_id)
        else:
            log_index, log_term = self.get_last_log_index_and_term()
            if last_log_term >= log_term and last_log_index >= log_index:
                my_vote = True
                RaftService.reset_and_start_timer()
                RaftService.logger.info("Voting YES to candidate %s" % candidate_id)
                RaftService.voted_for = candidate_id
                # TODO Need Review on this
                RaftService.term = term
                RaftService.voted_for = candidate_id
                RaftService.have_i_vote_this_term = True
                RaftService.node_dao.persist_vote_and_term(RaftService.voted_for, RaftService.term)

        return my_vote

    def exposed_lookupRPC(self):
        return RaftService.blog

    def exposed_postRPC(self, blog, client_id):

        return_value = False

        RaftService.logger.info("Received Post from client %s" % client_id)
        if RaftService.server_id != RaftService.leader_id:
            try:
                (ip, port) = RaftService.config_reader.get_leaders_port_ip(RaftService.leader_id, RaftService.peers)
                connection = rpyc.connect(ip, port, config={"allow_public_attrs": True})
                return_value = connection.root.exposed_post_leaderRPC(blog, client_id)

            except Exception as details:
                RaftService.logger.info(details)
        else:
            return_value = self.append_entries(blog, client_id)

        return return_value

    def exposed_post_leaderRPC(self, blog, client_id):

        RaftService.logger.info("Received Post from client %s" % client_id)
        return self.append_entries(blog, client_id)

    def append_entries(self, blog, client_id):
        # This code is to be executed by the LEADER
        # The driver of this method is Client or Followers forwarding client requests

        RaftService.logger.info("Received a call finally from some dude. Take it from here")

        # 1 Replicate the blog
        # (index, term, value, commit_status)
        previous_log_index, previous_log_term = RaftService.get_last_log_index_and_term()
        RaftService.logger.info("Prev Index %s Prev Term %s" % (previous_log_index, previous_log_term))
        entry = (previous_log_index + 1, RaftService.term, blog, False)
        RaftService.stable_log.append(entry)
        entries = list()
        entries.append(entry)

        # 2 Send RPCs and wait for majority
        if RaftService.state == LEADER:
            total_votes = self.replicate_log(entries, previous_log_index, previous_log_term) + 1

            if total_votes >= RaftService.majority_criteria:
                RaftService.logger.info(
                        "Reached consensus to replicate %s, %s" % (previous_log_index + 1, RaftService.term))
                self.apply_log_on_state_machine(blog)
            else:
                RaftService.logger.info("Reached no majority")
        else:
            RaftService.logger.info("I aint no leader. Somebody called me by accident!")

        return True

    def replicate_log(self, entries, prev_log_index, prev_log_term):

        total_votes = 0

        # TODO Redundant Code Ah Man!
        for peer in RaftService.peers:

            # TODO For now, as the dude doesnt fail, the entries are what client asks to replicate
            # TODO Remove this dangerous guy at once!
            # TODO Does it make sense to sleep for a while and try again network failure errors
            previous_log_index = prev_log_index
            previous_log_term = prev_log_term

            while True:
                try:
                    connection = rpyc.connect(peer[1], peer[2], config={"allow_public_attrs": True})
                    term, status, next_index = connection.root.append_entriesRPC(leaders_term=RaftService.term,
                                                                                 leaders_id=RaftService.server_id,
                                                                                 previous_log_index=previous_log_index,
                                                                                 previous_log_term=previous_log_term,
                                                                                 entries=entries,
                                                                                 commit_index=RaftService.commit_index)

                    if status == SUCCESS:
                        RaftService.logger.info("Received Success from %s" % peer[0])
                        total_votes = total_votes + 1
                        break

                    elif status == TERM_INCONSISTENCY or status == NEXT_INDEX_INCONSISTENCY:
                        RaftService.logger.info("Received term inconsistency from %s. Next index %s Term %s" % (
                            peer[0], term, next_index))
                        entries, previous_log_term = self.get_entries_from_index((next_index - 1))
                        previous_log_index = next_index - 1
                    else:
                        RaftService.logger.info("Shouldnt have reached here. something is wrong")

                except Exception as details:
                    RaftService.logger.info("replicate_log: Unable to connect to server %d" % peer[0])

        return total_votes

    def get_entries_from_index(index):
        entries = list()
        tuple_ = RaftService.stable_log[index]
        previous_log_term = tuple_[1]
        for i in range(index, len(RaftService.stable_log)):
            entries.append(RaftService.stable_log[i])

        return entries, previous_log_term

    def replicate_state_machine(self):
        pass

    def apply_log_on_state_machine(self, blog):
        RaftService.blog.append(blog)

    def exposed_append_entriesRPC(self,
                                  leaders_term,
                                  leaders_id,
                                  previous_log_index,
                                  previous_log_term,
                                  entries,
                                  commit_index):

        RaftService.logger.info(
                "You have Successfully made the RPC call to %s from %s" % (RaftService.server_id, leaders_id))

        # TODO Isnt this for heartbeat alone? Seems like overkill @SURAJ
        # AppendRPC received, need to reset my election timer
        RaftService.reset_and_start_timer()

        # If my term is less than leader's, update my term
        if leaders_term > RaftService.term:
            # real LEADER sent me an appendRPC, may be I am an old leader who needs to be neutralized
            if RaftService.state == LEADER:
                RaftService.heartBeatTimer.cancel()
                RaftService.state = FOLLOWER

            RaftService.term = leaders_term

        # If I am the CANDIDATE step down
        if RaftService.state == CANDIDATE:
            RaftService.state = FOLLOWER

        # TODO Check commit_index and update state machine accordingly

        if entries is not None:  # Not a heartbeat, entries to append

            RaftService.logger.info("Received appendRPC from %d" % leaders_id)
            # Get my last log index and last log index term
            my_prev_log_index, my_prev_log_entry_term = RaftService.get_last_log_index_and_term()
            my_next_index = my_prev_log_index + 1

            # Check if next index matches. If not, send Inconsistency error and next index of the Follower
            if previous_log_index != my_prev_log_index:
                my_next_index = my_next_index - 1
                RaftService.logger.info("Reply to AppendRPC: Sending NEXT_INDEX_INCONSISTENCY to  %d" % leaders_id)
                return (RaftService.term, NEXT_INDEX_INCONSISTENCY, my_next_index)

            # Check if previous log entry matches previous log term
            # If not, send Term Inconsistency error and next index of the Follower
            if previous_log_term != my_prev_log_entry_term:
                my_next_index = my_next_index - 1
                RaftService.logger.info("Reply to AppendRPC: Sending TERM_INCONSISTENCY to  %d" % leaders_id)
                return (RaftService.term, TERM_INCONSISTENCY, my_next_index)

            # Log consistency check successful. Append entries to log, persist on disk, send SUCCESS
            for entry in entries:
                RaftService.stable_log.append(entry)
                # TODO not sure if this is needed
                my_next_index = my_next_index + 1

            RaftService.logger.info("Log after appending ...")
            self.print_stable_log()
            RaftService.node_dao.persist_log(RaftService.stable_log)
            RaftService.logger.info("Reply to AppendRPC: Sending SUCCESS to %d" % leaders_id)
            return (RaftService.term, SUCCESS, my_next_index)

        else:
            if RaftService.leader_id != leaders_id:
                RaftService.leader_id = leaders_id
            RaftService.logger.info("Received HeartBeat from %d, my leader is %d" % (leaders_id, RaftService.leader_id))

        RaftService.logger.info("Leaving appendRPC ...")

    @staticmethod
    def get_last_log_index_and_term():
        tuple = 0, 0, 0, False
        # If stable_log is not empty
        if RaftService.stable_log:
            tuple = RaftService.stable_log[-1]

        return tuple[0], tuple[1]

    def print_stable_log(self):
        for tuple in RaftService.stable_log:
            RaftService.logger.info("%s %s %s %s" % (tuple[0], tuple[1], tuple[2], tuple[3]))


if __name__ == "__main__":
    RaftService.logger.info("Starting Server %d with Peers %s" % (RaftService.server_id, RaftService.peers))
    RaftService.start_election_timer()
    my_port = RaftService.id_ip_port[2]
    t = ThreadedServer(RaftService, port=my_port, protocol_config={"allow_public_attrs": True})
    t.start()
