import rpyc
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
from node_dao import NodeDAO
import logging
import sys
import time
import os

# States of Raft node
LEADER = "LEADER"
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"

# AppendRPC return states
SUCCESS = "SUCCESS"
TERM_INCONSISTENCY = "TERM_INCONSISTENCY"
NEXT_INDEX_INCONSISTENCY = "NEXT_INDEX_INCONSISTENCY"

# AppendRPC type
JOINT_CONFIGURATION = "JOINT_CONFIGURATION"
NEW_CONFIGURATION = "NEW_CONFIGURATION"


class RaftService(rpyc.Service):
    config_reader = ConfigReader("../config/config.ini")
    node_dao = NodeDAO()

    # stable log => (index, term, value)
    term, voted_for, stable_log, blog = node_dao.initialize_persistence_files(0, -1, list(), list())

    # Initializing commit_index based on blog
    # If blog is empty, nothing committed
    if not blog:
        commit_index = -1
    # Else, commit index is the last index of the blog
    else:
        commit_index = len(blog) - 1

    # Server log setup
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
    heartBeatInterval = int(config_reader.get_heartbeat_interval())
    majority_criteria = int(config_reader.get_majority_criteria())
    interrupt = False
    leader_id = -1
    should_i_die = False

    # Duplicate Config information to smooth out config change
    majority_criteria_old = int(config_reader.get_majority_criteria())
    total_nodes_old = int(config_reader.get_total_nodes())
    peers_old = config_reader.get_peers(server_id, total_nodes)

    @staticmethod
    def check_majority(votes):
        # During normal operation the criteria are the same values
        # During config change they will be different values
        if votes >= RaftService.majority_criteria and votes >= RaftService.majority_criteria_old:
            return True
        else:
            return False

    def switch_to_joint_config(self, new_majority, new_total_nodes, new_peers):
        RaftService.logger.info("Switching ..%s"%RaftService.server_id)
        RaftService.majority_criteria_old = RaftService.majority_criteria
        RaftService.total_nodes_old = RaftService.total_nodes
        RaftService.peers_old = RaftService.peers

        RaftService.majority_criteria = new_majority
        RaftService.total_nodes = new_total_nodes
        RaftService.peers = new_peers


    def switch_to_new_config(self):
        # Assumes you are running in Joint config mode

        if RaftService.server_id == RaftService.leader_id:
            #If you are leader send a heart beat to others
            # Connect to peers and send heartbeats
            time.sleep(20)

        RaftService.logger.info("Back from sending heartbeat")

        RaftService.majority_criteria_old = RaftService.majority_criteria
        RaftService.total_nodes_old = RaftService.total_nodes
        RaftService.peers_old = RaftService.peers

        if RaftService.should_i_die:
            RaftService.logger.info("Stepping down as I am not part of new config")
            #sys.exit(0)
            os._exit(0)

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
        RaftService.voted_for = RaftService.server_id
        RaftService.node_dao.persist_vote_and_term(RaftService.voted_for, RaftService.term)

        total_votes = RaftService.request_votes()

        # Check Majority
        if total_votes == -1:
            RaftService.logger.warning("Voting was interrupted by external factor")
            RaftService.state = FOLLOWER
            RaftService.reset_and_start_timer()

        elif RaftService.check_majority(total_votes):
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
        RaftService.logger.info("Sending heartbeat")
        for peer in RaftService.peers_old:
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
                RaftService.logger.warning("send_heartbeat: Unable to connect to server %d" % peer[0])


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
        for peer in RaftService.peers_old:
            try:
                vote = False
                connection = rpyc.connect(peer[1], peer[2], config={"allow_public_attrs": True})
                vote = connection.root.requestRPC(term=RaftService.term,
                                                  candidate_id=RaftService.server_id,
                                                  last_log_index=last_index,
                                                  last_log_term=last_term)

                RaftService.logger.info("Vote received: " + str(vote))
                if vote:
                    RaftService.logger.info("Received vote from server %d for leader election, term %d"
                                            % (peer[0], RaftService.term))
                    total_votes = total_votes + 1
            except Exception as details:
                print details


            RaftService.logger.warning("request_votes: Unable to connect to server %d" % peer[0])  # +1 to account for self-vote

        return total_votes + 1


    def exposed_requestRPC(self, term, candidate_id, last_log_index, last_log_term):
        my_vote = False
        RaftService.logger.info("Received requestRPC: candidate term: %d, my_term: %d" % (term, RaftService.term))


        if RaftService.term == term:
            # Check if I had voted to this candidate previously for this term. If YES, re-iterate my vote
            if RaftService.voted_for == candidate_id:
                my_vote = True
                RaftService.node_dao.persist_vote_and_term(RaftService.voted_for, RaftService.term)
            else:
                RaftService.logger.info("Server %s has already vote this term (%s) to %s" % (
                RaftService.server_id, RaftService.term, RaftService.voted_for))

        elif term < RaftService.term:
            RaftService.logger.info("Stale term of candidate %s" % candidate_id)

        elif term > RaftService.term:
            log_index, log_term = self.get_last_log_index_and_term()
            RaftService.logger.info(
                "In requestRPC: candidate_last_log_term: %d, my_last_log_term: %d, candidate_last_log_index: %d, my_last_log_index: %d" % (
                last_log_term, log_term, last_log_index, log_index))

            if last_log_term >= log_term and last_log_index >= log_index:
                my_vote = True
                RaftService.reset_and_start_timer()
                RaftService.logger.info("Voting YES to candidate %s" % candidate_id)
                # TODO Need Review on this
                RaftService.term = term
                RaftService.voted_for = candidate_id
                RaftService.state = FOLLOWER
                RaftService.node_dao.persist_vote_and_term(RaftService.voted_for, RaftService.term)
            else:
                RaftService.logger.info("Something went wrong. Shouldn't print this...")

        return my_vote


    def exposed_config_changeRPC(self, list_of_config_changes, client_id):
        new_config_change_success = False
        print list_of_config_changes
        RaftService.logger.info("Received Configuration Change Request from client %s" % client_id)
        if RaftService.server_id != RaftService.leader_id:
            try:
                RaftService.logger.info("Redirecting the request to Leader %s" % RaftService.server_id)
                (ip, port) = RaftService.config_reader.get_leaders_port_ip(RaftService.leader_id, RaftService.peers_old)
                connection = rpyc.connect(ip, port, config={"allow_public_attrs": True})
                new_config_change_success = connection.root.exposed_config_change_leaderRPC(list_of_config_changes, client_id)

            except Exception as details:
                print details
        else:

            entry = (JOINT_CONFIGURATION, list_of_config_changes)
            joint_config_change_success = self.append_entries(entry, client_id)
            if joint_config_change_success:
                # Joint consensus is running. So start new config now
                #time.sleep(20)
                new_config_change_success = self.append_entries((NEW_CONFIGURATION,list_of_config_changes), client_id)
                if new_config_change_success:
                    RaftService.logger.info("Successfully changed the configuration of the system.")
                else:
                    RaftService.logger.info("Couldnt change the configuration of system to new config.")
            else:
                RaftService.logger.info("Couldn't change the configuration of the system to joint config.")

        return new_config_change_success


    def exposed_config_change_leaderRPC(self, list_of_config_changes, client_id):
        new_config_change_success = False

        RaftService.logger.info("Received Configuration via Redirection from client %s" % client_id)
        joint_config_change_success = self.append_entries((JOINT_CONFIGURATION,list_of_config_changes), client_id)

        if joint_config_change_success:
            #time.sleep(20)
            # Joint consensus is running. So start new config now
            new_config_change_success = self.append_entries((NEW_CONFIGURATION,list_of_config_changes), client_id)
            if new_config_change_success:
                RaftService.logger.info("Successfully changed the configuration of the system.")
            else:
                RaftService.logger.info("Couldn't change the configuration of system to new config.")
        else:
            RaftService.logger.info("Couldn't change the configuration of the system to joint config.")

        return new_config_change_success


    def exposed_lookupRPC(self):
        blogs = RaftService.blog
        return blogs


    def exposed_postRPC(self, blog, client_id):
        return_value = False

        RaftService.logger.info("Received Post from client %s" % client_id)
        if RaftService.server_id != RaftService.leader_id:
            try:
                (ip, port) = RaftService.config_reader.get_leaders_port_ip(RaftService.leader_id, RaftService.peers_old)
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


    def append_entries(self, item_to_replicate, client_id):
        # This code is to be executed by the LEADER
        # The driver of this method is Client or Followers forwarding client requests

        # 1 Replicate the item_to_replicate
        # (index, term, value, commit_status)
        previous_log_index, previous_log_term = RaftService.get_last_log_index_and_term()
        RaftService.logger.info("Prev Index %s Prev Term %s" % (previous_log_index, previous_log_term))
        entry = (previous_log_index + 1, RaftService.term, item_to_replicate)
        RaftService.stable_log.append(entry)
        
        #RaftService.node_dao.persist_log(RaftService.stable_log)


        entries = list()
        entries.append(entry)

        # 2 Send RPCs and wait for majority
        if RaftService.state == LEADER:
            total_votes = self.replicate_log(entries, previous_log_index, previous_log_term) + 1

            if RaftService.check_majority(total_votes):
                RaftService.logger.info(
                        "Reached consensus to replicate %s, %s" % (previous_log_index + 1, RaftService.term))
                RaftService.commit_index = RaftService.commit_index + 1
                self.update_state_machine()
            else:
                RaftService.logger.info("Reached no majority")
        else:
            RaftService.logger.warning("I aint no leader. Somebody called me by accident!")

        return True


    def replicate_log(self, entries, prev_log_index, prev_log_term):
        total_votes = 0

        # TODO Redundant Code Ah Man!
        for peer in RaftService.peers_old:

            # TODO For now, as the dude doesnt fail, the entries are what client asks to replicate
            # TODO Remove this dangerous guy at once!
            # TODO Does it make sense to sleep for a while and try again network failure errors
            previous_log_index = prev_log_index
            previous_log_term = prev_log_term
            z = 0
            while True:
                #z = z+1
                #if z > 3:
                    #RaftService.logger.info("Breaking ...")
                    #break;
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
                        RaftService.logger.warning("Shouldn't have reached here. something is wrong")

                except Exception as details:
                    RaftService.logger.info(details)
                    RaftService.logger.warning("replicate_log: Unable to connect to server %d" % peer[0])
                    #time.sleep(3)
                    #TODO put the thread to sleep and try again. Cos we try again

        return total_votes


    def get_entries_from_index(index):
        entries = list()
        tuple_ = RaftService.stable_log[index]
        previous_log_term = tuple_[1]
        for i in range(index, len(RaftService.stable_log)):
            entries.append(RaftService.stable_log[i])

        return entries, previous_log_term


    def run_config_change(self, log_entry):
        
        try:
            RaftService.logger.info("Running config change %s"%log_entry[0])
            config_change_list = log_entry[1]
            mode = log_entry[0]

            if JOINT_CONFIGURATION == mode:
                new_total_nodes = RaftService.total_nodes
                new_peers = RaftService.peers_old

                for item_to_replicate in config_change_list:
                    command = item_to_replicate[0]
                    local_id = int(item_to_replicate[1])
                    
                    if command == "ADD":
                        ip = item_to_replicate[2]
                        port = int(item_to_replicate[3])
                        new_peers.append((local_id, ip, port))
                        new_total_nodes = new_total_nodes + 1
                    elif command == "REMOVE":
                        if (local_id == RaftService.server_id):
                            RaftService.should_i_die = True
                        new_peers = self.config_reader.get_new_peers_by_removing(local_id, new_peers)
                        new_total_nodes = new_total_nodes - 1
                    else:
                        print "Reached else conditon."

                new_majority_criteria = int(new_total_nodes / 2) + 1
                self.switch_to_joint_config(new_majority_criteria, new_total_nodes, new_peers)

            elif NEW_CONFIGURATION == mode:
                self.switch_to_new_config()

            else:
                RaftService.logger.info("Wrong mode called for applying to state machine")
        except Exception as details:
            RaftService.logger.warning(details)
        RaftService.logger.info("Leaving Run Config Change%s"%RaftService.server_id)


    def exposed_append_entriesRPC(self,
                                  leaders_term,
                                  leaders_id,
                                  previous_log_index,
                                  previous_log_term,
                                  entries,
                                  commit_index):


        #RaftService.logger.info("In method append entries RPC %s %s %s %s %s %s"%(leaders_term,
                                 # leaders_id,
                                 # previous_log_index,
                                 # previous_log_term,
                                  #entries,
                                  #commit_index))
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

        if entries is not None:  # Not a heartbeat, entries to append

            RaftService.logger.info("Received appendRPC from %d" % leaders_id)
            # Get my last log index and last log index term
            my_prev_log_index, my_prev_log_entry_term = RaftService.get_last_log_index_and_term()
            my_next_index = my_prev_log_index + 1

            # Check if next index matches. If not, send Inconsistency error and next index of the Follower
            if previous_log_index != my_prev_log_index:
                my_next_index = my_next_index - 1
                RaftService.logger.info("Previous Log Index: %s My Prev Log Index: %s"%(previous_log_index, my_prev_log_index))
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
                my_next_index = my_next_index + 1

            RaftService.node_dao.persist_log(RaftService.stable_log)
            
            #if RaftService.commit_index < commit_index:
                #RaftService.commit_index = commit_index
                #self.update_state_machine()
                #print "Back to append entries"
            #else:
                #RaftService.logger.info("Nothing to update in commmit index %s %s"%(RaftService.commit_index, commit_index))
            
            RaftService.logger.info("Reply to AppendRPC: Sending SUCCESS to %d" % leaders_id)
            return (RaftService.term, SUCCESS, my_next_index)

        else:
            if RaftService.leader_id != leaders_id:
                RaftService.leader_id = leaders_id

            RaftService.logger.info("Received HeartBeat from %d, my leader is %d" % (leaders_id, RaftService.leader_id))

            if RaftService.commit_index < commit_index:
                RaftService.commit_index = commit_index
                self.update_state_machine()
                RaftService.logger.info("Done updating machine after heartbeat %d"%RaftService.server_id)
        


    def update_state_machine(self):
        blog_last_index = len(RaftService.blog) - 1

        try:
            # Check if stable_log exists till commit_index
            if RaftService.commit_index <= (len(RaftService.stable_log) - 1):
                old_blog = RaftService.blog
                new_blogs = list()
                for i in range(len(RaftService.blog),RaftService.commit_index+1):
                    current_entry = RaftService.stable_log[i]
                    value = current_entry[2]
                    if not isinstance(value, basestring):
                        RaftService.logger.info("Calling Run Config inside update state machine %s"%RaftService.server_id)
                        self.run_config_change(value)
                    new_blogs.append(value)

                old_blog.append(new_blogs)
                RaftService.blog = old_blog
                # Persist blog
                RaftService.node_dao.persist_blog(RaftService.blog)
            else:
                print "Commit Index > Stable log index ??? How did this happen !!"
        except Exception as details:
            print details

        RaftService.logger.info("Exiting update state machine %s"%RaftService.server_id)


    @staticmethod
    def get_last_log_index_and_term():
        tuple = 0, 0, 0, False
        # If stable_log is not empty
        if RaftService.stable_log:
            tuple = RaftService.stable_log[-1]

        return tuple[0], tuple[1]


    def print_stable_log(self):
        for tuple in RaftService.stable_log:
            RaftService.logger.info("%s %s %s" % (tuple[0], tuple[1], tuple[2]))


if __name__ == "__main__":
    RaftService.logger.info(
        "Starting Server %d with Peers %s Term: %d, Voted_for: %d, Stable log: %s, Blog: %s, Commit Index: %d" % (
        RaftService.server_id, RaftService.peers, RaftService.term, RaftService.voted_for, RaftService.stable_log,
        RaftService.blog, RaftService.commit_index))
    RaftService.start_election_timer()
    my_port = RaftService.id_ip_port[2]
    t = ThreadedServer(RaftService, port=my_port, protocol_config={"allow_public_attrs": True, "allow_pickle":True})
    t.start()
