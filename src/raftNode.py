import rpyc
import threading
from random import randint
from rpyc.utils.server import ThreadedServer
from config_reader import ConfigReader
from persistence_manager import PersistenceManager
import pickle

# Possible states of Raft node
LEADER = "leader"
FOLLOWER = "follower"
CANDIDATE = "candidate"


class RaftService(rpyc.Service):
    config_reader = ConfigReader("../config/config.ini")
    persistence_manager = PersistenceManager("../persistence/persistence.ini")

    state = FOLLOWER
    electionTimer = 0
    server_id = int(config_reader.getConfiguration("CurrentServer", "sid"))
    total_servers = int(config_reader.getTotalNodes())
    timeoutLower = int(config_reader.electionTimeoutPeriod())  # Election timeout timer to be between, T to 2T (random)
    peers = config_reader.get_peers(server_id, total_servers)
    term = 1
    majority_criteria = 2
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
    def resetAndStartTimer():
        RaftService.electionTimer.cancel()
        RaftService.startElectionTimer()

    def exposed_request_vote(self, term, candidate_id, last_log_index, last_log_term):

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

        # TODO Run this concurrently
        # For now we assume that the network wont fail
        for peer in RaftService.peers:

            if RaftService.interrupt:
                return -1

            try:
                peer_connection = RaftService.connect(peer)
                if peer_connection != None:
                    vote = peer_connection.request_vote()
                    if vote:
                        total_votes = total_votes + 1

            except Exception as details:
                print details

        return total_votes

    @staticmethod
    def connect(peer):
        try:
            ip_address = peer[0]
            port = peer[1]
            connection = rpyc.connect(ip_address, port, config={"allow_public_attrs": True})
            peerConnection = connection.root
            return peerConnection

        except Exception as details:
            print details
            return None

    # Once election timer times out, need to start the election
    @staticmethod
    def startElection():

        print "Starting election for server %s" % (RaftService.server_id)
        RaftService.state = CANDIDATE
        RaftService.term = RaftService.term + 1
        total_votes = RaftService.request_votes()

        # Check Majority
        if total_votes == -1:
            print "Voting was interrupted by external factor"
            RaftService.state = FOLLOWER

        elif total_votes >= RaftService.majority_criteria:
            RaftService.leader_id = RaftService.server_id
            RaftService.state = LEADER
            print "Successfully Elected New Leader %s " % RaftService.leader_id

        else:
            # Step Down
            RaftService.state = FOLLOWER

        # Once election is done, reset Timer and start again
        RaftService.resetAndStartTimer()

    # Testing peers interrupting election timer
    # TODO Remove if not needed
    def exposed_interruptTimer(self):
        RaftService.resetAndStartTimer()

    def get_last_log_index_and_term(self):

        tuple = RaftService.stable_log[-1]
        return tuple[0], tuple[1]

    def persist_parameters(self):
        # TODO Optimize saving with and without log
        tuple = (RaftService.voted_for, RaftService.term, RaftService.stable_log)
        pickle.dump(tuple, open("persist_parameters.p", "wb"))

    def read_persisted_parameters(self):
        (RaftService.voted_for, RaftService.term, RaftService.stable_log) = pickle.load(
            open("persist_parameters.p", "rb"))


if __name__ == "__main__":
    RaftService.startElectionTimer()
    t = ThreadedServer(RaftService, port=18861, protocol_config={"allow_public_attrs": True})
    t.start()
