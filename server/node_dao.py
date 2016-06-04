import pickle
import os

class NodeDAO:

    def __init__(self):
        self.path_to_persist_vote = "../persistence/term_and_voted_for_parameters.p"
        self.path_to_persist_log = "../persistence/stable_log.p"
        self.path_to_persist_blog = "../persistence/blog.p"
        self.path_to_server_log = "../log/raft_node.log"

    def persist_vote_and_term(self, voted_for, term):
        tuple = (voted_for, term)
        pickle.dump(tuple, open(self.path_to_persist_vote, "wb"))

    def read_persisted_vote_and_term(self):
        (voted_for, term) = pickle.load(
                open(self.path_to_persist_vote, "rb"))
        return (voted_for, term)

    def persist_log(self, stable_log):
        pickle.dump(stable_log, open(self.path_to_persist_log, "wb"))

    def read_persisted_log(self):
        stable_log = pickle.load(open(self.path_to_persist_log, "rb"))
        return stable_log

    def persist_blog(self, blog):
        pickle.dump(blog, open(self.path_to_persist_blog, "wb"))

    def read_persisted_blog(self):
        blog = pickle.load(open(self.path_to_persist_blog, "rb"))
        return blog

    def initialize_persistence_files(self, voted_for, term, stable_log, blog):

        # Vote and Term
        if os.path.exists(self.path_to_persist_vote):
            voted_for, term = self.read_persisted_vote_and_term()
        else:
            pickle.dump((voted_for, term), open(self.path_to_persist_vote, "wb"))

        # Stable Log
        if os.path.exists(self.path_to_persist_log):
            stable_log = self.read_persisted_log()
        else:
            pickle.dump(stable_log, open(self.path_to_persist_log, "wb"))

	# Blog
        if os.path.exists(self.path_to_persist_blog):
            blog = self.read_persisted_blog()
        else:
            pickle.dump(blog, open(self.path_to_persist_blog, "wb"))

        #Server Log
        if os.path.exists(self.path_to_server_log):
            os.remove(self.path_to_server_log)

        return voted_for, term, stable_log, blog
