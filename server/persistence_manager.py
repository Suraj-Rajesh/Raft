import ConfigParser

class PersistenceManager:

    def __init__(self, config_file_path):

        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file_path)

    def getPersistenceInfo(self, section, entry):
        try:
            return self.config.get(section, entry)
        except Exception as details:
            print details
            return None

    def getCurrentTerm(self):
        
        try:
            return self.config.get("termDetails", "currentTerm")
        except Exception as details:
            print details
            return None

    def setCurrentTerm(self, termNo):
        
        try:
            self.config.set("termDetails", "currentTerm", termNo)
        except Exception as details:
            print details
            return None

    def iVotedTo(self):
        
        try:
            return self.config.get("termDetails", "votedFor")
        except Exception as details:
            print details
            return None
    
    def vote(self, voteId):
        
        try:
            self.config.set("termDetails", "votedFor", voteId)
        except Exception as details:
            print details
            return None
