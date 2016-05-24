import ConfigParser

class ConfigReader:

    def __init__(self, config_file_path):

        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file_path)

    def get_peers(self, total_servers):
        peers = list()
        for i in range(total_servers):
            peers.append(self.get_server_parameters("Server" + str(i)))
        return peers

    def get_server_parameters(self, section):
        try:
            id = self.get_configuration(section,"id")
            ip = self.get_configuration(section, "ip")
            port = int(self.get_configuration(section, "port"))
            return (id, ip, port)

        except Exception as details:
            print details
            return None

    def get_total_nodes(self):
        return int(self.get_configuration("GeneralConfig", "nodes"))

    def get_heartbeat_interval(self):
        return int(self.get_configuration("GeneralConfig", "heartBeatInterval"))

    def get_configuration(self, section, entry):
        """
        Parameters:
        section : section in [] in config.ini
        entry   : entry under corresponding section

        Gets configuration details defined in "config.ini" file
        After calling this function, check if None returned, handle exception accordingly
        """
        try:
            return self.config.get(section, entry)
        except Exception as details:
            print details
            return None
    

    def get_election_timeout_period(self):
        try:
            return self.config.get("GeneralConfig", "electionTimeoutLower")
        except Exception as details:
            print details
            return None

    def get_majority_criteria(self):
        try:
            return self.config.get("GeneralConfig", "majority_criteria")
        except Exception as details:
            print details
            return None
