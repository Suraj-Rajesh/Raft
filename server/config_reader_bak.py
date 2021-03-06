import ConfigParser

class ConfigReader:

    def __init__(self, config_file_path):

        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file_path)

    def get_leaders_port_ip(self, leader_id, peers):
        for peer in peers:
            if peer[0] == leader_id:
                return (peer[1],peer[2])
        
        return None

    def get_peers(self, server_id, total_servers):
        peers = list()
        for i in [x for x in range(total_servers) if x != server_id]:
            peers.append(self.get_server_parameters("Server" + str(i)))
        return peers

    def get_server_parameters(self, section):
        try:
            peer_id = int(self.get_configuration(section,"id"))
            ip = self.get_configuration(section, "ip")
            port = int(self.get_configuration(section, "port"))
            return (peer_id, ip, port)

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

    def get_new_peers_by_removing(self, id_server_to_remove, peers):
        
        new_peers = list()
        for peer in peers:
            if peer[0] != id_server_to_remove:
                new_peers.append(peer)

        return new_peers
