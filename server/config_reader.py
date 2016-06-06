import ConfigParser

class ConfigReader:

    def __init__(self, config_file_path):

        self.config = ConfigParser.ConfigParser()
        self.config_file_path = config_file_path

    def get_leaders_port_ip(self, leader_id, peers):
        for peer in peers:
            if peer[0] == leader_id:
                return (peer[1],peer[2])
        
        return None

    def get_peers(self, server_id, total_servers):
        peers = list()
    
        self.config.read(self.config_file_path)
        for i in [x for x in range(15) if x != server_id]:

            section = "Server" + str(i)

            if self.config.has_section(section):
                peer = self.get_server_parameters(section)
                peers.append(peer)

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
            self.config.read(self.config_file_path)
            return self.config.get(section, entry)
        except Exception as details:
            print details
            return None
    

    def get_election_timeout_period(self):
        try:
            self.config.read(self.config_file_path)
            return self.config.get("GeneralConfig", "electionTimeoutLower")
        except Exception as details:
            print details
            return None

    def get_majority_criteria(self):
        try:
            self.config.read(self.config_file_path)
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


    def set_total_nodes(self, new_node_count):

        try:
            self.config.set("GeneralConfig", "nodes", new_node_count)

            with open(self.config_file_path, "wb") as config_file:
                self.config.write(config_file)

        except Exception as details:
            print details
            
    def set_majority_criteria(self, new_majority_criteria):

        try:
            self.config.set("GeneralConfig", "majority_criteria", new_majority_criteria)

            with open(self.config_file_path, "wb") as config_file:
                self.config.write(config_file)

        except Exception as details:
            print details

    def remove_server_from_config(self, server_id):

        try:
            self.config.remove_section("Server" + str(server_id))

            with open(self.config_file_path, "wb") as config_file:
                self.config.write(config_file)

        except Exception as details:
            print details

    # ip should be in string format. e.g., "10.12.45.66"
    def add_server_in_config(self, server_id, ip, port):

        try:

            new_server_section = "Server" + str(server_id)

            self.config.add_section(new_server_section)
            self.config.set(new_server_section, "id", server_id)
            self.config.set(new_server_section, "ip", ip)
            self.config.set(new_server_section, "port", port)

            with open(self.config_file_path, "wb") as config_file:
                self.config.write(config_file)

        except Exception as details:
            print details

    def update_config_file(self, my_id, new_total_nodes, new_majority_criteria, new_peers):

        try:

            # Save my details
            if self.config.has_section("Server" + str(my_id)):
                (server_id, server_ip, server_port) = self.get_server_parameters("Server" + str(my_id))
            else:
                print "My own server details missing"

            # Remove all Server entries
            for i in range(15):
                if self.config.has_section("Server" + str(i)):
                    self.remove_server_from_config(i)

            # Update my details
            self.add_server_in_config(server_id, server_ip, server_port)

            # Update my peers
            for peer in new_peers:
                self.add_server_in_config(peer[0], peer[1], peer[2])

            # Update total_nodes and majority criteria
            self.set_total_nodes(new_total_nodes)
            self.set_majority_criteria(new_majority_criteria)

           # with open(self.config_file_path, "wb") as config_file:
            #    self.config.write(config_file)

        except Exception as details:
            print details
