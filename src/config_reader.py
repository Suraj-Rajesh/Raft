import ConfigParser

class ConfigReader:

    def __init__(self, config_file_path):

        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file_path)

    def get_peer_servers(self, server_id, total_servers):
        peers = list()
        for i in [x for x in range(total_servers) if x != server_id]:
            peers.append(self.get_ip_and_port_of_server("Server" + str(i)))
        return peers

    def get_ip_and_port_of_server(self,section):
        """
        Use this to get server info as a tuple (ip address, port)
        """

        try:
            ip = self.getConfiguration(section, "ip")
            port = int(self.getConfiguration(section, "port"))
            return (ip, port)

        except Exception as details:
            print details
            return None

    def getConfiguration(self, section, entry):
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
    
    def electionTimeoutPeriod(self):
        try:
            return self.config.get("GeneralConfig", "electionTimeoutLower")
        except Exception as details:
            print details
            return None
