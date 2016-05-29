import rpyc
import sys
from config_reader import ConfigReader

class Client(object):

    def __init__(self):
        self.connection = 0
        self.config_reader = ConfigReader("client_config.ini")
        self.total_nodes = self.config_reader.get_total_nodes()
        self.servers = self.config_reader.get_peers(self.total_nodes)
        self.client_id = 999

    def connect(self, server_id):
        # Get server details
        server_ip   = self.servers[server_id][1]
        server_port = self.servers[server_id][2]
        try:
            self.connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
        except Exception as details:
            print "Server down..."
            self.connection = None

    def post(self, msg,server_id):

        server_ip   = self.servers[server_id][1]
        server_port = self.servers[server_id][2]
        return_value = None
        try:
            connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
            return_value = connection.root.exposed_postRPC(blog=msg,client_id=self.client_id)
        except Exception as details:
            print "Server down..."
            print details

        return return_value

    def lookup(self, server_id):

        server_ip   = self.servers[server_id][1]
        server_port = self.servers[server_id][2]
        return_value = None
        try:
            connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
            return_value = connection.root.exposed_lookupRPC()
        except Exception as details:
            print "Server down..."

        return return_value

    def change_config_of_network(self, list_of_changes,server_id):
        server_ip   = self.servers[server_id][1]
        server_port = self.servers[server_id][2]
        return_value = None
        try:
            connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
            return_value = connection.root.exposed_config_changeRPC(list_of_config_changes =list_of_changes, client_id=self.client_id)
        except Exception as details:
            print "Server down..."

        return return_value


    def start_console(self):

        print "\nClient running..."

        while True:
            command = raw_input("\nOperations allowed: \nPOST <server id> message\nLOOKUP <server id>\nCONFIG <server_to_send_to> \nEnter 0 to EXIT\nEnter operation: ")

            command_parse = command.split()

            if command_parse[0] == "POST":
                print "Posting..."
                server_id = int(command_parse[1])
                msg = command_parse[2]
                return_value = self.post(msg,server_id)
                print return_value

            elif command_parse[0] == "LOOKUP":
                print "Lookup..."
                server_id = int(command_parse[1])
                print (self.lookup(server_id))

            elif command_parse[0] == "CONFIG":
                config_change_list = list() #Follow the format above
                server_id = int(command_parse[1])

                while True:
                    config_input = raw_input("Config Change Selected\n Please type ADD <server_id> <server_ip> <server_port> \n REMOVE <server_id> \n DONE\n")
                    config_command = config_input.split()
                    add_or_remove = config_command[0]
                    
                    if "ADD" == add_or_remove or "REMOVE" == add_or_remove:
                        config_change_list.append(config_command)
                    elif "DONE" == add_or_remove:
                        if config_change_list:
                            break
                        else:
                            print "Empty config change found!"
                            break
                    else:
                        print "Wrong command. Exiting program."
                        sys.exit(0)

                return_value = self.change_config_of_network(config_change_list,server_id)
                print return_value
            
            elif int(command) == 0:
                sys.exit(0)

            else:
                print "Unknown command"

if __name__ == "__main__":

    try:
        client = Client()
        client.start_console()
    
    except Exception as details:
        print details
