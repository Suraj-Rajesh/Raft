import rpyc
import sys
from config_reader import ConfigReader

class Client(object):

    def __init__(self):
        self.config_reader = ConfigReader("client_config.ini")
        self.total_nodes = self.config_reader.get_total_nodes()
        self.servers = self.config_reader.get_peers(self.total_nodes)
        self.client_id = 999

    def post(self, msg,server_id):

        server_ip, server_port = self.config_reader.get_server_port_ip(server_id, self.servers)
        return_value = None
        try:
            connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
            return_value = connection.root.postRPC(blog=msg,client_id=self.client_id)
        except Exception as details:
            print "\n"
            print "Server down..."

        return return_value

    def lookup(self, server_id):

        server_ip, server_port = self.config_reader.get_server_port_ip(server_id, self.servers)
        return_value = None
        try:
            connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
            return_value = connection.root.lookupRPC()
            print "\nBlogs: "
            print "\n"
            for blog in  return_value:
                if blog != "CONFIG_CHANGE":
                    print blog

        except Exception as details:
            print "\n"
            print "Server down..."

    def change_config_of_network(self, list_of_changes,server_id):
        server_ip, server_port = self.config_reader.get_server_port_ip(server_id, self.servers)
        return_value = None
        print list_of_changes
        try:
            connection = rpyc.connect(server_ip, server_port, config = {"allow_public_attrs" : True})
            return_value = connection.root.config_changeRPC(list_of_config_changes =list_of_changes, client_id=self.client_id)
        except Exception as details:
            print details
            print "\n"
            print "Server down..."

        if return_value:
            self.change_config_of_client(list_of_changes)

        return return_value

    def change_config_of_client(self, list_of_changes):
        for config in list_of_changes:
            if config[0] == "ADD":
                self.servers.append((config[1],config[2],int(config[3])))
        print self.servers

    def start_console(self):

        print "\n"

        while True:
            command = raw_input("\nOperations allowed: \n\nPOST <server id> message\nLOOKUP <server id>\nCONFIG <server_to_send_to> \nEnter 0 to EXIT\n\nEnter operation: ")

            command_parse = command.split()

            if command_parse[0] == "POST":
                server_id = int(command_parse[1])
                msg = command_parse[2]
                try:
                    return_value = self.post(msg,server_id)
                    if return_value:
                        print "\nBlog successfully posted..."
                except Exception as details:
                    print details

            elif command_parse[0] == "LOOKUP":
                server_id = int(command_parse[1])
                self.lookup(server_id)

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
