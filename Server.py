#from Marshal import *
#from Config import *
import socket
import time
import sys
import random
import optparse


class Server:
    def __init__(self):
        self.UDP_ip = "127.0.0.1"
        self.UDP_port = 7777
        self.time = time.time()
        self.cache = []
        self.cacheLimit = 10
        self.monitorList = []  # monitoring list format: [address, filePathname]
        self.invocationSemantics = 'AT_LEAST_ONCE'
        self.simulateLoss = True

    def run(self):
        try:
            self.sock = socket.socket(socket.AF_INET,  # Internet
                                      socket.SOCK_DGRAM)  # UDP
        except socket.error as e:
            print('Failed to create socket:\n{}'.format(e))
            sys.exit()

        # bind socket to port
        serverAddress = (self.UDP_ip, self.UDP_port)
        print('Starting server on {} Port {}...'.format(
            self.UDP_ip, self.UDP_port))

        #print('Invocation semantics used: {}'.format(self.invocationSemantics))

        try:
            self.sock.bind(serverAddress)
        except socket.error as e:
            print('Socket bind failed:\n{}'.format(e))
            sys.exit()

        # once socket bind, keep talking to client
        self.awaiting()

    # await data from client
    def awaiting(self):
        while True:
            #print('Monitor List: {}'.format(self.monitorList))
            print('Awaiting data from client...')
            data, address = self.sock.recvfrom(4096) #buffer size of 4096
            print('Received data from {}:\n{!r}'.format(address, data))
            #self.replyReq(data, address)

if __name__ == "__main__":
    parser = optparse.OptionParser()

    parser.add_option('-i', '--UDP_ip',
                      action="store", dest='UDP_ip',
                      help="Sets the ip address of the server", default="localhost")

    parser.add_option('-p', '--UDP_port',
                      action="store", dest="UDP_port",
                      help="Sets the port of server",
                      default=2222)

    # parser.add_option('-s', '--invocationSemantics',
    #                   action="store", dest="invocationSemantics",
    #                   help="Sets the invocation semantics",
    #                   default='AT_LEAST_ONCE')

    server = Server()
    options, args = parser.parse_args()
    server.UDP_ip = str(options.UDP_ip)
    server.UDP_port = int(options.UDP_port)
#    server.invocationSemantics = str(options.invocationSemantics)
    server.run()
