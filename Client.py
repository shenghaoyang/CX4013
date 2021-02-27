import os
import sys
import socket
import optparse
import time
import random


class Client:
    def __init__(self):
        self.cache = [0, 0, '']  # cache = [Tvalid, Tclient, cacheEntry]
        self.HOST = 'localhost'
        self.PORT = 2222
#        self.freshness_interval = 10
#        self.simulateLoss = True

    def run(self):
        print('Starting client socket...')

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(1)
        except socket.error as e:
            print("Failed to create client socket:\n{}".format(e))
            sys.exit()

        while True:
            print('Choose a service according to the following options:\n')
            print('1: Query the availability of a facility (Select days and facility)\n')
            print('2: Booking a facility (for a period of time)\n')
            print('3: Manage Booking (change booking)\n')
            print('4: Check Availability (monitor using callback)\n')
            print('5: Idempotent example\n')
            print('6: non-Idempotent example\n')


            userChoice = input('Input 1-5 or "q" to exit:')

            if userChoice == '1':
                filePathname = input('Input file path name:')
                offset = int(input('Input offset in bytes:'))
                numBytes = int(input('Input number of bytes:'))
                #check = self.checkCache()
                # if not check:
                #     print('Retrieved from Cache: {}'.format(self.cache[-1]))
                # else:
                #     print('Server Reply: {}'.format(
                #         self.queryRead(filePathname, offset, numBytes)[-1]))
            
            elif userChoice == 'q':
                self.close()
                break
            else:
                print('You have entered an incorrect service.')
                print('Please input a number from 1-5 or "q" to exit.\n')

        return

    def close(self):
        print('Closing socket...')
        try:
            self.sock.close()
        except socket.error as e:
            print('Error closing socket:\n{}'.format(e))
        print('Socket closed...')

if __name__ == "__main__":

    parser = optparse.OptionParser()

    parser.add_option('-t', '--freshness_interval',
                      action="store", dest="freshness_interval",
                      help='Sets the freshness interval of the client',
                      default=10
                      )

    parser.add_option('-i', '--ip_server',
                      action="store", dest="ip",
                      help='Sets the ip address of the server for client to send data to',
                      default='localhost'
                      )

    parser.add_option('-p', '--port',
                      action="store", dest="port",
                      help='Sets the port of the server for client to send data to',
                      default=7777
                      )

    options, args = parser.parse_args()

    client = Client()

    client.freshness_interval = options.freshness_interval
    client.PORT = int(options.port)
    client.HOST = str(options.ip)

    client.run()