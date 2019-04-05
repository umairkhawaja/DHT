import sys,os
from socket import AF_INET, socket, SOCK_STREAM
from hashlib import sha1

ENCODING = 'utf-8'

class Node(object):

    def __init__(self,address,m):
        self.address = str(address[0]) + ":" +  str(address[1])
        self.files = {}
        self.fingerTable = []
        self.id = hashIt(self.address)

        for i in range(m):
            self.fingerTable.append(
                {
                    'id+2i': self.id + 2**i,
                    'succ' : {
                        'id' : self.id,
                        'addr' : address
                    }
                }
            )
    
    def getLastSuccessor(self):
        return self.fingerTable[-1]
    
    def getFile(self,key):
        return self.files[key]
    
    def getAddr(self):
        return self

    def addFile(self,key,file):
        self.fingerTable[key] = file




def hashIt(name):
    global maxNodes
    return int(sha1(name.encode()).hexdigest(),16) % maxNodes

m = 0
maxNodes = 0
node_socket = None

if __name__ == "__main__":
    numargs = len(sys.argv) - 1
    args = sys.argv[1:]

    if numargs == 4 and 'bootstrap' in args:
        host = args[0]
        port = int(args[1])
        m = int(args[3])
        maxNodes = (1<<m-1)

        node = Node((host,port),m)
        print(node.getLastSuccessor())


        node_socket = socket(AF_INET,SOCK_STREAM)
        node_socket.bind((host,port))
        node_socket.listen(maxNodes)
        print(f"Bootstrap node created. Listening at {host}:{port}")
        while True:
            client,client_addr = node_socket.accept()
            print(f"New Node Connected: {client_addr}")
            client.send("ack".encode())
            break



    elif numargs == 3:
        chord_host = args[0]
        chord_port = int(args[1])
        m = int(args[2])
        maxNodes = (1<<m-1)

        node_socket = socket(AF_INET,SOCK_STREAM)
        node_socket.bind(('',0))
        host,port = node_socket.getsockname()
        node = Node((host,port),m)
        print(node.getLastSuccessor())
        node_socket.connect((chord_host,chord_port))
        while True:
            try:
                msg = node_socket.recv(512).decode()
                if msg == "ack":
                    print(f"Successfully connected with bootstrap node at {(chord_host,chord_port)}")
                    break
            except RuntimeError:
                print("Err")


    else:
        print("Usage for bootstrap node: <IP> <PORT> bootstrap <Number of bits for keyspace (m) >")
        print("Usage for new node:<IP address of a bootsrap node> <PORT of a bootstrap node> <Number of bits for keyspace (m) >")