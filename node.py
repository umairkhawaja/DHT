import sys,os
from socket import AF_INET, SOCK_STREAM, SHUT_RDWR
from socket import socket as Socket
import socket
from hashlib import sha1
from threading import Thread
import json
from time import sleep
import os
from glob import glob
import pprint

'''
    GLOBAL VARIABLES

'''
ENCODING = 'utf-8'
BUFFSIZE = 512
m = 0
maxNodes = 0
# host = '127.0.0.1'

'''
    HELPER FUNCTIONS
'''
def genMsg(title,obj,from_addr):
    return json.dumps({
        'title' : title,
        'payload' : obj,
        'from' : from_addr
    })

def hashIt(name):
    global maxNodes
    return int(sha1(name.encode()).hexdigest(),16) % maxNodes

def sendMsg(to_addr,msg):
    s = Socket(AF_INET,SOCK_STREAM)
    s.bind(('',0))
    while True:
        try:
            s.connect(tuple(to_addr))
            break
        except:
            sleep(2)
    s.send(msg.encode())
    s.shutdown(SHUT_RDWR)
    s.close()
    return

def inRange(key,left,right):
    current = left

    while(current != right):
        if key == current:
            return True
        current = (current + 1) % maxNodes
    return False
    

class Node(object):

    def __init__(self):
        self.socket = None
        self.address = None
        self.files = None
        self.fingerTable = None
        self.id = None
        self.object = None
        self.pred = None
        self.succ = None
        self.sentPred = False
        self.sentSucc = False
        self.exit = False

    def initialiseNew(self,bootstrap_addr):
        self.socket = Socket(AF_INET,SOCK_STREAM)
        self.socket.bind((bootstrap_addr[0],0))
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.address = self.socket.getsockname()
        self.files = {}
        self.fingerTable = []
        self.id = hashIt(str(self.address))
        print(f"My ID: {self.id}")
        print(f"My Address: {self.address}")
        self.object = {'id' : self.id , 'addr' : self.address}
        self.scanFolder()        
        Thread(target=self.startListening).start()
        self.joinNetwork(bootstrap_addr)
        Thread(target=self.stablize).start()
        
        # self.findOwner()

        # for i in range(m):
        #     self.fingerTable.append(
        #         {
        #             'id+2i': self.id + 2**i,
        #             'succ' : {
        #                 'id' : self.id,
        #                 'addr' : self.address
        #             }
        #         }
        #     )
        return

    
    
    def initialiseBootstrap(self,addr):
        self.address = addr
        self.files = {}
        self.fingerTable = []
        self.id = hashIt(str(self.address))
        print(f"My ID: {self.id}")
        self.object = {'id' : self.id , 'addr' : self.address}
        self.succ = {'id':self.id,'addr' : self.address}
        self.pred = None

        self.socket = Socket(AF_INET,SOCK_STREAM)
        self.socket.bind(tuple(self.address))
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.scanFolder()
        Thread(target=self.startListening).start()
        Thread(target=self.stablize).start()    
        sleep(60)
        self.leave()
        # for i in range(m):
        #     self.fingerTable.append(
        #         {
        #             'id+2i': self.id + 2**i,
        #             'succ' : {
        #                 'id' : self.id,
        #                 'addr' : self.address
        #             }
        #         }
        #     )
        return
    
    def scanFolder(self):
        files = glob(os.getcwd() + '/**/*',recursive=True)
        files.remove(os.getcwd() + '/node.py')
        # pprint.pprint(files)
        self.files = {hashIt( path.split('/')[-1]  ):path for path in files}

    def findOwner(self):
        notMyFiles = { (key if not (key < self.id and key > self.pred['id']) else key) : (value if not (key < self.id and key > self.pred['id']) else value) for key,value in self.files.items() }
        pprint.pprint(notMyFiles)
    
    def addFile(self,filename): # TO BE IMPLEMENTED
        self.files[key] = file

    
    # def updateFingerTable(self,succ_obj):
    #     for i in range(m):
    #         if succ_obj['id'] >= self.fingerTable[i]['succ']['id']:
    #             self.fingerTable[i]['succ'] = succ_obj
    #             print(f"Update in finger table: {self.fingerTable[i]}")
    #             break

    # def closestPrecedingNode(self,nid):
    #     succ_id = self.succ['id']

    #     if(self.id > succ_id):
    #         if ((2**m) - succ_id - nid) > (nid - self.id):
    #             return self.getNodeObj()
    #     else:
    #         if (nid - self.id) > (succ_id - nid):
    #             return self.getSucc()
    #         else:
    #             return self.getNodeObj()


    '''
        FUNCTIONS FOR THE "SERVER" i.e. Nodes already in the network
    '''
    def leave(self):
        print(f"{self.id} is leaving...")
        # TRANSFER FILES GOES HERE 
        self.exit = True
        msg = genMsg("I am your predecessor",self.pred,self.address)
        sendMsg(self.succ['addr'],msg)
        msg = genMsg("Your Successor",self.succ,self.address)
        sendMsg(self.pred['addr'],msg)
        
        try;
            self.socket.shutdown(SHUT_RDWR)
            self.socket.close()
            exit(1)
        except:
            pass
        

    def handleNodeJoin(self,msg_obj):
        new_node = msg_obj['payload']
        self.findSuccessor(new_node)
        return

    def findSuccessor(self,new_node):
        new_node_id = new_node['id']
        new_node_addr = new_node['addr']
        successor = None

        if self.pred == None and self.succ == self.object:
            successor = self.object
        elif inRange(new_node_id,self.id,self.succ['id']):
            successor = self.succ
        else:
            msg = genMsg("Node Join",new_node,new_node_addr)
            # print(f"Forwarding request to : {self.succ['id']}")
            sendMsg(self.succ['addr'],msg)
            return

        print(f"Found successor for {new_node_id} : {successor}")
        msg = genMsg("Your Successor",successor,self.address)
        print(f"Sending {msg} to {new_node}")
        sendMsg(new_node['addr'],msg)
        return
    
        
    def startListening(self):
        print(f"Listening for incoming messages at {self.address}")
        self.socket.listen()
        while True and self.exit == False:
            try:
                client_socket,client_address = self.socket.accept()
                Thread(target=self.handleRequests,args=(client_socket,)).start()
            except KeyboardInterrupt:
                self.socket.shutdown(SHUT_RDWR)
                self.socket.close()
                print("Terminating...")
                exit(1)

    def handlePredReq(self,msg_obj):
        pred_node = msg_obj['payload']
        self.pred = pred_node
        # if self.pred == None:
        #     self.pred = pred_node
        # else:
        #     msg = genMsg("Your Successor",pred_node,self.address)
        #     sendMsg(self.pred['addr'],msg)
        #     self.pred = pred_node
        # print(f"My Predecessor is {self.pred}")
    
    def checkSuccessor(self,msg_obj):
        successor_pred = msg_obj['payload']
        if successor_pred != None and int(successor_pred['id']) != self.id:
            self.succ = successor_pred
            msg = genMsg("I am your predecessor",self.object,self.address)
            sendMsg(successor_pred['addr'],msg)


    def handleRequests(self,client_socket):
        msg = client_socket.recv(BUFFSIZE).decode()
        msg_obj = json.loads(msg)
        msg_title = msg_obj['title']
        
        if "Node Join" in msg_title:
            Thread(target=self.handleNodeJoin,args=(msg_obj,)).start()
        
        elif "Find Successor" in msg_title:
            Thread(target=self.findSuccessor,args=(msg_obj,)).start()
        
        elif "I am your predecessor" in msg_title:
            Thread(target=self.handlePredReq,args=(msg_obj,)).start()
        
        elif "Your Successor" in msg_title:
            print(f"Successor set: {msg_obj['payload']}")
            self.succ = msg_obj['payload']
            msg = genMsg("I am your predecessor",self.object,self.address)
            sendMsg(self.succ['addr'],msg)

        elif "Send Predecessor" in msg_title:
            msg = genMsg("Requested Predecessor",self.pred,self.address)
            sendMsg(msg_obj['from'],msg)

        elif "Requested Predecessor" in msg_title:
            Thread(target=self.checkSuccessor,args=(msg_obj,)).start()

            

    def stablize(self):
        sleep(15)
        while True and self.exit == False:
            msg = genMsg("Send Predecessor",{},self.address)
            sendMsg(self.succ['addr'],msg)
            state = {
                'ID' : self.id,
                'Successor' : self.succ,
                'Predecessor' : self.pred,
            }
            pprint.pprint(state)
            sleep(5)

    '''
        FUNCTIONS FOR THE "CLIENT" i.e Nodes that want to join a network
    '''
    def joinNetwork(self,bootstrap_addr):
        msg = genMsg("Node Join",self.object,self.address)
        print("Sending Join request")
        sendMsg(bootstrap_addr,msg)
        return

    

if __name__ == "__main__":
    args = sys.argv[1:]
    numargs = len(sys.argv) - 1

    if 'bootstrap' in args and numargs == 4:
        host = args[0]
        port = int(args[1])
        m = int(args[3])
        maxNodes = (1<<(m-1))

        node = Node()
        node.initialiseBootstrap((host,port))
    elif numargs == 3:
        chord_host = args[0]
        chord_port = int(args[1])
        m = int(args[2])
        maxNodes = (1<<m-1)

        node = Node()
        node.initialiseNew((chord_host,chord_port))
    else:
        print("Usage for bootstrap node: <IP> <PORT> bootstrap <Number of bits for keyspace (m) >")
        print("Usage for new node:<IP address of a bootsrap node> <PORT of a bootstrap node> <Number of bits for keyspace (m) >")