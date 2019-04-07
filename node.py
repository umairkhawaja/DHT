import sys,os
from socket import AF_INET, socket, SOCK_STREAM
from hashlib import sha1
from threading import Thread
import json

ENCODING = 'utf-8'
BUFFSIZE = 512

m = 0
maxNodes = 0
node_socket = None

current_nodes = 0

class Node(object):

    def __init__(self,address,m):
        self.address = address
        self.files = {}
        self.fingerTable = []
        self.id = hashIt(str(self.address[0]) + ":" +  str(self.address[1]))

        self.succ = {'id':self.id,'addr' : address}
        self.pred = None

        for i in range(m):
            self.fingerTable.append(
                {
                    'id+2i': self.id + 2**i,
                    'succ' : {
                        'id' : self.id,
                        'addr' : self.address
                    }
                }
            )
    
    def getLastSuccessor(self,key):
        return self.fingerTable[-1]
    
    def getFile(self,key):
        return self.files[key]
    
    def getAddr(self):
        return self.address

    def addFile(self,key,file):
        self.fingerTable[key] = file

    def getFingerTable(self):
        return self.fingerTable.copy()
    
    def getId(self):
        return self.id
    
    def getSucc(self):
        return self.succ

    def getNodeObj(self):
        return {'id' : self.id , 'addr' : self.address}

    def setSucc(self,succ_obj):
        self.succ = succ_obj

    def setPred(self,pred_obj):
        self.pred = pred_obj
    
    def getPred(self):
        return self.pred
    
    def updateFingerTable(self,succ_obj):
        for i in range(m-1,-1,-1):
            if succ_obj['id'] >= self.fingerTable[i]['succ']['id']:
                self.fingerTable[i]['succ'] = succ_obj
                print(f"Update in finger table: {self.fingerTable[i]}")




def hashIt(name):
    global maxNodes
    return int(sha1(name.encode()).hexdigest(),16) % maxNodes


def handleNewNode(node_socket,new_node_socket,new_node_addr,node_object):
    global current_nodes
    newNode = None
    while True:
        msg = new_node_socket.recv(BUFFSIZE).decode()
        if "NodeJoin" in msg:
            i = msg.find('{')
            j = msg.find(';')
            msg = msg[i:j]
            print(msg)
            new_node = json.loads(msg)
            findSuccessor(new_node_socket,new_node,node_object)
            print("Find Successor call complete")
            # new_node_socket.close()
            # return
        elif "I am your predecessor" in msg:
            print(f"{node_object.getId()} has a new predecessor : {new_node['id']}")
            node_object.setPred(new_node)
            new_node_socket.send("Got it".encode())
            current_nodes+=1
            # new_node_socket.close()
            # break
            return
    # new_node_socket.close()

def listenForNewNodes(node_socket,node_object):
    while True:
        try:
            new_node_socket,new_node_addr = node_socket.accept()
            print(f"New Connection {new_node_addr}")
            Thread(target=handleNewNode,args=(node_socket,new_node_socket,new_node_addr,node_object)).start()
        except OSError:
            print(OSError)

def findSuccessor(node_socket,new_node_obj,bootstrap_node):
    nid = new_node_obj['id']
    addr = new_node_obj['addr']
    bnode_id = bootstrap_node.getId()
    bnode_succ_id = bootstrap_node.getSucc()['id']
    bnode_pred = bootstrap_node.getPred()
    while True:
        if  current_nodes == 1:
            new_node_succ = bootstrap_node.getAddr()
            print(f"Successor found for {addr}")
            msg = f"YourSuccessor:"+json.dumps(bootstrap_node.getNodeObj()) + ";"
            node_socket.send(msg.encode())
            return
        elif bnode_pred != None and (nid > bnode_pred['id'] and nid <= bnode_id):
            new_node_succ = bootstrap_node.getAddr()
            print(f"Successor found for {addr}")
            msg = f"YourSuccessor:"+json.dumps(bootstrap_node.getNodeObj()) + ";"
            node_socket.send(msg.encode())
            return
        elif (nid > bnode_id and nid <= bnode_succ_id):
            new_node_succ = bootstrap_node.getSucc()['addr']
            print(f"Successor found for {addr}")
            msg = f"YourSuccessor:"+json.dumps(bootstrap_node.getSucc()) + ";"
            node_socket.send(msg.encode())
            return
        else:
            bootstrap_fingerTable = bootstrap_node.getFingerTable()
            for entry in list(reversed(bootstrap_fingerTable)):
                if entry['succ']['id'] in range(bootstrap_node.getId(),nid):
                    node_socket.close()
                    print(f"{new_node_obj} trying to connect with {entry['succ']}")
                    node_socket.connect(entry['succ']['addr'])
                    msg = "NodeJoin:" + json.dumps(new_node_obj) + ";"
                    node_socket.send(msg.encode())
                    return




if __name__ == "__main__":
    numargs = len(sys.argv) - 1
    args = sys.argv[1:]

    if numargs == 4 and 'bootstrap' in args:
        host = args[0]
        port = int(args[1])
        m = int(args[3])
        maxNodes = (1<<m-1)

        node = Node((host,port),m)
        current_nodes+=1
        print(f"My ID: {node.getId()}")

        node_socket = socket(AF_INET,SOCK_STREAM)
        node_socket.bind((host,port))
        node_socket.listen(maxNodes)
        print(f"Bootstrap node created. Listening at {host}:{port}")
        Thread(target=listenForNewNodes,args=(node_socket,node)).start()



    elif numargs == 3:
        chord_host = args[0]
        chord_port = int(args[1])
        m = int(args[2])
        maxNodes = (1<<m-1)

        node_socket = socket(AF_INET,SOCK_STREAM)
        node_socket.bind(('127.0.0.1',0))
        host,port = node_socket.getsockname()
        print(host,port)
        node = Node((host,port),m)
        print(f"My ID: {node.getId()}")

        node_socket.connect((chord_host,chord_port))
        print(f"Connected to {chord_host}:{chord_port}")
        node_obj = {'id' : node.getId() , 'addr' : node.getAddr()}
        msg = "NodeJoin:" + json.dumps(node_obj) + ";"
        node_socket.send(msg.encode())
        print(f"Sending msg: {msg}")
        while True:
            # try:
            response = node_socket.recv(BUFFSIZE).decode()
            print(f"Got response: {response}")
            if "YourSuccessor" in response:
                i = response.find('{')
                j = response.find(';')
                response = response[i:j]
                succ_obj = json.loads(response)
                print(f"Successor object parsed: {succ_obj}")
                succ_id = succ_obj['id']
                succ_addr = tuple(succ_obj['addr'])
                print(f"Successor addr: {succ_addr}")
                if succ_addr != (chord_host,chord_port):
                    node_socket.close()
                    node_socket.connect(succ_addr)
                    print("Connected to successor")
                msg = "I am your predecessor:" + json.dumps(node.getNodeObj()) + ";"
                node_socket.send(msg.encode())
                print(f"Sent message: {msg}")
                while True:
                    try:
                        res = node_socket.recv(BUFFSIZE).decode()
                        print(f"Got res: {res}")
                        if res == "Got it":
                            print(f"Successfully entered the network via {succ_addr}")
                            node.setSucc(succ_obj)
                            node.updateFingerTable(succ_obj)
                            node_socket.close()
                            break
                    except:
                        print("Error 1")
            break
            node_socket.close()
            node_socket.listen(maxNodes)
        Thread(target=listenForNewNodes,args=(node_socket,node)).start()
            # except:
                # print("Error 2")
                




    else:
        print("Usage for bootstrap node: <IP> <PORT> bootstrap <Number of bits for keyspace (m) >")
        print("Usage for new node:<IP address of a bootsrap node> <PORT of a bootstrap node> <Number of bits for keyspace (m) >")