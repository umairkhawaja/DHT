import sys,os
from socket import AF_INET, SOCK_STREAM, SHUT_RDWR
from socket import socket as Socket
import socket
from hashlib import sha1
from threading import Thread
import json
from time import sleep

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
        self.pred = {'id':self.id,'addr' : address}

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

    def closestPrecedingNode(self,nid):
        succ_id = self.succ['id']

        if(self.id > succ_id):
            if ((2**m) - succ_id - nid) > (nid - self.id):
                return self.getNodeObj()
        else:
            if (nid - self.id) > (succ_id - nid):
                return self.getSucc()
            else:
                return self.getNodeObj()
        # for entry in list(reversed(self.fingerTable)):
        #     entry_id = entry['succ']['id']
        #     if entry_id > self.id and entry_id < nid:
        #         return entry['succ']
        # return self.getNodeObj()
    


def sendMsg(fromID,to_addr,msg,wait):
    s = Socket(AF_INET,SOCK_STREAM)
    s.bind(('',0))
    while True:
        try:
            s.connect(tuple(to_addr))
            break
        except:
            sleep(2)
    s.send(msg.encode())
    if wait == 0:
        s.shutdown(SHUT_RDWR)
        s.close()
        return
    else:
        while True:
            try:
                msg = s.recv(BUFFSIZE).decode()
                s.shutdown(SHUT_RDWR)
                s.close()
                return msg
            except:
                sleep(1)
    


def hashIt(name):
    global maxNodes
    return int(sha1(name.encode()).hexdigest(),16) % maxNodes

newNode = None
def handleNewNode(node_socket,new_node_socket,new_node_addr,node_object):
    global newNode
    while True:
        print("Listening for new messages")
        msg = new_node_socket.recv(BUFFSIZE).decode()
        if msg != None and msg != '':
            print(msg)
        if "NodeJoin" in msg:
            i = msg.find('{')
            j = msg.find(';')
            msg = msg[i:j]
            newNode = json.loads(msg)

            findSuccessor(new_node_socket,newNode,node_object)
            print("Find Successor call complete")
        if "I am your predecessor" in msg:
            print("Got a predecessor request")
            old_pred = node_object.getPred()
            
            node_object.setPred(newNode)
            print(f"{node_object.getId()} has a new predecessor : {newNode['id']}")
            new_node_socket.send("Got it".encode())

            if node_object.getSucc() == node_object.getNodeObj():
                print(f"{node_object.getId()} has a successor : {newNode['id']}")
                node_object.setSucc(newNode)
                node_object.updateFingerTable(newNode)
                
            # elif (old_pred == None) or (newNode['id'] > old_pred['id'] and newNode['id'] < node_object.getId()):
            
            elif old_pred != None and old_pred != node_object.getNodeObj():
                connect_socket = Socket(AF_INET,SOCK_STREAM)
                connect_socket.bind(('',0))
                while True:
                    try:
                        connect_socket.connect(tuple(old_pred['addr']))
                        break
                    except:
                        sleep(2)
                msg = "YouHaveNewSuccessor:" + json.dumps(newNode) + ";"
                connect_socket.send(msg.encode())
                connect_socket.shutdown(SHUT_RDWR)
                connect_socket.close()
                
            return
        if "YouHaveNewSuccessor" in msg:
            i = msg.find('{')
            j = msg.find(';')
            msg = msg[i:j]
            new_succ = json.loads(msg)
            print(f"{node_object.getId()} has a new Successor : {new_succ['id']}")
            node_object.setSucc(new_succ)
            node_object.updateFingerTable(new_succ)
            # break
            return

    # return

def listenForNewNodes(node_socket,node_object):
    print("Listening for new connections")
    while True:
        # node_socket.listen()
        try:
            new_node_socket,new_node_addr = node_socket.accept()
            print(f"New Connection {new_node_addr}")
            t = Thread(target=handleNewNode,args=(node_socket,new_node_socket,new_node_addr,node_object))
            t.start()
            t.join()
            # node_socket.shutdown(SHUT_RDWR)
            # node_socket.close()
        except OSError:
            pass
            # print(OSError)



def findSuccessor(node_socket,new_node_obj,bootstrap_node):
    nid = new_node_obj['id']
    addr = new_node_obj['addr']
    print(f"Finding Successor for {addr} with id: {nid}")
    bnode_id = bootstrap_node.getId()
    bnode_succ_id = bootstrap_node.getSucc()['id']
    bnode_pred_id = bootstrap_node.getPred()['id']
    successor = None
    while True:
        print(f'Finding Successor for {nid}')
        if (nid > bnode_id and nid <= bnode_succ_id):
            successor = bootstrap_node.getSucc()
            break
        elif bnode_id == bnode_succ_id or nid == bnode_id:
            successor = bootstrap_node.getNodeObj()
            break
        elif bnode_succ_id == bnode_pred_id:
            if bnode_succ_id >= nid:
                if bnode_id > bnode_succ_id or bnode_id < nid:
                    successor = bootstrap_node.getNodeObj()
                    break
            else:
                if (bnode_id > nid and bnode_id > bnode_succ_id) or (bnode_id < nid and bnode_id < bnode_succ_id):
                    successor = bootstrap_node.getSucc()
                    break
                else:
                    successor = bootstrap_node.getNodeObj()
                    break
        else:
            closest_node = bootstrap_node.closestPrecedingNode(nid)
            if closest_node['id'] == nid:
                successor = bootstrap_node.getSucc()
                break
            elif closest_node['id'] == bnode_id:
                successor = bootstrap_node.getNodeObj()
                break
            else:
                print(f"Forwarding NodeJoin request to {closest_node['id']}")
                while True:
                    try:
                        node_socket.connect(tuple(closest_node['addr']))
                        msg = "NodeJoin:" + json.dumps(closest_node) + ";"
                        node_socket.send(msg.encode())
                        return
                    except:
                        sleep(2)
    print(f"Found successor for {nid} : {successor}")
    msg = "YourSuccessor:" + json.dumps(successor) + ";"
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

        node_socket = Socket(AF_INET,SOCK_STREAM)
        node_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        node_socket.bind((host,port))
        node_socket.listen(maxNodes)
        print(f"Bootstrap node created. Listening at {host}:{port}")
        Thread(target=listenForNewNodes,args=(node_socket,node)).start()
        #GET/PUT menu goes here



    elif numargs == 3:
        chord_host = args[0]
        chord_port = int(args[1])
        m = int(args[2])
        maxNodes = (1<<m-1)

        node_socket = Socket(AF_INET,SOCK_STREAM)
        node_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
                if tuple(succ_addr) != (chord_host,chord_port):
                    node_socket.close()
                    node_socket.connect(succ_addr)
                    print(f"Connected to successor : {succ_id}")
                    # while True:
                    #     try:
                    #         break
                    #     except:
                    #         pass
                            # sleep(1)

                msg = "I am your predecessor:" + json.dumps(node.getNodeObj()) + ";"
                node_socket.send(msg.encode())
                print(f"Sent message: {msg}")
                while True:
                    try:
                        print("Waiting for response")
                        res = node_socket.recv(BUFFSIZE).decode()
                        print(f"Got res: {res}")
                        if res == "Got it":
                            print(f"Successfully entered the network via {succ_addr}")
                            node.setSucc(succ_obj)
                            node.updateFingerTable(succ_obj)
                            node_socket.shutdown(SHUT_RDWR)
                            node_socket.close()
                            break
                    except:
                        print("Error 1")
            break

        node_socket = Socket(AF_INET,SOCK_STREAM)

        while True:
            try:
                node_socket.bind((host,port))
                node_socket.listen()
                break
            except OSError:
                sleep(2)

        Thread(target=listenForNewNodes,args=(node_socket,node)).start()
        #GET/PUT menu goes here




    else:
        print("Usage for bootstrap node: <IP> <PORT> bootstrap <Number of bits for keyspace (m) >")
        print("Usage for new node:<IP address of a bootsrap node> <PORT of a bootstrap node> <Number of bits for keyspace (m) >")