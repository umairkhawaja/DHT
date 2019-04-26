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
from pprint import pprint

'''
    GLOBAL VARIABLES

'''
ENCODING = 'utf-8'
BUFFSIZE = 1024
m = 0
maxNodes = 0
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
        self.succList = []
        self.succListIndex = 0
        self.super_succ = None
        self.scanFolder()

    def initialiseNew(self,bootstrap_addr):
        self.socket = Socket(AF_INET,SOCK_STREAM)
        self.socket.bind((bootstrap_addr[0],0))
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.address = self.socket.getsockname()
        self.fingerTable = []
        self.id = hashIt(str(self.address))
        print(f"My ID: {self.id}")
        print(f"My Address: {self.address}")
        self.object = {'id' : self.id , 'addr' : self.address}
        Thread(target=self.menu).start()
        Thread(target=self.startListening).start()
        self.joinNetwork(bootstrap_addr)
        Thread(target=self.stablize).start()
        Thread(target=self.checkSuccConnection()).start()
        return

    def initialiseBootstrap(self,addr):
        self.address = addr
        self.fingerTable = []
        self.id = hashIt(str(self.address))
        print(f"My ID: {self.id}")
        self.object = {'id' : self.id , 'addr' : self.address}
        self.succ = {'id':self.id,'addr' : self.address}
        self.pred = None

        self.socket = Socket(AF_INET,SOCK_STREAM)
        self.socket.bind(tuple(self.address))
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        Thread(target=self.menu).start()
        Thread(target=self.startListening).start()
        Thread(target=self.stablize).start()
        Thread(target=self.checkSuccConnection()).start()
        return
    
    def scanFolder(self):
        files = glob(os.getcwd() + '/**/*',recursive=True)
        files.remove(os.getcwd() + '/node.py')
        files = [path for path in files if os.path.isfile(path)]
        self.files = {hashIt( path.split('/')[-1]  ):path for path in files}


    def showFiles(self):
        files = {key : value.split('/')[-1] for key,value in self.files.items()}
        pprint(files)
    
    def menu(self):
        menu_dict = {
            '1' : self.printState,
            '2' : self.put_interface,
            '3' : self.get_interface,
            '4' : self.showFiles,
            '5' : self.leave
        }
        while True and self.exit != True:
            print("1) Print State")
            print("2) Upload a file")
            print("3) Download a file")
            print("4) List Files")
            print("5) Exit")
            choice = input(">")
            menu_dict[choice]()
    
    def printState(self):
        state = {
            'ID' : self.id,
            'Successor' : self.succ,
            'Predecessor' : self.pred,
            'Super Successor' : self.super_succ
            }
        pprint(state)

    def startListening(self):
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
            except OSError:
                if self.exit == True:
                    print("Exiting...")


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
            sendMsg(self.succ['addr'],msg)
            return

        msg = genMsg("Your Successor",successor,self.address)
        sendMsg(new_node['addr'],msg)
        return


    def populateSuccessorList(self):
        msg = genMsg("Send Successor",{},self.address)
        try:
            sendMsg(self.succ['addr'],msg)
        except:
            sleep(2)
    

    def handleNodeJoin(self,msg_obj):
        new_node = msg_obj['payload']
        self.findSuccessor(new_node)
        return


    def handlePredReq(self,msg_obj):
        pred_node = msg_obj['payload']
        self.pred = pred_node

    
    def checkSuccessor(self,msg_obj):
        successor_pred = msg_obj['payload']
        if successor_pred != None and int(successor_pred['id']) != self.id:
            self.succ = successor_pred
            msg = genMsg("I am your predecessor",self.object,self.address)
            sendMsg(successor_pred['addr'],msg)


    def stablize(self):
        sleep(10)
        while True and self.exit == False:
            try:
                msg = genMsg("Send Predecessor",{},self.address)
                sendMsg(self.succ['addr'],msg)
                sleep(5)
                self.populateSuccessorList()
            except:
                sleep(2)


    def checkSuccConnection(self):
        count = 0
        while True and self.exit != True:
            if self.succ and self.pred and self.id == self.succ['id'] and self.id == self.pred['id']:
                count = 0
            elif self.super_succ != None and self.succ['id'] != self.id:
                sock = Socket(AF_INET,SOCK_STREAM)
                try:
                    sock.connect(tuple(self.succ['addr']))
                    msg = genMsg("You good?",{},self.address)
                    sock.send(msg.encode())
                    reply = sock.recv(BUFFSIZE).decode()
                    sock.shutdown(SHUT_RDWR)
                    sock.close()
                except socket.error:
                    count+=1
                
                if count == 3:
                    try:
                        print(f"{self.succ['id']} disconnected")
                        sock2 = Socket(AF_INET,SOCK_STREAM)
                        sock2.connect(tuple(self.super_succ['addr']))
                        msg = genMsg("I am your predecessor",self.object,self.address)
                        sock2.send(msg.encode())
                        self.succ = self.super_succ
                        msg = genMsg("Requested Successor",self.succ,self.address)
                        sendMsg(self.pred['addr'],msg)
                        self.populateSuccessorList()
                        count = 0
                        sock2.shutdown(SHUT_RDWR)
                        sock2.close()
                    except:
                        pass

    def leave(self):
        print(f"{self.id} is leaving...")
        
        if self.pred != None and self.succ['id'] != self.id:
            for key in self.files:
                path = self.files[key]
                if os.path.isfile(path):
                    self.sendFile(path,self.succ['addr'])
            self.exit = True
            msg = genMsg("I am your predecessor",self.pred,self.address)
            sendMsg(self.succ['addr'],msg)
            msg = genMsg("Your Successor",self.succ,self.address)
            sendMsg(self.pred['addr'],msg)
        try:
            self.exit = True
            self.socket.shutdown(SHUT_RDWR)
            self.socket.close()
            exit(1)
        except:
            pass

    
    def requestFile(self,key,owner):
        msg = genMsg("Send file",key,self.address)
        sendMsg(msg,owner)


    def sendFile(self,path,addr):
        sock = Socket(AF_INET,SOCK_STREAM)
        sock.bind(('',0))
        filename = path.split('/')[-1]
        while True:
            try:
                sock.connect(tuple(addr))
                break
            except:
                pass
        msg = genMsg("Incoming file",filename,self.address)
        sock.send(msg.encode())
        res = sock.recv(BUFFSIZE).decode()

        if "Send it" in res:
            with open(path,'rb') as f:
                sock.sendfile(f,0)
            sock.shutdown(SHUT_RDWR)
            sock.close()

    def receiveFile(self,filename,sock):
        sock.send("Send it".encode())
        chunk = sock.recv(BUFFSIZE)
        data = chunk
        while chunk:
            chunk = sock.recv(BUFFSIZE)
            data += chunk
        with open(filename,'wb') as f:
            f.write(data)
        self.scanFolder()
    
    def get_interface(self):
        print("Enter the name of the file you want to download")
        search_file = input(">")
        hashed_key = hashIt(search_file)
        self.get(hashed_key,self.address)
   
    def get(self,key,query_addr):
        payload = {
            'key' : key,
            'query_addr' : query_addr,
        }
        if int(key) in self.files:
            payload = {
                'key' : key,
                'owner_addr' : self.address,
            }
            msg = genMsg("Got owner",payload,self.address)
            sendMsg(query_addr,msg)
        else: 
            msg = genMsg("Get owner",payload,self.address)
            sendMsg(self.succ['addr'],msg)
        

    def put_interface(self):
        file_paths = glob(os.getcwd() + '/**/*',recursive=True)
        file_paths = [path for path in file_paths if os.path.isfile(path)]
        file_names = [path.split('/')[-1] for path in file_paths]
        print("Choose file you want to upload to the network:")
        for index,file in enumerate(file_names):
            print(f"{index}) {file}")
        choice = int(input(">"))
        selected_file = file_names[choice]
        selected_path = file_paths[choice]
        payload = {
            'key' : hashIt(selected_file),
            'addr' : self.address,
        }
        msg = {
            'title' : "Find owner",
            'payload' : payload,
            'from' : self.address,
        }
        if self.pred != None and self.succ['id'] != self.id:
            Thread(target=self.findOwner,args=(payload,)).start()
        else:
            print("No other node in DHT. Cannot upload")


    def put(self,filename,):
        full_path = os.path.join(os.getcwd(),filename)
        hashed_key = hashIt(filename)
        self.sendFile()    
    

    def findOwner(self,msg_body):
        key = msg_body['key']
        query_node_addr = msg_body['addr']

        if inRange(key,self.pred['id'],self.id):
            payload = {
                'key'  : key,
                'addr' : self.address,
            }
            msg = genMsg("Found owner",payload,self.address)
            sendMsg(query_node_addr,msg)
        else:
            payload = {
                'key'  : key,
                'addr' : query_node_addr,
            }
            if tuple(self.succ['addr']) != tuple(query_node_addr):
                msg = genMsg("Find owner",payload,query_node_addr)
                sendMsg(self.succ['addr'],msg)
            else:
                msg = genMsg("404",payload,self.address)
                sendMsg(query_node_addr,msg)

    
    def handleRequests(self,client_socket):
        msg = client_socket.recv(BUFFSIZE).decode()
        try:
            msg_obj = json.loads(msg)
            msg_title = msg_obj['title']
        except json.JSONDecodeError:
            pass
        if "Node Join" in msg_title:
            Thread(target=self.handleNodeJoin,args=(msg_obj,)).start()
        
        elif "Find Successor" in msg_title:
            Thread(target=self.findSuccessor,args=(msg_obj,)).start()
        
        elif "I am your predecessor" in msg_title:
            Thread(target=self.handlePredReq,args=(msg_obj,)).start()
        
        elif "Your Successor" in msg_title:
            self.succ = msg_obj['payload']
            msg = genMsg("I am your predecessor",self.object,self.address)
            sendMsg(self.succ['addr'],msg)

        elif "Send Predecessor" in msg_title:
            msg = genMsg("Requested Predecessor",self.pred,self.address)
            sendMsg(msg_obj['from'],msg)

        elif "Requested Predecessor" in msg_title:
            Thread(target=self.checkSuccessor,args=(msg_obj,)).start()
        
        elif "Incoming file" in msg_title:
            filename = msg_obj['payload']
            Thread(target=self.receiveFile,args=(filename,client_socket)).start()
        
        elif "Send Successor" in msg_title:
            reply = genMsg("Requested Successor",self.succ,self.address)
            sendMsg(msg_obj['from'],reply)
        
        elif "Requested Successor" in msg_title:
            self.super_succ = msg_obj['payload']

        elif "You good?" in msg_title:
            client_socket.send("I'm good".encode())
        
        elif "Find owner" in msg_title:
            Thread(target=self.findOwner,args=(msg_obj['payload'],)).start()
        
        elif "Found owner" in msg_title:
            key = msg_obj['payload']['key']
            owner_addr = msg_obj['payload']['addr']
            file_path = self.files[key]
            if owner_addr != self.address: 
                Thread(target=self.sendFile,args=(file_path,owner_addr)).start()
        
        elif "Get owner" in msg_title:
            key = msg_obj['payload']['key']
            query_addr = msg_obj['payload']['query_addr']
            if tuple(query_addr) != tuple(self.address):
                Thread(target=self.get,args=(key,query_addr)).start()
            else:
                print("File Does not exist")
        
        elif "Got owner" in msg_title:
            owner_addr = msg_obj['payload']['owner_addr']
            requested_file = msg_obj['payload']['key']
            if owner_addr != self.address:
                msg = genMsg("Send file",requested_file,self.address)
                sendMsg(owner_addr,msg)
        
        elif "Send file" in msg_title:
            key = int(msg_obj['payload'])
            print(f"Got send file request for {key}")
            file_path = self.files[key]
            self.sendFile(file_path,msg_obj['from'])

        elif "404" in msg_title:
            print("Requested File not found")

    '''
        FUNCTIONS FOR THE "CLIENT" i.e Nodes that want to join a network
    '''
    def joinNetwork(self,bootstrap_addr):
        msg = genMsg("Node Join",self.object,self.address)
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