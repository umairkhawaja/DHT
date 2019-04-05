class Node(object):

    def __init__(self,address):
        self.address = address
        self.files = {}
        self.fingerTable = []
    
    def getLastSuccessor(self):
        return self.fingerTable[-1]
    
    def getFile(self,key):
        return self.files[key]
    
    def getAddr(self):
        return self

    def addFile(self,key,file):
        self.fingerTable[key] = file

    