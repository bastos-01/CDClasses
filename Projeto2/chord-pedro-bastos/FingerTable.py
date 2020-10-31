# finger table 
import socket
import threading
import logging
import pickle
from utils import dht_hash, contains_predecessor, contains_successor

class FingerTable:

    def __init__(self,size,id):
        self.size = size
        self.ft = [(None,None)] * self.size
        self.id = id
        self.contador = 1

    def setSuccessor(self,succ_id,succ_addr):
        self.ft[1] = (succ_id,succ_addr)

    def getFirstId(self):
        return self.ft[1][0]
        
    def getFirstAddr(self):
        return self.ft[1][1]
 
    def getK(self):
        return ((self.id + 2**(self.contador-1)) % 1024)

    def updateFT(self,id,addr):
        self.ft[self.contador] = (id,addr)
        self.contador=self.contador+1
        if self.contador == self.size:
            self.contador = 1
            
    def getNext(self, key_hash):
        keys = []
        for entry in self.ft:
            if (entry != (None, None)):
                keys.append(entry)
        
        keys = sorted(keys)
        for i in range(len(keys)-1, -1, -1):
            if keys[i][0] <= key_hash:
                return keys[i]
        return keys[len(keys)-1]  

        
