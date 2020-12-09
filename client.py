"""///// Comands for run fclient //////
upload 
- python fclient.py upload archivotoupload dir_node_to_connect
- example
    python client.py upload underground.mp4 localhost:4000
"""

import zmq
import sys
import json
import os
import hashlib

partsize = 1024*1024 #N° MBytes

class Hash():
    def __init__(self,tipohash):
        self.tipohash = tipohash

    def getHash(self, file):
        if self.tipohash == "md5":
            hasher = hashlib.md5()
            hasher.update(file)
            return (hasher.hexdigest())
        elif self.tipohash == "sha1":
            hasher = hashlib.sha1(file).hexdigest()
            return hasher
        elif self.tipohash == "sha256":
            hasher = hashlib.sha256(file).hexdigest()
            return hasher
        else:
            print("Don´t found hash type, insert md5,sha1 or sha256")
            return("NoHash")

class Client():
    def __init__(self):
        self.context = zmq.Context()
        self.hashobj = Hash("sha1")

    def run(self):
        #socket = self.context.socket(zmq.REQ) #REQ este socket va a ser utilizado para hacer solicitudes
        
        cmd = sys.argv[1]
        if cmd == 'upload':
            address_to_connect = sys.argv[3]
            #socket.connect("tcp://{}".format(address_to_connect) #conexión con el nodo principal 
            filename = sys.argv[2]
            
            hash_whole_file, completbytes = self.complethash(filename)

            #TODO en la fun complethash guardar el hash en el f.chord
            self.createChord(filename,hash_whole_file)
            self.saveAndSendHashes(filename, address_to_connect)
            self.sendChordRing(filename, address_to_connect)
            
        elif cmd == 'download':
            socket.connect("tcp://{}".format(sys.argv[4])) 
            filetodownload = sys.argv[2]
            user = sys.argv[3]
            self.download(socket,filetodownload,user)
            
        else:
            print('Error, comando no valido')

    def saveAndSendHashes(self,filename, address_to_connect):
        #hashes = []
        with open(filename,'rb') as f:
            while True:
                partbytes = f.read(partsize) #leo solo una parte del archivo (partsize=1M)
                if not partbytes:
                    break
                parthash= self.hashobj.getHash(partbytes)
                self.updateHashChord(filename, parthash)
                print('parthash: '+parthash)
                self.findSuccesor(parthash, address_to_connect, partbytes)

 
    def findSuccesor(self, hash, address_to_connect, partbytes):
        print("connect with node")
        #ipSend = ''
        socket = self.context.socket(zmq.REQ)
        socket.connect("tcp://{}".format(address_to_connect))
        
        socket.send_multipart([b'responsible', hash.encode('utf-8')])
        resp = socket.recv_json()
        print(resp)
        if resp['response'] == "true":
            ipSend = resp['ip']
            print(ipSend)
            print('--- Encontre nodo')
            #socket = self.context.socket(zmq.REQ)
            #socket.connect("tcp://{}".format(ip_to_send))
            
            socket.send_multipart([b'upload', hash.encode('utf-8'), partbytes])
            resp = socket.recv_json()
            print(resp['message'])
            socket.close()
        else:
            print('entre al else d findSuccessor')
            #ip_successor = resp['ip']
            print('ip_successor: ' + resp['ip'])
            #ip of successor
            self.findSuccesor(hash,resp['ip'], partbytes)

    def createChord(self,filename,complete_hash):
        name_file = filename.split('.')[0]
        f = open(name_file+".chord", "w")
        f.close()
        f = open(name_file+".chord", "r")
        lines = f.readlines()
        lines.insert(0,filename)
        lines.insert(0,complete_hash+'\n')
        
        f.close()
        f = open(name_file+".chord", "w")
        f.writelines(lines)
        f.close()

    def updateHashChord(self,name,hash):
        filename =  name.split('.')[0]
        with open(filename+".chord", 'a') as f:
            f.write('\n'+hash)

    def sendChordRing(self, name, address_to_connect):
        filename = name.split('.')[0]
        hashChord, completbytes = self.complethash(filename+'.chord')
        print('Hash Chord: '+ hashChord)
        self.findSuccesor(hashChord, address_to_connect, completbytes)

    def complethash(self,filename):
        #TODO save complethash in f.chord
        #'rb' read binary
        with open(filename, 'rb') as f:
            completbytes = f.read() #obtengo todos los bytes
            hash= self.hashobj.getHash(completbytes) #hash de los bytes
            return hash, completbytes
    
    def readPart(self,filename, index):
        bytes = 0
        with open(filename, 'rb') as f:
            f.seek(partsize*index)
            bytes = f.read(partsize)
        return bytes
    
    def buildFile(self, parthash):
        pass
        """
        with open(filename, 'rb') as f:
            completbytes = f.read() #obtengo todos los bytes
            hash= self.hashobj.getHash(completbytes) #hash de los bytes
            return hash"""


if __name__ == "__main__":
    client = Client()
    client.run()