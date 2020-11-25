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
        self.hashobj = Hash("sha256")

    def run(self):
        print("Connecting to server…")
        socket = self.context.socket(zmq.REQ) #REQ este socket va a ser utilizado para hacer solicitudes
        
        cmd = sys.argv[1]
        if cmd == 'upload':
            socket.connect("tcp://{}".format(sys.argv[3])) #conexión con el nodo principal 
            filename = sys.argv[2]
            ids_nodos = self.idsNodos(socket)
            #info_send = self.sendInfoProxy(socket,filename)
            #self.sendDataServers(info_send)
            socket.close()
            
        elif cmd == 'list':
            socket.connect("tcp://{}".format(sys.argv[3])) #Se conect de modo local, por el pueto indicado en la linea de comandos
            user = sys.argv[2]
            self.list(socket,user)
            socket.close()
            
        elif cmd == 'download':
            socket.connect("tcp://{}".format(sys.argv[4])) 
            filetodownload = sys.argv[2]
            user = sys.argv[3]
            self.download(socket,filetodownload,user)
            
        else:
            print('Error, comando no valido')

    #return 
    def findSuccesor(self, socket, hash):
        #main node should send his idnumber and address successor
        socket.send_multipart([b'idnumber'])
        ans = socket.recv_multipart()
        print(ans[1].decode('utf-8'))
        print(ans[2].decode('utf-8'))"""
        

    def sendInfoProxy(self,socket,filename):
        print("enviando info al proxy de {}".format(filename))
        hash_whole_file = self.complethash(filename)
        part_hash = self.partHash(filename)
        user = sys.argv[3]
        fileinfo = {
            "user": user,
            "filename": filename,
            "parts": part_hash,
            "complethash": hash_whole_file
        }
        #socket.send_json(fileinfo)
        str_json = json.dumps(fileinfo) #se convierte el json en una cadena
        socket.send_multipart([b'upload', str_json.encode('utf-8')])
        resp = socket.recv_json()
        print(resp)
        return resp

    def sendDataServers(self, info_send):
        #TODO  for next implementation: user deleted in json when file already exist, set array of user
        dir_servers = info_send['servers']
        dir_servers_new = []

        #lista sin repetir de las direcciones de los servers
        for address in dir_servers:
            if not(address in dir_servers_new):
                dir_servers_new.append(address) 

        print(dir_servers_new)
        
        #por cada servidor envio los paquetes correspondientes
        for i in range(len(dir_servers_new)):
            socket = self.context.socket(zmq.REQ)
            socket.connect("tcp://"+dir_servers_new[i])
            len_array = len(dir_servers)
            intervalo = len(dir_servers_new) #salto de acuerdo al número de servers que tenga
            
            for j in range(i,len_array,intervalo):
                #print("index: "+str(j))
                data_bytes = self.readPart(info_send['filename'],j)
                socket.send_multipart([b'upload', info_send['parts'][j].encode('utf-8'), data_bytes])
                resp = socket.recv_string()
                print(resp)

            socket.close() 

    def uniqueAdresses(self, dir_servers):
        dir_servers_new = []
        for address in dir_servers:
            if not(address in dir_servers_new):
                dir_servers_new.append(address)
        return dir_servers_new


    def list(self,socket,user):
        socket.send_multipart([bytes("list", encoding='utf-8'),bytes(user, encoding='utf-8')])
        resp = socket.recv_multipart()
        for ans in resp:
            print(ans.decode('utf-8'))

    def download(self,socket,filetodownload,user):
        # send request to proxy about file client wants download
        # proxy return server address and parts_hash to download
        # Group file of each server and download
        socket.send_multipart([b'download', filetodownload.encode('utf-8'), user.encode('utf-8')])
        resp_proxy = socket.recv_json()
        print(resp_proxy)
        socket.close()
        #if the dictionary don´t empty
        if resp_proxy:
            filename = resp_proxy['filename']
            dir_servers = resp_proxy['servers']
            #print(dir_servers)

            dir_servers_new = self.uniqueAdresses(dir_servers)

            #por cada servidor descargo los paquetes correspondientes
            for i in range(len(dir_servers_new)):
                socket = self.context.socket(zmq.REQ)
                socket.connect("tcp://"+dir_servers_new[i])
                len_array = len(dir_servers)
                intervalo = len(dir_servers_new) #salto de acuerdo al número de servers que tenga
                
                for j in range(i,len_array,intervalo):
                    socket.send_multipart([b'download', resp_proxy['parts'][j].encode('utf-8')])
                    resp = socket.recv_multipart()
                
                    if resp[0] == b'downloading':
                        with open('downloads/'+resp_proxy['parts'][j], 'wb') as f:
                            f.write(resp[1])
                        print("parte descargada")
                socket.close()
            self.buildFile(resp_proxy['parts'])
            
            """
            index = 0
            for dire in dir_servers:
                socket = self.context.socket(zmq.REQ)
                socket.connect("tcp://" + dire)
                socket.send_multipart([b'download', resp_proxy['parts'][index].encode('utf-8')])
                resp = socket.recv_multipart()
                
                if resp[0] == b'downloading':
                    with open('downloads/'+filename, 'ab') as f:
                        f.write(resp[1])
                    print("parte descargada")
                index += 1
                socket.close()
            print("download ready")"""

        else:
            print("don´t founded")
        
    def complethash(self,filename):
        #'rb' read binary
        with open(filename, 'rb') as f:
            completbytes = f.read() #obtengo todos los bytes
            hash= self.hashobj.getHash(completbytes) #hash de los bytes
            return hash

    def partHash(self,filename):
        hashes = []
        with open(filename,'rb') as f:
            while True:
                partbytes = f.read(partsize) #leo solo una parte del archivo (partsize=1M)
                if not partbytes:
                    break
                parthash= self.hashobj.getHash(partbytes)
                hashes.append(parthash)
            return hashes
    
    def readPart(self,filename, index):
        bytes = 0
        with open(filename, 'rb') as f:
            f.seek(partsize*index)
            bytes = f.read(partsize)
        return bytes
    
    #TODO do function
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
    
