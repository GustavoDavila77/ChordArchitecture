#comand
# python nodo.py  none localhost:4000 6 20 alpha

#node client
# python nodo.py localhost:4000 localhost:5000 6 40 none

import time
import zmq
import os
import json
from datetime import datetime
import sys
from random import choice
import string
import hashlib
import math

class FServer():
    #en los argumentos recivir la carpeta donde va a se almacenado todo
    def __init__(self):
        self.context = zmq.Context() #nos permite crear el socket

    def run(self):
        validation = self.receiveParameters()
        if validation == True:
            address_to_connect = sys.argv[1]
            self.ip_and_port = sys.argv[2]
            self.ip_server = self.ip_and_port.split(':')[0]
            self.port_server = self.ip_and_port.split(':')[1]
            self.bits = sys.argv[3]
            self.number_server = sys.argv[4]
            self.type_server = sys.argv[5]

            #random_str = self.randomString(self.ip_and_port)
            #self.id_server = int(self.hashString(random_str),16)
            #self.number_server = "37893173180865016123064796019639841985096597669"

            
            socket = self.initSocket()
            if self.type_server == "alpha":
                print("First/main node")
                self.saveServer(self.ip_and_port, self.ip_and_port)
                self.receive(socket)
            else:
                print("join to ring")
                self.saveServer()
                self.findSuccessor(address_to_connect)
                self.receive(socket)


    def receiveParameters(self):
        boolean = True
        try:
            sys.argv[1]
        except:
            print("Input ip_server:port_server to connect")
            boolean = False
        try:
            sys.argv[2]
        except:
            print("Input Own ip_server:port_server")
            boolean = False
        try:
            sys.argv[3]
        except:
            print("Input bits")
            boolean = False
        try:
            sys.argv[4]
        except:
            print("Input id/number of server")
            boolean = False
        try:
            sys.argv[5]
        except:
            print("Input node type if first node: type is alpha")
            boolean = False
        return boolean
        

    def initSocket(self):
        socket = self.context.socket(zmq.REP) #REP(REPLY) to answer to clients
        socket.bind("tcp://*:{}".format(self.port_server)) #se enlaza por medio del protocolo tcp y va a responder todo lo que venga del pueto 5555
        print("Socket created in port {}!!!".format(self.port_server))
        return socket

    def saveServer(self, successor="", predecessor=""):
        first_upper_limit = math.pow(2,int(self.bits))
        print(first_upper_limit)
        info_server = {
            "id_server": self.number_server,
            "ip": self.ip_server,
            "port": self.port_server,
            "successor": successor,
            "number_successor": self.number_server, 
            "predecessor": predecessor,
            "number_predecessor": self.number_server         
        }
        f = open('info_server.json','w')
        #servers_dict[self.id_server] = info_server
        json.dump(info_server, f, indent=4)
        f.close()    
    
    def findSuccessor(self, address_to_connect):
        #code range of responsability
        #recorrer anillo hasta conectarse
        print("connect with Ring..")
        socket = self.context.socket(zmq.REQ)
        socket.connect("tcp://{}".format(address_to_connect))
        #print("conectado")
        #while resp[0] == 'ok' seguir buscando
        socket.send_multipart([b'ringconnect', self.number_server.encode('utf-8'), self.ip_and_port.encode('utf-8')])
        resp = socket.recv_json()

        if resp['first'] == 'true':
            print("two node in the ring")
            f = open('info_server.json','r')
            servers_dict = json.load(f)
            f.close()
            servers_dict['predecessor'] = resp['ip']
            servers_dict['number_predecessor'] = resp['number']
            servers_dict['successor'] = resp['ip']
            servers_dict['number_successor'] = resp['number']
            f = open('info_server.json','w')
            json.dump(servers_dict, f, indent=4)
            f.close()
        elif resp['response'] == 'true':
            print("find responsible")
            ip_predecessor = resp['predecessor']
            number_predecessor = resp['number_predecessor']
            f = open('info_server.json','r')
            servers_dict = json.load(f)
            f.close()
            servers_dict['successor'] = resp['ip'] # es necesario  actualizar el predecessor en el nodo que hace la solicitud para el caso donde se tengan que reparitir los archivos
            servers_dict['number_successor'] = resp['number']
            servers_dict['predecessor'] = ip_predecessor
            servers_dict['number_predecessor'] = number_predecessor

            f = open('info_server.json','w')
            json.dump(servers_dict, f, indent=4)
            f.close()
            socket.close()

            socket = self.context.socket(zmq.REQ)
            print("ip predecessor: "+ ip_predecessor) 
            socket.connect("tcp://{}".format(resp['predecessor']))
            socket.send_multipart([b'newsuccessor', self.ip_and_port.encode('utf-8'), self.number_server.encode('utf-8')])
            ans = socket.recv_json()
            print(ans)

        elif resp['response'] == 'false':
            print("find new responsible")
            #TODO do recursive call to findSuccessor()
        

        
        print(resp)      


    def receive(self,socket):
        print("waiting messages..")
        while True:
            message = socket.recv_multipart()

            if message[0] == b'ringconnect':

                number_node = message[1].decode('utf-8')
                address_request = message[2].decode('utf-8')
                self.isMyRange(socket,number_node, address_request)
            
            elif message[0] == b'newsuccessor':
                ip_successor = message[1].decode('utf-8')
                number_successor = message[2].decode('utf-8')
                self.updateSuccessor(socket, ip_successor, number_successor)

            elif message[0] == b'idnumber':
                self.idNumber(socket)

            elif message[0] == b'upload':
    
                name_parthash = message[1].decode('utf-8')
                print("name_parthash: {}".format(name_parthash))
                        
                dirserver = "D:\Escritorio\Arquitectura cliente servidor\code/files/"+self.name_server+"/"
                print(self.name_server)
                
                with open(dirserver + name_parthash, 'wb') as f:
                    f.write(message[2])
                    socket.send_string("Part upload!!")

            elif message[0] == b'list':
                user = message[1].decode('utf-8')
                diruser = "D:\Escritorio\Arquitectura cliente servidor\code/files/"+user+"/"
                directorio = os.listdir(diruser)
                print(directorio)
                archivos = []

                for f in directorio:
                    archivos.append(f.encode('utf-8'))

                socket.send_multipart(archivos)

            elif message[0] == b'download':
                part_hash = message[1].decode('utf-8')
                print(part_hash)
                print(self.name_server)
                dir_server = "D:\Escritorio\Arquitectura cliente servidor\code/files/"+self.name_server+"/"
            
                try:
                    with open(dir_server+ part_hash,'rb') as f:
                        bytes = f.read()
                        socket.send_multipart([b"downloading", bytes])
                except:
                    socket.send_multipart([b"Notfound"])
            else:
                print('Error!!')
                socket.send_string("Error")

    def isMyRange(self,socket, number_node, address_request):
        print('address request' + address_request)
        print("Enter ismyrange fuction")
        f = open('info_server.json','r')
        servers_dict = json.load(f)
        f.close()
        number_predecessor = int(servers_dict['number_predecessor'])
        number_successor = int(servers_dict['number_successor'])
        ip_predecessor = servers_dict['predecessor']
        ip_successor = servers_dict['successor']
        id_server = int(servers_dict['id_server'])
        
        #verified if node is first in connect to chord
        if (number_predecessor == int(self.number_server)) and (number_successor == int(self.number_server)):
            print("First node in connect")
            f = open('info_server.json','w')
            servers_dict['predecessor'] = address_request
            servers_dict['successor'] = address_request
            servers_dict['number_predecessor'] = number_node
            servers_dict['number_successor'] = number_node
            json.dump(servers_dict, f, indent=4)
            f.close()
            #TODO in node request, update info_server too
            socket.send_json({"response": "true", "successor": ""})
        
        else:

            if int(number_node) > number_predecessor and int(number_node) <= id_server:
                print("esta en el rango")
                f = open('info_server.json','w')
                servers_dict['predecessor'] = address_request
                servers_dict['number_predecessor'] = number_node
                json.dump(servers_dict, f, indent=4)
                f.close() 
                socket.send_json({"response": "true", "successor": "","ip": self.ip_and_port, "number": self.number_server, "first": 'false', "predecessor": ip_predecessor, "number_predecessor": number_predecessor})
            else:
                print("no esta en el rango") 
                socket.send_json({"response": "false", "ip_successor": ip_successor, "first": 'false'})

    def updateSuccessor(self, socket, ip_successor, number_successor):
        f = open('info_server.json','r')
        servers_dict = json.load(f)
        f.close()
        f = open('info_server.json','w')
        servers_dict['successor'] = ip_successor
        servers_dict['number_successor'] = number_successor
        json.dump(servers_dict, f, indent=4)
        f.close() 
        socket.send_json({"response": "update ok"})

    def idNumber(self, socket):
        f = open('info_server.json','r')
        servers_dict = json.load(f)
        id_server = servers_dict['id_server']
        address_successor = servers_dict['successor']
        socket.send_multipart([b'info_server', id_server.encode('utf-8'), address_successor.encode('utf-8')])
        f.close()

    def randomString(self, s = '', n =30):
        chars = string.ascii_uppercase + string.ascii_lowercase + '0123456789'
        return s + ''.join(choice(chars) for i in range(30))
    
    def hashString(self, s):
        sha1 = hashlib.sha1()
        sha1.update(s.encode('utf-8'))
        return sha1.hexdigest()

    def rangeResponsibility(self, ids):
        for i in ids:
            print(i)
        
        print("------")
        resp = [(ids[-1],ids[0])]
        for i in range(1,len(ids)):
            resp.append((ids[i-1],ids[i]))
        
        for i in range(len(resp)):
            print("{} resp {}".format(i,resp[i]))

if __name__ == "__main__":
    server = FServer()
    #proxy.servers()
    server.run()
    #TODO cada server debe tener su nombre aleatorio
    #TODO poner esto en el cliente

    """"
    ip = '192.236.2.78:2500'
    sids = [server.randomString(ip) for x in range(3)]
    ids = [int(server.hashString(s),16) for s in sids]
    ids.sort()

    server.rangeResponsibility(ids)
    #server 0: 37893173180865016123064796019639841985096597669
    #server 1: 515748800960014143714155257729430307158535308957
    #server 2: 647584871384597889933400031157176508445575663970

    
    Range Responsability
    0 resp (647584871384597889933400031157176508445575663970, 37893173180865016123064796019639841985096597669)
    1 resp (37893173180865016123064796019639841985096597669, 515748800960014143714155257729430307158535308957)
    2 resp (515748800960014143714155257729430307158535308957, 647584871384597889933400031157176508445575663970)
    """