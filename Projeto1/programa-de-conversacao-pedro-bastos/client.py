# Echo client program
import socket
import sys
import fcntl
import os
import selectors
import json

class Client:
    def __init__(self):
        self.HOST = 'localhost' # Address of the host running the server 
        self.PORT = 5000 # The same port as used by the server
        self.f = True
        self.receiver = ""
        self.message = ""
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.HOST, self.PORT))
        self.s.setblocking(False)

    def json_encode(self, receiver, message):
        jsonText = {"sender" : sys.argv[1], "receiver" : receiver, "message" : message}
        jsonText = json.dumps(jsonText)
        jsonText = jsonText.encode('utf-8')
        return jsonText
    
    def json_decode(self, data):
        data = data.decode('utf-8')
        userData = json.loads(data)
        sender = userData["sender"]
        receiver = userData["receiver"]
        message = userData["message"]
        return sender, receiver, message

    # function to be called when enter is pressed
    def got_keyboard_data(self, stdin, mask):
        if(self.f):
            self.receiver = stdin.read().rstrip()
            if self.receiver in '\r\n':
                sys.exit(0)
            self.f = False
        else:
            self.message = stdin.read().rstrip()
            if self.message in '\r\n':
                sys.exit(0)
            data = cliente.json_encode(self.receiver, self.message)
            self.s.send(data)
            self.f = True

    # function to be called when a message is received
    def got_message(self, conn, mask):
        data = conn.recv(1024)

        sender, receiver, message = cliente.json_decode(data)
        print("Sender: {}, Message: {}".format(sender, message))
    
# register event
selector = selectors.DefaultSelector()
# set sys.stdin non-blocking
orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

cliente = Client()
jsonText = cliente.json_encode("",sys.argv[1])
cliente.s.send(jsonText)

selector.register(cliente.s, selectors.EVENT_READ, cliente.got_message) #register socket
selector.register(sys.stdin, selectors.EVENT_READ, cliente.got_keyboard_data) #register stdin as socket

while True:
    if(cliente.f):
        print("Destination: \n", end='', flush=True)
    else:
        print("Message: \n", end='', flush=True)
    #print('Type something and hit enter: ', end='', flush=True)
    for k, mask in selector.select():
        callback = k.data
        callback(k.fileobj, mask)





