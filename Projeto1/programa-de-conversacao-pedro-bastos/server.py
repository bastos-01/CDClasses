import selectors
import socket
import json

dicionario = {}

sel = selectors.DefaultSelector()

def getKey(x):
    for key, value in dicionario.items():
         if x == value:
             return key
    return "no user"

def json_decode(data):
    data = data.decode('utf-8')
    userData = json.loads(data)
    receiver = userData["receiver"]
    message = userData["message"]
    return receiver,message

def json_encode(sender, receiver, data):
    jsonText = {"sender" : sender, "receiver" : receiver, "message" : data}
    jsonText = json.dumps(jsonText)
    jsonText = jsonText.encode('utf-8')
    return jsonText

def accept(sock, mask):
    conn, addr = sock.accept() # Should be ready
    print('accepted', conn, 'from', addr)
    conn.setblocking(False)
    data = conn.recv(1024)
    receiver, message = json_decode(data)
    if message not in dicionario:
        dicionario[message] = conn
    else:
        print("Username token")
        return
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn, mask):
    data = conn.recv(1000)  # Should be ready
    if data:
        receiver, message = json_decode(data)    
        print('echoing', repr(data), 'to', conn)
        sender = getKey(conn)
        send_msg = json_encode(sender, receiver, message)
        dicionario[receiver].send(send_msg)  # Hope it won't block
    else:
        print('closing', conn)
        sel.unregister(conn)
        conn.close()

sock = socket.socket()
sock.bind(('', 5000)) #se estiver vazio pode receber de todos os computadores
sock.listen(100)
sock.setblocking(False)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)
