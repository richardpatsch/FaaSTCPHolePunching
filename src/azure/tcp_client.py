import azure.functions as func
import datetime
import json
import logging
import json
import sys
import logging
import socket
from threading import Event, Thread
import struct
from collections import namedtuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
STOP = Event()

app = func.FunctionApp()

def accept(context, port):
    context.thread_local_storage.invocation_id = context.invocation_id
    logger.info("accept %s", port)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind(('', port))
    s.listen(1)
    s.settimeout(5)
    while not STOP.is_set():
        try:
            conn, addr = s.accept()
        except socket.timeout:
            continue
        else:
            logger.info("Accept %s connected!", port)
            # STOP.set()


def connect(context, local_addr, addr):
    context.thread_local_storage.invocation_id = context.invocation_id
    logger.info("connect from %s to %s", local_addr, addr)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind(local_addr)
    while not STOP.is_set():
        try:
            s.connect(addr)
        except socket.error:
            continue
        # except Exception as exc:
        #     logger.exception("unexpected exception encountered")
        #     break
        else:
            logger.info("connected from %s to %s success!", local_addr, addr)
            # STOP.set()


def main(context: func.Context, host='18.185.109.81', port=9001):
    sa = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sa.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sa.connect((host, port))
    priv_addr = sa.getsockname()

    send_msg(sa, addr_to_msg(priv_addr))
    data = recv_msg(sa)
    logger.info("client %s %s - received data: %s", priv_addr[0], priv_addr[1], data)
    pub_addr = msg_to_addr(data)
    send_msg(sa, addr_to_msg(pub_addr))

    data = recv_msg(sa)
    pubdata, privdata = data.split(b'|')
    client_pub_addr = msg_to_addr(pubdata)
    client_priv_addr = msg_to_addr(privdata)
    logger.info(
        "client public is %s and private is %s, peer public is %s private is %s",
        pub_addr, priv_addr, client_pub_addr, client_priv_addr,
    )

    threads = {
        '0_accept': Thread(target=accept, args=(context, priv_addr[1],)),
        '1_accept': Thread(target=accept, args=(context, client_pub_addr[1],)),
        '2_connect': Thread(target=connect, args=(context, priv_addr, client_pub_addr,)),
        '3_connect': Thread(target=connect, args=(context, priv_addr, client_priv_addr,)),
    }
    
    for name in sorted(threads.keys()):
        logger.info('start thread %s', name)
        threads[name].start()

    while threads:
        keys = list(threads.keys())
        for name in keys:
            try:
                threads[name].join(1)
            except TimeoutError:
                continue
            if not threads[name].is_alive():
                threads.pop(name)


@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    main(context)

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
def addr_from_args(args, host='127.0.0.1', port=9999):
    if len(args) >= 3:
        host, port = args[1], int(args[2])
    elif len(args) == 2:
        host, port = host, int(args[1])
    else:
        host, port = host, port
    return host, port


def msg_to_addr(data):
    ip, port = data.decode('utf-8').strip().split(':')
    return (ip, int(port))


def addr_to_msg(addr):
    return '{}:{}'.format(addr[0], str(addr[1])).encode('utf-8')


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


class Client(namedtuple('Client', 'conn, pub, priv')):

    def peer_msg(self):
        return addr_to_msg(self.pub) + b'|' + addr_to_msg(self.priv)
