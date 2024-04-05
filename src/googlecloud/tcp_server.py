#!/usr/bin/env python
import sys
import logging
import socket
import struct
import fcntl
import os
from collections import namedtuple

logger = logging.getLogger()
clients = {}


def main(host='0.0.0.0', port=9001):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(1)
    s.settimeout(30)
    logger.info(f'Listening on {port}')

    while True:
        try:
            conn, addr = s.accept()
        except socket.timeout:
            continue

        logger.info('connection address: %s', addr)
        data = recv_msg(conn)
        priv_addr = msg_to_addr(data)
        send_msg(conn, addr_to_msg(addr))
        data = recv_msg(conn)
        data_addr = msg_to_addr(data)
        if data_addr == addr:
            logger.info('client reply matches')
            clients[addr] = Client(conn, addr, priv_addr)
        else:
            logger.info('client reply did not match')
            conn.close()

        logger.info('server - received data: %s', data)

        if len(clients) == 2:
            (addr1, c1), (addr2, c2) = clients.items()
            logger.info('server - send client info to: %s', c1.pub)
            send_msg(c1.conn, c2.peer_msg())
            logger.info('server - send client info to: %s', c2.pub)
            send_msg(c2.conn, c1.peer_msg())
            clients.pop(addr1)
            clients.pop(addr2)

    conn.close()


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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    main()