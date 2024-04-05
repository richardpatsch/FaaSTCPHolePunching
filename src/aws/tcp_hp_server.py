import datetime
import socket
import threading
import datetime

SERVER_IP = '0.0.0.0'  # Replace with your server IP
SERVER_PORT = 9001  # Replace with your server port
TIMEOUT = 3
BUFFER_SIZE = 1024


class Connection:
    def __init__(self, client_socket, client_address):
        self.client_socket = client_socket
        self.client_address = client_address


def send_message(socket, text):
    msg = bytes(text + "END", encoding='utf-8')
    socket.sendall(msg)
    print(f"sent message: {msg}")


def send_message_to_all(sockets, text):
    for socket in sockets:
        msg = bytes(text + "END", encoding='utf-8')
        socket.sendall(msg)


def server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    sock.listen(1)

    print('Server listening on {}:{}'.format(SERVER_IP, SERVER_PORT))

    #client_addresses = []
    #client_connections = []
    connections = []

    while True:
        try:
            new_connection, client_address = sock.accept()
            print(f'New connection from {client_address}')


            connections.append(Connection(new_connection, client_address))

            if len(connections) == 2:
                # send_message_to_all(client_connections, f'you guys can connect now')
                for i, c in enumerate(connections):
                    other_address = connections[0].client_address if i==1 else connections[1].client_address
                    send_message(c.client_socket, f'{other_address[0]},{other_address[1]}')
                connections.clear() # reset after 2 connected clients
                print(f'connections count reset')
                print(datetime.datetime.now())

        except socket.timeout:
            print('Connection timed out')
            pass


if __name__ == '__main__':
    server()