import socket
import threading
import datetime

SERVER_IP = '0.0.0.0'  # Replace with your server IP
SERVER_PORT = 9001  # Replace with your server port
BUFFER_SIZE = 1024


class Connection:
    def __init__(self, client_socket, client_address):
        self.client_socket = client_socket
        self.client_address = client_address
        self.buffer = []


def send_message(socket, text):
    msg = bytes(text + "END", encoding='utf-8')
    socket.sendall(msg)
    #print(f"sent message: {msg}")


def relay_messages(source_conn, target_conn):
    global BUFFER_SIZE

    # first message has to be the buffer size
    buffer = source_conn.client_socket.recv(10).decode('utf-8')
    BUFFER_SIZE = int(buffer)
    print(f"{source_conn.client_address} set BUFFER SIZE: {BUFFER_SIZE}")

    while True:
        try:
            data = source_conn.client_socket.recv(BUFFER_SIZE)
            if not data:
                # No data means the connection was closed
                print("Connection closed by", source_conn.client_address)
                target_conn.client_socket.close()
                break
            target_conn.client_socket.sendall(data)
        except Exception as e:
            print(f"Error relaying messages: {e}")
            source_conn.client_socket.close()
            target_conn.client_socket.close()
            break


def server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    sock.listen(2)  # Adjusted to expect 2 connections only for simplicity

    print('Server listening on {}:{}'.format(SERVER_IP, SERVER_PORT))


    while True:
        connections = []

        while len(connections) < 2:
            if len(connections) == 1:
                sock.settimeout(5.0)
            else:
                sock.settimeout(None)

            try:
                new_connection, client_address = sock.accept()
                print(f'New connection from {client_address}')
                connections.append(Connection(new_connection, client_address))
            except socket.timeout:
                print('Connection timed out')
                break

        if len(connections) < 2:
            # If we don't have 2 connections, restart the loop
            continue

        for connection in connections:
            send_message(connection.client_socket, f'START')
            print(f"sent go message to: {connection.client_address}")

        print(f"they should be ready by now")

        # Start relaying messages between the two connections
        sock.settimeout(10.0)
        t1 = threading.Thread(target=relay_messages, args=(connections[0], connections[1]))
        t2 = threading.Thread(target=relay_messages, args=(connections[1], connections[0]))

        t1.start()
        t2.start()

        # Wait for both threads to complete before restarting the loop
        t1.join()
        t2.join()
        print("Relay threads completed. Restarting the server loop.")
        connections.clear()


if __name__ == '__main__':
    server()