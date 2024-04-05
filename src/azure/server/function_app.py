# SERVER

import asyncio
import logging
import os
import math
import struct
import datetime
import uuid
import socket
import json
import time
import threading
import psutil
#import aiofiles
import aiohttp
import traceback
import azure.functions as func
from azure.storage.blob.aio import BlobServiceClient, BlobClient
from azure.core.exceptions import AzureError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = func.FunctionApp()

SERVER_IP = '18.153.91.33' # Server to get other Lambda's IP
  # Server to get other Lambda's IP
SERVER_PORT = 9001  # Default to 9001
BUFFER_SIZE = 4096
SERVER_IP_TWO = '18.193.111.130'

CACHE_COLD_START = None

CONNECTION_STRING = 'STRING'
CONNECTION_KEY = 'KEY'
CONTAINER_NAME = 'data'

blob_service_client = None
container_client = None

LOCAL_FILE_PATH_S3_FILE = "/tmp/example_file"
LOCAL_FILE_RECEIVED_FILE = "/tmp/example_file_received"

start_time = None
end_time = None

#INFOS FOR LOGGING RECORD
cold_start = True
S3_DOWNLOAD_START = None
S3_DOWNLOAD_END = None
SEND_TIME_START = None
SEND_TIME_END = None
RUN_ID = None


def get_timestamp():
    # Fetch the current UTC time
    now_utc = datetime.datetime.utcnow()
    # Format the timestamp with microseconds
    formatted_timestamp_with_ms = now_utc.strftime('%H:%M:%S.%f')[:-3]  # Retains microsecond precision
    return formatted_timestamp_with_ms


def get_lambda_memory_size():
    memory_stats = psutil.virtual_memory()
    total_memory_mb = memory_stats.total / (1024 * 1024)
    return total_memory_mb

def generate_random_file_name(extension=None):
    """
    Generates a random file name. An optional file extension can be provided.

    :param extension: Optional file extension (e.g., 'txt', 'jpg').
    :return: A string representing the random file name, with extension if provided.
    """
    file_name = str(uuid.uuid4())  # Generate a random UUID
    if extension:
        return f"{file_name}.{extension}"  # Append extension if provided
    return file_name


async def download_blob(container_name, object_name, local_file_path):
    global blob_service_client
    global blob_client
    #blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    #blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_name)
    try:
        # Download the blob to a local file asynchronously
        async with blob_client as client:
            download_stream = await client.download_blob()
            data = await download_stream.readall()

        # Write the downloaded data to a file
        with open(local_file_path, "wb") as download_file:
            download_file.write(data)
            
        logger.info("Blob was downloaded successfully.")
        return True
    except Exception as e:
        logger.error(f"Error downloading blob: {e}")
        return False


async def send_file_contents(writer, file_path):
    #logger.info(f"send_file_contents {file_path}")
    file_stats = os.stat(LOCAL_FILE_PATH_S3_FILE)
    packet_counts = math.ceil(file_stats.st_size / BUFFER_SIZE)
    logger.info(f"Packet counts: {packet_counts}")
    packed_number = struct.pack('i', packet_counts)


    # Assuming you've already connected and have a writer object
    global SEND_TIME_START
    global SEND_TIME_END
    logger.info(f"start sending now {get_timestamp()}")
    SEND_TIME_START = get_timestamp()

    #send packet amoung first
    writer.write(packed_number)
    await writer.drain()

    #send actual
    i = 0
    with open(file_path, 'rb') as file:
        while True:
            data = file.read(BUFFER_SIZE)  # Read file in chunks
            if not data:
                break  # End of file
            writer.write(data)
            await writer.drain()
            i = i + 1

    SEND_TIME_END = get_timestamp()
    logger.info(f"sent {i} packets")


async def download_and_send_file(bucket_name, object_name, peer_ip, peer_port):
    # Step 1: Download file from S3
    await download_blob(bucket_name, object_name, LOCAL_FILE_PATH_S3_FILE)

    # Step 2: Connect to peer and prepare for sending file
    reader, writer = await connect_with_retry(peer_ip, peer_port, 9002)

    # Step 3: Send the file contents
    await send_file_contents(writer, LOCAL_FILE_PATH_S3_FILE)

    # Cleanup: Close the writer
    writer.close()
    await writer.wait_closed()


async def connect_with_reusable_port(remote_host, remote_port, local_port):
    # Create a socket manually
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Set socket options to reuse address and port
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # On platforms supporting SO_REUSEPORT, uncomment the following line
    # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    # Bind the socket to a specific local port
    sock.bind(('', local_port))

    # Make the socket non-blocking and initiate the connection
    sock.setblocking(False)
    await asyncio.get_event_loop().sock_connect(sock, (remote_host, remote_port))
    ##await asyncio.wait_for(asyncio.open_connection(sock=sock, ))
    #loop = asyncio.get_running_loop()
    #await loop.sock_connect(sock, (remote_host, remote_port))

    # At this point, the connection is established, and you can use the socket
    # with asyncio's open_connection, passing the existing socket as the parameter
    reader, writer = await asyncio.open_connection(sock=sock)
    
    local_address = sock.getsockname()  # Returns a tuple (local_ip, local_port)
    logger.info(f"Socket is bound to local address: {local_address}")

    return reader, writer


async def connect_with_retry(host, port, local_port, max_retries=3, timeout=3):
    logger.info(f"connect_with_retry(host={host}, port={port}, local_port={local_port}, max_retries={max_retries}, timeout={timeout})")
    """
    Attempt to connect to a socket at (host, port) with retries.
    
    Parameters:
    - host: The hostname or IP address to connect to.
    - port: The port number to connect to.
    - max_retries: The maximum number of retries for the connection.
    - timeout: The connection attempt timeout in seconds.
    
    Returns:
    A connected socket object if successful, None otherwise.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            # Create a socket object
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', local_port))
            s.setblocking(False)
            s.settimeout(timeout)  # Set the timeout for the connection attempt

            await asyncio.get_event_loop().sock_connect(s, (host, port))
            logger.info(f"after sock_connect")
            reader, writer = await asyncio.open_connection(sock=s)
            logger.info(f"after open_connection")

            logger.info(f"Connection established to {host}:{port} on attempt {attempt + 1}")
            return reader, writer  # Return the connected socket object
        except (socket.timeout, socket.error) as e:
            logger.error(f"Attempt {attempt + 1}: Could not connect to {host}:{port}. Retrying...")
            attempt += 1
            time.sleep(1)  # Wait for a short period before retrying
        finally:
            if attempt == max_retries:
                logger.info("Maximum retry attempts reached. Failed to connect.")
            elif attempt < max_retries:
                s.close()  # Close the socket if retrying
    return None  # Return None if connection was not established


async def send_message(writer, message):
    """Asynchronously send a message."""
    writer.write((message + "END").encode('utf-8'))
    await writer.drain()


async def send_message_wo_end(writer, message):
    writer.write((message).encode('utf-8'))
    await writer.drain()


async def send_large_data(writer, data):
    # Ensure data is bytes
    if isinstance(data, str):
        data = data.encode('utf-8')  # Encode string to bytes

    # Prefix the message with its length
    length_prefix = len(data)
    length_prefix_bytes = length_prefix.to_bytes(4, byteorder='big')  # 4 bytes to represent the length
    logger.info(f"will send {length_prefix}")
    writer.write(length_prefix_bytes + data)
    await writer.drain()

async def connect_with_reusable_port_dos(remote_host, remote_port, local_port, max_retries = 3, timeout = None):
    logger.info(f"connect_with_reusable_port_dos({remote_host}, {remote_port}, {local_port})")
    attempt = 0
    success = False

    while attempt < max_retries:
        try: 
            # Create a socket manually
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("1")

            # Set socket options to reuse address and port
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            logger.info("2")

            # On platforms supporting SO_REUSEPORT, uncomment the following line
            # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            #sock.settimeout(3)
            #sock.setblocking(False)
            sock.settimeout(timeout)

            # Bind the socket to a specific local port
            sock.bind(('', local_port))
            logger.info("3")

            # Make the socket non-blocking and initiate the connection
            #sock.setblocking(False)
            await asyncio.wait_for(asyncio.get_event_loop().sock_connect(sock, (remote_host, remote_port)), 3)
            #await asyncio.get_event_loop().sock_connect(sock, (remote_host, remote_port))
            logger.info("4")

            # At this point, the connection is established, and you can use the socket
            # with asyncio's open_connection, passing the existing socket as the parameter
            reader, writer = await asyncio.open_connection(sock=sock)
            logger.info("5")

            success = True

            return reader, writer
        except (socket.timeout, socket.error, asyncio.TimeoutError) as e:
            logger.error(f"Attempt {attempt + 1}: Could not connect to {remote_host}:{remote_port}. Retrying...")
            attempt += 1
            time.sleep(1)  # Wait for a short period before retrying
        finally:
            if attempt == max_retries:
                logger.info("Maximum retry attempts reached. Failed to connect.")
            elif attempt < max_retries and success == False:
                logger.info("SOCKET CLOSED")
                sock.close()  # Close the socket if retrying
    return None, None


async def receive_large_data(reader):
    logger.info("--receive-large-data--")
    # Read the length prefix first
    length_prefix = await reader.readexactly(4)  # Read exactly 4 bytes for the length
    message_length = int.from_bytes(length_prefix, byteorder='big')
    logger.info(f"incoming {message_length}")

    # Read the message data based on the length prefix
    data_chunks = []
    while message_length > 0:
        chunk = await reader.read(min(message_length, BUFFER_SIZE))  # Read in chunks up to BUFFER_SIZE
        if not chunk:
            raise RuntimeError("Connection closed before all data was received")
        data_chunks.append(chunk)
        message_length -= len(chunk)
        logger.info(f"remaining length {message_length} bytes")

    logger.info(f"receiving done! {len(data_chunks)}")
    # Combine the chunks into the final message
    message = b''.join(data_chunks)
    return message


async def read_messages(reader):
    """Asynchronously read a message from the reader."""
    logger.info("read_message()")
    data = await reader.read(BUFFER_SIZE)
    messages = data.decode()
    split_messages = messages.split('END')[:-1]
    return split_messages


# Handler functions for different commands
async def handle_echo_command(message, writer):
    await send_message(writer, f"Echo: {message}")



# Command dispatcher
async def dispatch_command(reader, writer):
    while True:
        messages = await read_messages(reader)
        for message in messages:
            if message == "":
                logger.info("Connection closed by peer.")
                writer.close()
                await writer.wait_closed()
                break
            if message.startswith("echo"):
                await handle_echo_command(message[5:], writer)
            else:
                await send_message(writer, "Unknown command")


async def fetch_peer_info(ip):
    """Fetch other Lambda's IP and port from a central server."""
    #reader, writer = await connect_with_retry(SERVER_IP, SERVER_PORT, 9002)
    reader, writer = await connect_with_reusable_port_dos(ip, SERVER_PORT, 9002)
    logger.info("connected to the relay server")
    response = await read_messages(reader)
    logger.info("received messages")
    peer_info = response[0]
    writer.close()
    await writer.wait_closed()
    return peer_info


async def process_command(command, writer):
    """Process a command and return a response."""
    # Example command processing
    if command == "hello":
        return "Hello received"
    elif command.startswith("ADD "):
        number = int(command[4:])+1 # increment by 1
        await send_message(writer, f"ADD {number}")
        return f"Addition for: {number}"
    elif command.startswith("DATA"):
        return f"DATA"
    else:
        return "Unknown command"

def read_with_timeout(sock, num_bytes, timeout_seconds):
    data = None
    def target():
        nonlocal data
        data = sock.recv(num_bytes)

    thread = threading.Thread(target=target)
    thread.start()

    thread.join(timeout=timeout_seconds)
    if thread.is_alive():
        print("The read operation has timed out.")
        return None
    return data


async def receive_and_save_file(reader, output_file_path):
    start_time = time.perf_counter()
    with open(output_file_path, 'wb') as file:
        while True:
            chunk = await read_with_timeout(reader, BUFFER_SIZE, 1)
            if not chunk:
                break
            file.write(chunk)
    end_time = time.perf_counter()
    logger.info(f"received file from socket in  {end_time - start_time:0.4f} seconds")

    #logger.info(f"File received and saved to {output_file_path}")
    time.sleep(1)

async def interact_with_peer(reader, writer, data):
    """Send a command to the peer, wait for a response, and process it."""
    global RUN_ID
    global S3_DOWNLOAD_START
    global S3_DOWNLOAD_END

    S3_DOWNLOAD_START = get_timestamp()
    await download_blob(CONTAINER_NAME, S3_KEY, LOCAL_FILE_PATH_S3_FILE)
    S3_DOWNLOAD_END = get_timestamp()

    RUN_ID = str(uuid.uuid4())
    await send_message_wo_end(writer, RUN_ID)
    await send_message_wo_end(writer, "DATA")

    #logger.info(f"downloaded file from S3 in  {S3_DOWNLOAD_TIME} seconds")
    # Step 3: Send the file contents
    await send_file_contents(writer, LOCAL_FILE_PATH_S3_FILE)
    #logger.info(f"file sent in  {SENDING_DURATION} seconds")

    start_time = time.perf_counter()
    while True:
        response = await reader.readexactly(5)
        response = response.decode('utf-8')

        if response == 'CLOSE':
            end_time = time.perf_counter()
            logger.info(f"Connection will be closed after waiting for {end_time - start_time:0.4f} seconds")
            break



async def connect_to_peer(peer_ip, peer_port):
    """Connect to the peer Lambda given its IP and port."""
    logger.info(f"connect_to_peer({peer_ip}, {peer_port})")
    #reader, writer = await connect_with_retry(peer_ip, peer_port, 9002)
    reader, writer = await connect_with_reusable_port_dos(peer_ip, peer_port, 9002, max_retries = 15)

    '''
    ip_one = '0.0.0.0'
    port_one = 9002

    if reader == None:
        logger.info("reader was none, so trying to host socket")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((ip_one, port_one))
        sock.settimeout(20)
        sock.listen(1)
        print('Server listening on {}:{}'.format(ip_one, port_one))

        #while True:
        try: 
            new_connection, client_address = sock.accept()
            logger.info(f'new connection from {client_address}')        
        except socket.timeout:
            logger.error(f'connections timed out')
            pass
    '''
    
    logger.info("a")
    await interact_with_peer(reader, writer, None)
    logger.info("b")
    writer.close()
    await writer.wait_closed()
    logger.info("c")


@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
async def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logger.info(generate_random_file_name())

    try:
        global cold_start
        global BUFFER_SIZE
        global S3_KEY
        global CACHE_COLD_START
        global blob_service_client
        global container_client
        global SERVER_IP

        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        logger.info(f"cold start: {cold_start}")
        CACHE_COLD_START = cold_start
        cold_start = False

        req_body = req.get_json()

        BUFFER_SIZE = int(req_body.get('buffer'))
        logger.info(f"BUFFER_SIZE set to: {BUFFER_SIZE}")
        S3_KEY = req_body.get('filename')
        logger.info(f"S3_KEY set to: {S3_KEY}")

        if 'server_ip' in req_body:
            SERVER_IP = req_body.get('server_ip')
            logger.info(f"server ip set to: {SERVER_IP}")

        #success = await download_blob(CONTAINER_NAME, S3_KEY, LOCAL_FILE_PATH_S3_FILE)

        #if success:
        #    logger.info("Blob was downloaded successfully.")

        #loop = asyncio.get_event_loop()

        # Fetch other Lambda's IP and port
        logger.info("before fetch_peer_info()")
        peer_info = await fetch_peer_info(SERVER_IP)
        logger.info("after fetch_peer_info()")

        peer_ip, peer_port = peer_info.split(',')
        logger.info(f"Connecting to peer at {peer_ip}:{peer_port}")

        # Fetch other Lambda's IP and port
        logger.info("before fetch_peer_info()")
        peer_info = await fetch_peer_info(SERVER_IP_TWO)
        logger.info("after fetch_peer_info()")

        peer_ip, peer_port = peer_info.split(',')
        logger.info(f"Connecting to peer at {peer_ip}:{peer_port}")

        # Connect to peer and send a message
        logger.info("before connect_to_peer()")
        await connect_to_peer(peer_ip, int(peer_port))
        logger.info("after connect_to_peer()")

        #logger.info(f"{cold_start}; {S3_KEY}; {BUFFER_SIZE}; {S3_DOWNLOAD_TIME}; {SEND_TIME_START}; {SENDING_DURATION}; {RUN_ID}; ")

        log_data = {
            "cold_start_server": CACHE_COLD_START,
            "s3_key": S3_KEY,
            "buffer_size_server": BUFFER_SIZE,
            "send_time_start": SEND_TIME_START,
            "send_time_end": SEND_TIME_END,
            "s3_download_start": S3_DOWNLOAD_START,
            "s3_download_end": S3_DOWNLOAD_END,
            "run_id": RUN_ID,
            "memory_server": int(get_lambda_memory_size())
        }

        CACHE_COLD_START = None

        logger.info(json.dumps(log_data))
        
        return func.HttpResponse(
                "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
                status_code=200
        )    
    except Exception as e: 
        error_text =  f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
        logger.exception(error_text)
        return func.HttpResponse(
            error_text,
            status_code=501
        )

