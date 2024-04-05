
import asyncio
import json
import logging
import os
import secrets
import socket
import time
import uuid
import boto3
from botocore.exceptions import NoCredentialsError
import datetime
import math
import struct

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EC2_INSTANCE_ID = os.environ.get('EC2_INSTANCE_ID')

SERVER_IP = os.environ.get('EC2_IP')  # Server to get other Lambda's IP
SERVER_PORT = 9001  # Default to 9001
BUFFER_SIZE = 4096
s3_client = boto3.client('s3')

CACHE_COLD_START = None

S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
S3_KEY = os.environ.get('S3_KEY')

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
    memory_size = os.getenv('AWS_LAMBDA_FUNCTION_MEMORY_SIZE')
    return memory_size

def upload_file_to_s3(file_name, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except NoCredentialsError:
        logger.error("Credentials not available")
        return False
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return False

    return True

async def download_file_from_s3(bucket_name, object_name, local_file_path):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, s3_client.download_file, bucket_name, object_name, local_file_path)


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
    await download_file_from_s3(bucket_name, object_name, LOCAL_FILE_PATH_S3_FILE)

    # Step 2: Connect to peer and prepare for sending file
    reader, writer = await connect_with_reusable_port(peer_ip, peer_port, 9002)

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

    # At this point, the connection is established, and you can use the socket
    # with asyncio's open_connection, passing the existing socket as the parameter
    reader, writer = await asyncio.open_connection(sock=sock)

    return reader, writer

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


async def fetch_peer_info():
    """Fetch other Lambda's IP and port from a central server."""
    reader, writer = await connect_with_reusable_port(SERVER_IP, SERVER_PORT, 9002)

    response = await read_messages(reader)
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

async def read_with_timeout(reader, num_bytes, timeout_seconds):
    try:
        # Wait for the read operation, with timeout
        data = await asyncio.wait_for(reader.read(num_bytes), timeout_seconds)
        return data
    except asyncio.TimeoutError:
        print("The read operation has timed out.")
        return None


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
    await download_file_from_s3(S3_BUCKET, S3_KEY, LOCAL_FILE_PATH_S3_FILE)
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
    reader, writer = await connect_with_reusable_port(peer_ip, peer_port, 9002)

    await interact_with_peer(reader, writer, None)
    writer.close()
    await writer.wait_closed()


def handler(event, context):
    global cold_start
    global BUFFER_SIZE
    global S3_KEY
    global CACHE_COLD_START

    logger.info(f"cold start: {cold_start}")
    CACHE_COLD_START = cold_start
    cold_start = False

    if 'buffer' in event:
        BUFFER_SIZE = int(event['buffer'])
        logger.info(f"BUFFER_SIZE set to: {BUFFER_SIZE}")

    if 'filename' in event:
        S3_KEY = event['filename']
        logger.info(f"S3_KEY set to: {S3_KEY}")

    loop = asyncio.get_event_loop()

    # Fetch other Lambda's IP and port
    peer_info = loop.run_until_complete(fetch_peer_info())
    peer_ip, peer_port = peer_info.split(',')
    logger.info(f"Connecting to peer at {peer_ip}:{peer_port}")

    # Connect to peer and send a message
    loop.run_until_complete(connect_to_peer(peer_ip, int(peer_port)))

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

    return {
        "statusCode": 200,
        "body": json.dumps("It's over, it's done."),
        "mode": "server"
    }
