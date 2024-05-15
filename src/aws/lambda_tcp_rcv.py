import asyncio
import json
import logging
import os
import datetime
import socket
import struct
import time
import uuid
import boto3
from botocore.exceptions import NoCredentialsError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

EC2_INSTANCE_ID = os.environ.get('EC2_INSTANCE_ID')

SERVER_IP = os.environ.get('EC2_IP')  # Server to get other Lambda's IP
SERVER_PORT = 9001  # Default to 9001
BUFFER_SIZE = 4096
s3_client = boto3.client('s3')

S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
S3_KEY = os.environ.get('S3_KEY')

LOCAL_FILE_PATH_S3_FILE = "/tmp/example_file"
LOCAL_FILE_RECEIVED_FILE = "/tmp/example_file_received"

CACHE_COLD_START = None


start_time = None
end_time = None

#logging data
cold_start = True
RUN_ID = None
RECEIVED_FILE_START = None
RECEIVED_FILE_END = None
RECEIVED_FILE_SIZE = None
S3_UPLOAD_START = None
S3_UPLOAD_END = None



def get_timestamp():
    # Fetch the current UTC time
    now_utc = datetime.datetime.utcnow()
    # Format the timestamp with microseconds
    formatted_timestamp_with_ms = now_utc.strftime('%Y-%m-%d %H:%M:%S.%f')  # Retains microsecond precision
    return formatted_timestamp_with_ms


def get_lambda_memory_size():
    memory_size = os.getenv('AWS_LAMBDA_FUNCTION_MEMORY_SIZE')
    return memory_size


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
    # Assuming you've already connected and have a writer object
    i = 0
    with open(file_path, 'rb') as file:
        while True:
            data = file.read(BUFFER_SIZE)  # Read file in chunks
            if not data:
                break  # End of file
            writer.write(data)
            i = i + 1

        await writer.drain()
    #logger.info("leave send_file_content method")


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
    global RECEIVED_FILE_START
    global RECEIVED_FILE_END

    RECEIVED_FILE_START = get_timestamp()
    #receive packet numbers:
    packet = await reader.readexactly(4)
    received_number = struct.unpack('i', packet)[0]
    logger.info(f"There should be {received_number} packets incoming!")

    i = 0
    with open(output_file_path, 'wb') as file:
        while True:
            chunk = await read_with_timeout(reader, BUFFER_SIZE, 0.5)

            if not chunk:
                logger.info(f"chunk is empty at {i}")
                if i >= received_number:
                    break
            else:
                i = i + 1

            file.write(chunk)
    RECEIVED_FILE_END = get_timestamp()
    logger.info(f"Received {i} packets")
    #logger.info(f"received file from socket in  {RECEIVED_DURATION} seconds")


async def interact_with_peer(reader, writer, data):
    global RUN_ID

    while True:
        response = await reader.readexactly(36)
        RUN_ID = response.decode('utf-8')

        response = await reader.readexactly(4)
        response = response.decode('utf-8')

        # Process the response (you could also send another command based on the response)
        processed_response = await process_command(response, writer)
        if processed_response == "DATA":
            await receive_and_save_file(reader, LOCAL_FILE_RECEIVED_FILE)
            file_stats = os.stat(LOCAL_FILE_RECEIVED_FILE)
            global RECEIVED_FILE_SIZE
            RECEIVED_FILE_SIZE = round(file_stats.st_size / (1024 * 1024),2)
            logger.info(f'Received File Size in MegaBytes is {RECEIVED_FILE_SIZE}')

            global S3_UPLOAD_START
            global S3_UPLOAD_END
            new_file_name = generate_random_file_name("jpg")

            S3_UPLOAD_START = get_timestamp()
            upload_successful = upload_file_to_s3(LOCAL_FILE_RECEIVED_FILE, S3_BUCKET, new_file_name )
            S3_UPLOAD_END = get_timestamp()
            #S3_UPLOAD_DURATION = f"{end_time - start_time:0.4f}"
            #logger.info(f"uploaded received to s3 in  {S3_UPLOAD_DURATION} seconds")
            if upload_successful:
                logger.info(f"upload successful")
                logger.info(f"{new_file_name}")

            break

    logger.info(f"send CLOSE message!")
    await send_message_wo_end(writer, 'CLOSE')

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

    #event variable: 'buffer'
    if 'buffer' in event:
        BUFFER_SIZE = int(event['buffer'])
        logger.info(f"Buffer size set to: {BUFFER_SIZE}")

    logger.info(f"cold start: {cold_start}")
    CACHE_COLD_START = cold_start
    cold_start = False

    loop = asyncio.get_event_loop()

    # Fetch other Lambda's IP and port
    peer_info = loop.run_until_complete(fetch_peer_info())
    peer_ip, peer_port = peer_info.split(',')
    logger.info(f"Connecting to peer at {peer_ip}:{peer_port}")

    # Connect to peer and send a message
    loop.run_until_complete(connect_to_peer(peer_ip, int(peer_port)))

    #logger.info(f"{cold_start}; {BUFFER_SIZE}; {RECEIVED_FILE_SIZE}; {RECEIVED_DURATION}; {RECEIVED_FILE_TIMESTAMP}; {S3_UPLOAD_DURATION}; {RUN_ID}")

    # Organizing data into a dictionary
    log_data = {
        "cold_start_client": CACHE_COLD_START,
        "buffer_size_client": BUFFER_SIZE,
        "received_file_size": RECEIVED_FILE_SIZE,
        "received_file_start": RECEIVED_FILE_START,
        "received_file_end": RECEIVED_FILE_END,
        "s3_upload_start": S3_UPLOAD_START,
        "s3_upload_end": S3_UPLOAD_END,
        "run_id": RUN_ID,
        "memory_client": int(get_lambda_memory_size())
    }

    CACHE_COLD_START = None

    # Converting dictionary to JSON string and logging
    logger.info(json.dumps(log_data))

    return {
        "statusCode": 200,
        "body": json.dumps("It's over, it's done."),
        "mode": "client"
    }
