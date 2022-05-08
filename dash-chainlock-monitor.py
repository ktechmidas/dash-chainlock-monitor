import binascii
import zmq
import struct
import datetime
import threading
import time
import logging
import os

# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
logger = logging.getLogger(__name__)

channel_id = "C03E3PVEGDD"
#channel_id = "C03ELP73D2Q"

port = 20003

#These are global since we're using multithreading. Please don't shoot me
currentblockhash = "a0"
blocktime = ""
prevblockhash = ""
chainlocked = False
timenotlocked = 0
informed = 0

def process_zmq_message(topic, body):
    global currentblockhash
    global chainlocked
    global blocktime

    block_seen_time = datetime.datetime.utcnow()
    blockhash = binascii.hexlify(body).decode("utf-8")
    print('{}\tTopic received: {}\tData: {}'.format(block_seen_time, topic, blockhash))

    chainlock_status = False
    chainlock_seen_time = None

    # Set ChainLock Status
    if topic == "hashchainlock":
        chainlock_status = True
        chainlock_seen_time = block_seen_time

    if blockhash == currentblockhash:
        existing_block = True
    else:
        existing_block = False

    if existing_block:
        # Update Only
        data = (chainlock_status, chainlock_seen_time, blockhash)
        chainlocked = chainlock_status
        print(data, flush=True)
    else:
        # Insert
        data = (blockhash, chainlock_status, block_seen_time, chainlock_seen_time)
        currentblockhash = blockhash
        chainlocked = chainlock_status
        print(data, flush=True)

def listen_to_zmq():
        msg = zmqSubSocket.recv_multipart()
        topic = str(msg[0].decode("utf-8"))
        body = msg[1]
        sequence = "Unknown"

        if len(msg[-1]) == 4:
          msgSequence = struct.unpack('<I', msg[-1])[-1]
          sequence = str(msgSequence)

        if topic == "hashblock" or topic == "hashchainlock":
            process_zmq_message(topic, body)
        
        listen_to_zmq()

def monitor_chainlocks():
    global currentblockhash
    global prevblockhash
    global chainlocked
    global timenotlocked

    print(currentblockhash)
    
    if currentblockhash == "a0":
        print("No blocks received yet... sleeping", flush=True)
        time.sleep(10)
    else:
        print("Block received", flush=True)
        if chainlocked == False and currentblockhash == prevblockhash:
            timenotlocked = timenotlocked + 10
        else:
            timenotlocked = 0
            print("block "+currentblockhash+" locked")
        time.sleep(10)
    
    if timenotlocked > 30:
        try:
            # Call the conversations.list method using the WebClient
            result = client.chat_postMessage(
            channel=channel_id,
            text="ALERT: Block "+currentblockhash+" not locked for "+timenotlocked+" seconds. Please check!"
            # You could also use a blocks[] array to send richer content
            )

        except SlackApiError as e:
            print(f"Error: {e}")
    elif timenotlocked > 90:
        try:
            if informed == 0:
                # Call the conversations.list method using the WebClient
                result = client.chat_postMessage(
                channel=channel_id,
                text="<!channel> ALERT: Block "+currentblockhash+" not locked for "+timenotlocked+" seconds. Please check!"
                # You could also use a blocks[] array to send richer content
                )
                informed = informed + 1
            else:
                # Call the conversations.list method using the WebClient
                result = client.chat_postMessage(
                channel=channel_id,
                text="<!channel> ALERT: Block "+currentblockhash+" not locked for "+timenotlocked+" seconds. This will be the final message from the bot in order to not overload the channel with useless information. Please restart manually"
                # You could also use a blocks[] array to send richer content
                )
                quit()


        except SlackApiError as e:
            print(f"Error: {e}")
    
    prevblockhash = currentblockhash
    monitor_chainlocks()

# ZMQ Setup
zmqContext = zmq.Context()
zmqSubSocket = zmqContext.socket(zmq.SUB)
zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashblock")
zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashchainlock")
zmqSubSocket.connect("tcp://138.68.22.77:%i" % port)


# I banish thee to another thread, main thread stopping task.
x= threading.Thread(target=listen_to_zmq)
x.start()

y = threading.Thread(target=monitor_chainlocks)
y.start()