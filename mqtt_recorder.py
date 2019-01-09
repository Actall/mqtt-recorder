"""MQTT recorder"""

import argparse
import asyncio
import json
import logging
import sys
import base64
import time
from hbmqtt.client import MQTTClient, QOS_0, QOS_1


TOPICS = [("#", QOS_1)]

logger = logging.getLogger('mqtt_recorder')


async def mqtt_record(server: str, output: str = None):
    """Record MQTT messages"""
    mqtt = MQTTClient()
    await mqtt.connect(server)
    await mqtt.subscribe(TOPICS)
    if output is not None:
        output_file = open(output, 'wt')
    else:
        output_file = sys.stdout
    while True:
        message = await mqtt.deliver_message()
        record = {
            'time': time.time(),
            'qos': message.qos,
            'retain': message.retain,
            'topic': message.topic,
            'msg_b64': base64.urlsafe_b64encode(message.data).decode()
        }
        print(json.dumps(record), file=output_file)


async def mqtt_replay(server: str, input: str = None, delay: int = 0, realtime: bool = False):
    """Replay MQTT messages"""

    replay_start_time = time.time()
    recording_start_time = None

    mqtt = MQTTClient()
    await mqtt.connect(server)
    await mqtt.subscribe(TOPICS)
    if input is not None:
        input_file = open(input, 'rt')
    else:
        input_file = sys.stdin
    for line in input_file:
        record = json.loads(line)
        logger.info("%s", record)

        # Throttle publishing if realtime flag is set
        if realtime:
            if recording_start_time is none:
                recording_start_time = msg['time']
            next_event_time = replay_start_time + (msg['time'] - recording_start_time)
            time.sleep(next_event_time - time.time())

        if 'msg_b64' in record:
            msg = base64.urlsafe_b64decode(record['msg_b64'].encode())
        elif 'msg' in record:
            msg = record['msg'].encode()
        else:
            logger.warning("Missing message attribute: %s", record)
            next
        logger.info("Publish: %s", record)
        await mqtt.publish(record['topic'], msg,
                                 retain=record.get('retain'),
                                 qos=record.get('qos', QOS_0))
        if delay > 0:
            time.sleep(delay/1000)


def build_argparser():
    """ Create a parser for command line arguments."""
    parser = argparse.ArgumentParser(description='MQTT recorder')

    parser.add_argument('--server',
                        dest='server',
                        metavar='server',
                        help='MQTT broker',
                        default='mqtt://127.0.0.1/')
    parser.add_argument('--mode',
                        dest='mode',
                        metavar='mode',
                        choices=['record', 'replay'],
                        help='Mode of operation (record/replay)',
                        default='record')
    parser.add_argument('--delay',
                        dest='delay',
                        type=int,
                        default=0,
                        metavar='milliseconds',
                        help='Delay between replayed events')
    parser.add_argument('--realtime',
                        dest='realtime',
                        action='store_true',
                        help='Replay events at the same rate they originally occurred.')
    parser.add_argument('--input',
                        dest='input',
                        metavar='filename',
                        help='Recorded file to replay')
    parser.add_argument('--output',
                        dest='output',
                        metavar='filename',
                        help='Destination file for recording')
    parser.add_argument('--debug',
                        dest='debug',
                        action='store_true',
                        help="Enable debug logging")
    return parser


def validate_arguments(args):
    """ Assertians regarding consistency of arguments."""
    if (args.mode == 'record'):
        assert args.input is None
        assert args.delay == 0
        assert args.realtime is not None
    elif (args.mode == 'replay'):
        assert args.output is None
        assert args.realtime == False, "--realtime flag only applies to replay"
        if args.delay != 0:
            assert args.realtime, "Cannot use both --delay and --realtime features."
    else:
        assert false, "--mode must be 'record' or 'replay'"
    return args


def set_global_config(args):
    """ Configure global settings based on args."""
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)


def main():
    """ Main function"""
    args = build_argparser().parse_args()
    validate_arguments(args)
    set_global_config(args)

    if args.mode == 'replay':
        process = mqtt_replay(server=args.server, input=args.input, delay=args.delay, realtime=args.realtime)
    else:
        process = mqtt_record(server=args.server, output=args.output)

    asyncio.get_event_loop().run_until_complete(process)


if __name__ == "__main__":
    main()
