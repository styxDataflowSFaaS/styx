import asyncio
import os
import socket

import aiojobs
import aiozmq
import uvloop
import zmq

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.serialization import Serializer
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.stateful_function import make_key_hashable, StrKeyNotUUID, NonSupportedKeyType

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
registered_operator_connections: dict[str, dict[str, tuple[str, int]]] = {}


async def get_registered_operators(networking_manager):
    global registered_operator_connections
    registered_operator_connections = await networking_manager.send_message_request_response(DISCOVERY_HOST,
                                                                                             DISCOVERY_PORT,
                                                                                             "",
                                                                                             "",
                                                                                             {"__COM_TYPE__": "DISCOVER",
                                                                                              "__MSG__": ""},
                                                                                             Serializer.MSGPACK)
    logging.info(registered_operator_connections)


async def main():
    global registered_operator_connections
    router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")  # coordinator
    scheduler = await aiojobs.create_scheduler(limit=None)
    networking_manager: NetworkingManager = NetworkingManager()
    logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT} IP:{socket.gethostbyname(socket.gethostname())}")
    while True:
        resp_adr, data = await router.read()
        deserialized_data: dict = networking_manager.decode_message(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message: dict = deserialized_data['__MSG__']
            if message_type == 'REMOTE_FUN_CALL':
                logging.warning(f"Received message that calls function {message['__FUN_NAME__']}")
                # RECEIVE MESSAGE FROM A CLIENT TO PASS INTO A STATEFLOW GRAPH'S OPERATOR FUNCTION
                operator_name = message['__OP_NAME__']
                function_name = message['__FUN_NAME__']
                key = message['__KEY__']
                if operator_name not in registered_operator_connections:
                    await scheduler.spawn(get_registered_operators(networking_manager))
                    await asyncio.sleep(1)  # !!!! wait for the reply to return should lock it with an asyncio event
                try:
                    try:
                        partition: str = str(int(make_key_hashable(key)) %
                                             len(registered_operator_connections[operator_name].keys()))
                        worker: tuple[str, int] = registered_operator_connections[operator_name][partition]
                    except KeyError:
                        await scheduler.spawn(get_registered_operators(networking_manager))
                        await asyncio.sleep(1)  # !!! wait for the reply to return should lock it with an asyncio event
                        logging.info(registered_operator_connections)
                        partition: str = str(
                            int(make_key_hashable(key)) % len(
                                registered_operator_connections[operator_name].keys()))
                        worker: tuple[str, int] = registered_operator_connections[operator_name][partition]
                    worker: StateflowWorker = StateflowWorker(worker[0], worker[1])
                    logging.debug(f"Opening connection to: {worker.host}:{worker.port}")
                    message.update({'__PARTITION__': int(partition)})
                    logging.debug(f'Sending packet: {message} to {worker.host}:{worker.port}')

                    await scheduler.spawn(networking_manager.send_message(worker.host,
                                                                          worker.port,
                                                                          operator_name,
                                                                          function_name,
                                                                          {"__COM_TYPE__": "RUN_FUN",
                                                                           "__MSG__": message},
                                                                          Serializer.MSGPACK))
                except StrKeyNotUUID:
                    logging.error(f"String key: {key} is not a UUID")
                except NonSupportedKeyType:
                    logging.error(f"Supported keys are integers and UUIDS not {type(key)}")

            else:
                logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
