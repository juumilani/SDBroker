import zmq
import logging

from time import time
from threading import Thread

logging.basicConfig(level=logging.INFO)


class Broker:

    def __init__(self, cport=4321, rport=4322):

        self.collectors = {}
        self.reader = []
        self.collector_port = cport
        self.reader_port = rport
        
        self.poller = None
        self.backend = None
        self.frontend = None

        self.heartbeat_interval = 2
        self.heartbeat_at = time() + self.heartbeat_interval
        
        logger = logging.getLogger().handlers[0]
        fmt = logging.Formatter('[Broker] %(levelname)s: %(message)s')
        logger.setFormatter(fmt)

        self.start()

    def start(self):

        context = zmq.Context.instance()

        # Router for collectors
        logging.info(f'Listening to collectors in port {self.collector_port}.')
        self.backend = context.socket(zmq.ROUTER)
        self.backend.bind(f'tcp://*:{self.collector_port}')

        # Router for reader
        logging.info(f'Listening to collectors in port {self.reader_port}.')
        self.frontend = context.socket(zmq.ROUTER)
        self.frontend.bind(f'tcp://*:{self.reader_port}')

        # Register poller to both routers
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)

    def relay(self, frames):

        # Function that relays the frames from the data collector to its reader
        address = frames[0]
        content = frames[1]
        
        logging.info(f'Relaying message from {address} to reader...')
        for reader in self.collectors[address]:
            message = [reader, content]
            self.frontend.send_multipart(message)

    def run(self):
        while True:
            socks = dict(self.poller.poll(self.heartbeat_interval * 1000))

            # When a message is recieved from the data collector
            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()
                if not frames:
                    logging.info('Received an empty message!')
                else:
                    address = frames[0]

                    # Register new collector connecting to the broker
                    if address not in self.collectors:
                        self.collectors[address] = []

                    # Relay new message to all readers registered to this service
                    if len(self.collectors[address]) > 0 and b'HEARTBEAT' not in frames:
                        self.relay(frames)
                    if b'HEARTBEAT' not in frames:
                        logging.info(f'Received {frames[1]} from {address}')

            # When a message is recieved from the reader
            if socks.get(self.frontend) == zmq.POLLIN:
                frames = self.frontend.recv_multipart()
                address = frames[0]

                # Register new reader connecting to the broker
                if address not in self.reader:
                    self.reader.append(address)
                logging.info(f'New request from reader: {frames[1:]}')

                # Tries to register the reader to the queue asked
                if b'REGISTER' in frames:
                    if self.collectors.get(frames[2]) is not None:
                        self.collectors[frames[2]].append(address)
                        response = [address, b'OK']
                    else:
                        response = [address, b'BAD REQUEST']
                    self.frontend.send_multipart(response)
                    
            # Send heartbeat to idle collectors
            if time() > self.heartbeat_at:
                for collector in self.collectors.keys():
                    msg = [collector, b'HEARTBEAT']
                    self.backend.send_multipart(msg)
                self.heartbeat_at = time() + self.heartbeat_interval 


def main():
    broker = Broker()
    try:
        broker.run()
    except Exception as e:
        logging.error(e)
        logging.info('Broker has been stopped.')


if __name__ == "__main__":
    main()