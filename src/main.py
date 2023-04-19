import logging
import socket
from multiprocessing.pool import ThreadPool
from dotenv import load_dotenv
from os import environ
from client import Client

def main():

    load_dotenv()

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    ip_address = environ.get('BIND_ADDRESS')
    port = environ.get('BIND_PORT')

    sock.bind((ip_address, port))

    sock.listen()

    threads_pool = ThreadPool()

    while(True):
        client_socket, ip_ep = sock.accept()
        logging.info("client {}:{} connected".format(ip_ep[0], ip_ep[1]))
        client = Client(client_socket, ip_ep=ip_ep)
        client.start_connection_processing()

if __name__ == "__main__":
    main()
