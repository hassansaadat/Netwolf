import errno
import os
import socket
import threading
from threading import Timer
import time
from time import sleep
import argparse
import sys
import logging
import tqdm

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

DEFAULT_NODE_NAME = 'N1'
DEFAULT_UDP_PORT = 8000
DEFAULT_DIRECTORY = './n1'
DEFAULT_CLUSTER_FILE = 'f'
DEFAULT_DISCOVERY_INTERVAL = 2
DEFAULT_MAX_WAITING_TIME = 5  # default max waiting time to get response
DEFAULT_MAX_SERVICING_NODES = 5  # default max servicing nodes simultaneously
DEFAULT_HOST_IP = '127.0.0.1'


MESSAGE_LENGTH_SIZE = 1024
ENCODING = 'utf-8'
DEFAULT_START_TIME = 10000000
BUFFER_SIZE = 1024
MANUAL_DELAY_TO_RESPOND = 0.01


class Node:
    cluster_list = {}
    udp_server = None
    tcp_server = None
    tcp_client = None
    tcp_port = None

    DSC_SEND = True
    DSC_FETCH = True
    TCP_SERVER = True
    CMD = True

    # my = {('N1', '127.0.0.1', 8000): [200, 212]}
    delay_stamp = {}
    start_time = DEFAULT_START_TIME
    getting_file = None
    prior_communications = []
    servicing_nodes = 0
    host_ip = socket.gethostbyname(socket.gethostname())


    def __init__(self, name, udp_port, directory, cluster_file, disc_inter, mwt, msn, is_gateway):
        self.name = name if name else DEFAULT_NODE_NAME
        self.udp_port = udp_port if udp_port is not None else DEFAULT_UDP_PORT
        self.disc_inter = disc_inter if disc_inter is not None else DEFAULT_DISCOVERY_INTERVAL
        self.max_waiting_time = mwt if mwt is not None else DEFAULT_MAX_WAITING_TIME
        self.max_servicing_nodes = msn if msn is not None else DEFAULT_MAX_SERVICING_NODES
        self.is_gateway_node = is_gateway
        self.hostname = DEFAULT_HOST_IP
        self.host_info = (self.hostname, self.udp_port) if not is_gateway else (self.host_ip, self.udp_port)
        # print(disc_delay, max_waiting_time, max_servicing_nodes)

        if directory:
            self.directory = directory
        else:
            self.directory = DEFAULT_DIRECTORY
            if not os.path.exists(self.directory):
                os.mkdir(DEFAULT_DIRECTORY)
        if cluster_file:
            self.cluster_file = cluster_file
        else:
            self.cluster_file = DEFAULT_CLUSTER_FILE
            if not os.path.exists(self.cluster_file):
                with open(self.cluster_file, 'w') as f:
                    f.write('')

        with open(self.cluster_file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line != '':
                    line = line.strip().split(' ')
                    if len(line) == 3:
                        name, ip, port = line[0], line[1], int(line[2])
                        self.cluster_list[name] = (ip, port)
        self.lock = threading.Lock()



    def discovery_message_serializer(self, clist):
        msg = ''
        for k, v in clist.items():
            # k, v = name, (ip, port)
            msg += f'{k},{v[0]},{v[1]}|'
        msg = msg[:-1]
        # msg = msg.encode(ENCODING)
        return msg


    def discovery_message_deserializer(self, msg=''):
        lines = msg.split('|')
        lst = {}
        for line in lines:
            arr = line.strip().split(',')
            name, ip, port = arr[0], arr[1], int(arr[2])
            lst[name] = (ip, port)
        # logging.debug(lst)
        return lst


    def discovery_sender(self):
        # print('sender')
        while self.DSC_SEND:
            sleep(self.disc_inter)
            if len(self.cluster_list) == 0:
                continue
            sending_list = {**self.cluster_list, **{self.name: self.host_info}}
            # logging.debug(sending_list)
            serialized_list = self.discovery_message_serializer(sending_list)
            for name, host_info in self.cluster_list.items():
                try:
                    cmd = 'DISCOVERY'
                    if self.is_gateway_node:
                        if host_info[0] == DEFAULT_HOST_IP:
                            cmd = 'GATEWAY_DISCOVERY'
                            for name_, host_info_ in sending_list.items():
                                # change local address to proxy address
                                if host_info_[0] != DEFAULT_HOST_IP:
                                    sending_list[name_] = (DEFAULT_HOST_IP, self.udp_port)
                            serialized_list = self.discovery_message_serializer(sending_list)
                        elif host_info[0] != DEFAULT_HOST_IP:
                            cmd = 'BORDER_DISCOVERY'
                    else:
                        pass
                        # if host_info[0] == DEFAULT_HOST_IP:
                        #     cmd = 'DISCOVERY'
                        # else:
                            # logging.debug('unpredicted discovery message')
                    msg = f'{cmd}:{self.name}:{name}:{serialized_list}'
                    self.udp_server.sendto(msg.encode(ENCODING), host_info)
                except socket.error as e:
                    pass

    def file_writer(self, added_items):
        # writing in file
        # logging.debug('writing...')
        with open(self.cluster_file, 'a+') as f:
            for name, host_info in added_items.items():
                f.write(f'\n{name} {host_info[0]} {host_info[1]}')



    def start(self):
        print('[NETWOLF STARTS] starting...')
        self.udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if self.is_gateway_node:
            print(f'public ip: {self.host_ip}')
            self.udp_server.bind(('0.0.0.0', self.udp_port))
            print('listening on {}:{} UDP'.format('0.0.0.0', self.host_info[1]))
        else:
            print('listening on {}:{} UDP'.format(self.host_info[0], self.host_info[1]))
            self.udp_server.bind(self.host_info)

        self.tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_server.bind((self.hostname if not self.is_gateway_node else '0.0.0.0', 0))
        self.tcp_server.listen()
        self.tcp_port = self.tcp_server.getsockname()[1]
        print('listening on {}:{} TCP'.format(self.host_info[0] if not self.is_gateway_node else '0.0.0.0', self.tcp_port))

        tcp_server_thread = threading.Thread(target=self.tcp_server_handler, args=(self.tcp_server, ), name='tcp-server-thread')
        tcp_server_thread.start()

        discovery_sender_thread = threading.Thread(target=self.discovery_sender, args=(), name='sender-thread')
        discovery_sender_thread.start()

        discovery_dispatcher_thread = threading.Thread(target=self.udp_server_handler, args=(), name='dispatcher-thread')
        discovery_dispatcher_thread.start()


        cmd_thread = threading.Thread(target=self.command_handler, args=(), name='cmd-thread')
        cmd_thread.start()

        tcp_server_thread.join()
        discovery_sender_thread.join()
        discovery_dispatcher_thread.join()
        cmd_thread.join()

        print('Good bye!')
        sys.exit(0)


    def get_tcp_client(self):
        if self.tcp_client is None:
            return socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            return self.tcp_client



    def tcp_server_handler(self, server_socket):
        try:
            while self.TCP_SERVER:
                client_socket, address = server_socket.accept()
                client_handler = threading.Thread(target=self.serve_file_handler,
                                                  args=(client_socket, address),
                                                  name='client-handler-thread')
                client_handler.start()
        except:
            pass
        finally:
            self.tcp_server.close()

    def udp_server_handler(self):
        while self.DSC_FETCH:
            try:
                data, address = self.udp_server.recvfrom(MESSAGE_LENGTH_SIZE)
                if data and address != self.host_info:
                    req_arr = data.decode(ENCODING).strip().split(':')
                    # logging.debug(req_arr)
                    cmd, src, dst, lst = req_arr[0], req_arr[1], req_arr[2], req_arr[3]
                    # <cmd>: <src>: <dst>: <clist | filename | nothing>
                    # if packet is mine get and process it, else perform semi-NAT and forward it! :)
                    # print(req_arr)
                    if dst == self.name:
                        if cmd == 'BORDER_DISCOVERY':
                            rlist = self.discovery_message_deserializer(lst)
                            added_nodes = {}
                            for name, host_info in rlist.items():
                                # change local address to proxy address
                                if host_info[0] == DEFAULT_HOST_IP:
                                    rlist[name] = address
                                # when proxy know's this network nodes by it's gateway node(current node)
                                elif host_info[0] == self.host_ip and host_info[1] != self.udp_port:
                                    rlist[name] = (DEFAULT_HOST_IP, host_info[1])
                                # add to added_nodes
                                if name not in self.cluster_list.keys() and name != self.name:
                                    added_nodes[name] = rlist[name]

                            # merge with cluster list
                            self.cluster_list.update(added_nodes)
                            # write in file
                            self.file_writer(added_nodes)
                        elif cmd.__contains__('DISCOVERY'):
                            rlist = self.discovery_message_deserializer(lst)
                            added_nodes = {}
                            for name, host_info in rlist.items():
                                # add to added_nodes
                                if name not in self.cluster_list.keys() and name != self.name:
                                    added_nodes[name] = rlist[name]

                            # merge with cluster list
                            self.cluster_list.update(added_nodes)
                            # write in file
                            self.file_writer(added_nodes)
                        elif cmd == 'GET':
                            # print('get received')
                            file = lst
                            if (file in os.listdir(self.directory)
                                    and self.servicing_nodes < self.max_servicing_nodes
                                    and file != self.getting_file):
                                ack = 'ACK:{}:{}:{}:{}'.format(self.name, src, file, self.tcp_port).encode(ENCODING)
                                if address not in self.prior_communications:
                                    sleep(MANUAL_DELAY_TO_RESPOND)
                                self.udp_server.sendto(ack, address)
                            else:
                                nack = 'NACK:{}:{}:{}'.format(self.name, src, file).encode(ENCODING)
                                self.udp_server.sendto(nack, address)
                        elif cmd == 'GET':
                            # print('get received')
                            file = lst
                            if (file in os.listdir(self.directory)
                                    and self.servicing_nodes < self.max_servicing_nodes
                                    and file != self.getting_file):
                                ack = 'ACK:{}:{}:{}:{}'.format(self.name, src, file, self.tcp_port).encode(ENCODING)
                                if address not in self.prior_communications:
                                    sleep(MANUAL_DELAY_TO_RESPOND)
                                self.udp_server.sendto(ack, address)
                                # print(f'acking {src} request')
                            else:
                                nack = 'NACK:{}:{}:{}'.format(self.name, src, file).encode(ENCODING)
                                self.udp_server.sendto(nack, address)
                                # print(f'nacking {src} request')
                        elif cmd == 'ACK':
                            now = time.time()
                            if (lst == self.getting_file or  # not when packet receives too late(eg for prior request)
                                    now < self.start_time + self.max_waiting_time):  # not when time's up
                                tcp_port = int(req_arr[-1])
                                self.delay_stamp[src] += [now, tcp_port]
                            else:
                                # packet is for prior GET request
                                pass
                        elif cmd == 'NACK':
                            # print('nacked')
                            if lst == self.getting_file:
                                self.delay_stamp.pop(src)
                            else:
                                # packet is for prior GET request
                                pass
                    else:
                        # if cmd == 'GET':
                        #     print(f'GET forwarding to {dst}')
                        forwarding_data = data
                        if cmd == 'DISCOVERY':
                            rlist = self.discovery_message_deserializer(lst)
                            for name, host_info in rlist.items():
                                # change sending discovery messages from internal nodes to out of network
                                # by setting self ip as gateway
                                if host_info[0] == DEFAULT_HOST_IP:
                                    rlist[name] = (self.host_ip, self.udp_port)
                            data = f'BORDER_DISCOVERY:{src}:{dst}:{self.discovery_message_serializer(rlist)}'
                            forwarding_data = data.encode(ENCODING)
                        elif cmd == 'BORDER_DISCOVERY':
                            rlist = self.discovery_message_deserializer(lst)
                            for name, host_info in rlist.items():
                                # change sending discovery messages from external nodes to internal network
                                # by setting self ip as gateway
                                my_list = {**self.cluster_list, **{self.name: (DEFAULT_HOST_IP, self.udp_port)}}
                                if host_info[0] == DEFAULT_HOST_IP and (name in my_list.keys() or name == self.name):
                                    rlist[name] = my_list[name]
                                elif host_info[0] != DEFAULT_HOST_IP:
                                    rlist[name] = (DEFAULT_HOST_IP, self.udp_port)
                            data = f'GATEWAY_DISCOVERY:{src}:{dst}:{self.discovery_message_serializer(rlist)}'
                            forwarding_data = data.encode(ENCODING)
                        elif cmd == 'ACK':
                            # step 1 connect vi a tcp socket to source as a downloader client
                            # step 2 create tcp server file serving thread with random port
                            # step 3 send ack to next gateway node with tcp port=step 2 server port
                            # step 4 download from src and upload to dst simultaneously

                            # print(f'forwarding ack {forwarding_data.decode(ENCODING)} to {dst}')

                            proxy_tcp_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            proxy_tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            proxy_tcp_server.bind(('0.0.0.0', 0))
                            proxy_tcp_server.listen()
                            proxy_tcp_port = int(proxy_tcp_server.getsockname()[1])
                            # print(proxy_tcp_port)
                            sender_host_tcp_info = (address[0], int(req_arr[4]))
                            # print(sender_host_tcp_info)
                            file_name = req_arr[3]
                            new_ack = 'ACK:{}:{}:{}:{}'.format(src,
                                                               dst, file_name, proxy_tcp_port).encode(ENCODING)
                            # print(new_ack)
                            forwarding_data = new_ack
                            #
                            proxy_server_thread = threading.Thread(target=self.proxy_server_handler,
                                                                   args=(proxy_tcp_client,
                                                                         proxy_tcp_server,
                                                                         sender_host_tcp_info
                                                                         ),
                                                                   name='proxy-server-thread')
                            proxy_server_thread.start()
                            # proxy_server_thread.join()

                        # now forward
                        # print(f'forwarding {forwarding_data.decode(ENCODING)} to {dst}')
                        if dst in self.cluster_list.keys():
                            address = self.cluster_list[dst]
                            try:
                                self.udp_server.sendto(forwarding_data, address)
                            except socket.error as e:
                                pass

            except socket.error as e:
                err = e.args[0]
                if err == errno.ECONNRESET:
                    sleep(1)
                    continue
                # else:
                # a "real" error occurred
                # logging.debug(e.__str__())


    def proxy_server_handler(self, client_socket, server_socket, main_server_host_info):
        # logging.debug('proxy handler starting...')
        next_hop_client_socket, address = server_socket.accept()
        client_socket.connect(main_server_host_info)
        file_name = next_hop_client_socket.recv(BUFFER_SIZE)
        client_socket.send(file_name)
        response = client_socket.recv(BUFFER_SIZE)
        next_hop_client_socket.send(response)

        res = response.decode(ENCODING).strip()
        if res == 'NOT FOUND':
            next_hop_client_socket.shutdown(socket.SHUT_WR)
            next_hop_client_socket.close()
            # server_socket.shutdown(socket.SHUT_WR)
            return

        try:
            bytes_read = client_socket.recv(BUFFER_SIZE)
            while bytes_read:
                next_hop_client_socket.send(bytes_read)
                bytes_read = client_socket.recv(BUFFER_SIZE)
        except socket.error as e:
            print(e.__str__())
        finally:
            next_hop_client_socket.shutdown(socket.SHUT_WR)
            # print('finish proxy work!')
        next_hop_client_socket.close()


    def serve_file_handler(self, server, address):
        # print(f'sending to {address}')
        file_name = server.recv(BUFFER_SIZE).decode(ENCODING).strip()
        if file_name not in os.listdir(self.directory):
            msg = 'NOT FOUND'.encode(ENCODING)
            msg += b' ' * (BUFFER_SIZE - len(msg))
            server.send(msg)
        else:
            # critical section in increasing servicing_nodes counter
            self.lock.acquire()
            self.servicing_nodes += 1
            self.lock.release()

            path = os.path.join(self.directory, file_name)
            filesize = os.path.getsize(path)
            msg = f'{filesize}'.encode(ENCODING)
            msg += b' ' * (BUFFER_SIZE - len(msg))
            server.send(msg)

            # to see uploading progress uncomment this line and below lines which has 'progress' in it
            #FIXME progress = tqdm.tqdm(range(filesize), f"Sending {file_name}", unit="B", unit_scale=True, unit_divisor=1024)
            file = None
            try:
                file = open(path, 'rb')
                bytes_read = file.read(BUFFER_SIZE)
                while bytes_read:
                    server.send(bytes_read)
                    # update the progress bar
                    #FIXME progress.update(len(bytes_read))
                    bytes_read = file.read(BUFFER_SIZE)
                #FIXME progress.close()
            except Exception as e:
                logging.debug(e.__str__())
            finally:
                server.shutdown(socket.SHUT_WR)
                if file:
                    file.close()
        server.close()

        # critical section in decreasing servicing_nodes counter
        self.lock.acquire()
        self.servicing_nodes -= 1
        self.lock.release()


    def peer_selector(self):
        if len(self.delay_stamp) == 0:
            self.getting_file = None
            print('file not found! try again')
        else:
            min = DEFAULT_START_TIME  # only a large number needed such this! :)
            selected = None
            # logging.debug(self.delay_stamp)
            # dict[key] = [t1, t2, tcp_port]
            # key = ('127.0.0.1', 8000)
            for k, v in self.delay_stamp.items():
                if len(v) == 3:
                    delay = v[1] - v[0]
                    if delay < min:
                        min = delay
                        selected = k
            if not selected:
                self.getting_file = None
                print('file not found! try again')
            else:
                # tcp request for file
                print('Getting {} from node {}'.format(self.getting_file, selected))
                selected_tcp_port = self.delay_stamp[selected][2]
                host_tcp_info = (self.cluster_list[selected][0], selected_tcp_port)
                host_udp_info = self.cluster_list[selected]
                dhandler_thread = threading.Thread(target=self.download_handler,
                                                   args=(host_tcp_info, host_udp_info),
                                                   name='dhandler-thread')
                dhandler_thread.start()
        self.delay_stamp = {}
        self.start_time = DEFAULT_START_TIME


    def download_handler(self, host_tcp_info, host_udp_info):
        # print(f'downloading from {host_tcp_info}')
        client = self.get_tcp_client()
        client.connect(host_tcp_info) # ('127.0.0.1', 5000)
        # receive the file info
        # receive using client socket, not server socket
        file_name = self.getting_file
        msg = f"{file_name}".encode(ENCODING)
        msg += b' ' * (BUFFER_SIZE - len(msg))
        client.send(msg)
        response = client.recv(BUFFER_SIZE).decode(ENCODING).strip()
        if response == 'NOT FOUND':
            return
        # add to prior communications
        self.prior_communications.append(host_udp_info)
        file_size = int(response)
        progress = tqdm.tqdm(range(file_size), f"Receiving {file_name}", unit="B", unit_scale=True, unit_divisor=1024)

        # /home/users/file.txt
        path = os.path.join(self.directory, file_name)
        file = None
        try:
            file = open(path, "wb")
            bytes_read = client.recv(BUFFER_SIZE)
            while bytes_read:
                file.write(bytes_read)
                # update the progress bar
                progress.update(len(bytes_read))
                bytes_read = client.recv(BUFFER_SIZE)
            progress.close()
        except Exception as e:
            logging.debug(e.__str__())
        finally:
            if file:
                file.close()

        self.getting_file = None


    def command_handler(self):
        print('command line service starting...')
        while self.CMD:
            if self.getting_file:
                print('> waiting for response...')
            while self.getting_file:
                sleep(2)
            cmd = input('>')
            if cmd:
                if cmd == 'list':
                    for k, v in self.cluster_list.items():
                        print(k, v)
                elif len(cmd.split(' ')) == 2 and cmd.split(' ')[0] == 'get':
                    file = cmd.split(' ')[1]
                    dirs = os.listdir(self.directory)
                    if file not in dirs:
                        self.getting_file = file
                        self.start_time = time.time()
                        for name, host_info in self.cluster_list.items():
                            msg = f'GET:{self.name}:{name}:{file}'.encode(ENCODING)
                            self.delay_stamp[name] = [self.start_time]
                            try:
                                self.udp_server.sendto(msg, host_info)
                            except socket.error as e:
                                pass
                        t = Timer(self.max_waiting_time, self.peer_selector)
                        t.start()

                    else:
                        print('file already exists!')

                elif cmd == 'left':
                    print('Shutting down services...')
                    self.DSC_SEND = False
                    self.DSC_FETCH = False
                    self.TCP_SERVER = False
                    self.CMD = False
                    self.tcp_server.close()
                else:
                    print('command not found!')


def is_port_using(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    result = True
    try:
        sock.bind(("0.0.0.0", port))
        result = False
    except:
        pass
    finally:
        sock.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name')  # node name
    parser.add_argument('-l', '--list')  # cluster list filename
    parser.add_argument('-p', '--port')  # udp port
    parser.add_argument('-d', '--directory')  # serving directory
    parser.add_argument('-i', '--discovery_interval')  # discovery time interval
    parser.add_argument('-mwt', '--max_waiting_time')  # max waiting time to see other nodes response for file request
    parser.add_argument('-msn', '--max_servicing_nodes')
    parser.add_argument('-g', '--gateway', action='store_true')  # default is false
    args = parser.parse_args()

    name = args.name
    listfile = args.list
    port = args.port
    directory = args.directory
    disc_inter = args.discovery_interval
    mwt = args.max_waiting_time
    msn = args.max_servicing_nodes
    is_gateway = args.gateway

    # checking validity
    if port:
        if not port.isnumeric():
            print('invalid udp port')
            sys.exit(-1)
        port = int(port)
        if port <= 1024:
            print('ports reserved area not allowed!')
            sys.exit(-1)
        if is_port_using(port):
            print('port is in use!')
            sys.exit(-1)

    if directory:
        if not os.path.exists(directory):
            print('directory does not exist!')
            sys.exit(-1)
    if listfile:
        # print(listfile)
        if not os.path.exists(listfile):
            print('list file does not exist')
            sys.exit(-1)
    if disc_inter:
        if not disc_inter.isnumeric():
            print('invalid discovery delay!')
            sys.exit(-1)
        disc_inter = int(disc_inter)

    if mwt:
        if not mwt.isnumeric():
            print('invalid max waiting time(mwt) value!')
            sys.exit(-1)
        mwt = int(mwt)

    if msn:
        if not msn.isnumeric():
            print('invalid max servicing nodes(msn) value!')
            sys.exit(-1)
        msn = int(msn)


    # print(args)

    node = Node(name, port, directory, listfile, disc_inter, mwt, msn, is_gateway)
    node.start()


if __name__ == '__main__':
    main()
