import socket
import select
import threading
import datetime
import time
from array import *

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


connected_socket = []
disconnect_socket = []

threadFlg = True


def ingress_ais_sentence():
    host = ['202.129.173.114', '202.129.173.60', '202.129.173.59']
    port = [3334, 8040, 8040]
    ais_key_str = ['enav|Feedt03n@v','NMEA_MDM|NMEA_MDM', 'NMEA_MDM|NMEA_MDM']

    timeout = 30
    targetHost = 0

    while threadFlg:
        aiskey = array('b', [])
        keyLenCnt = 0
        keyHexVal = ''
        
        credential = ais_key_str[targetHost]
        credentials = credential.split('|')

        user =  bytearray(credentials[0], "utf-8")
        pwd =  bytearray(credentials[1], "utf-8")

        aiskey = bytearray([0x0001]) + user + bytearray([0x0000]) + pwd + bytearray([0x0000])        

        # for n in range(int(len(ais_key_str[targetHost]))):
        #     keyHexVal += ais_key_str[targetHost][n:n+1]
        #     keyLenCnt += 1

        #     if keyLenCnt % 2 == 0:
        #         aiskey.append(int(keyHexVal, 16))
        #         keyHexVal = ''


        try:
            data = ''
            defaultTime = datetime.datetime(1900, 1, 1, 00, 00, 00)
            now = datetime.datetime(1900, 1, 1, 00, 00, 00)

            logging.info(f'attempt to server {host[targetHost]} via port {port[targetHost]}...')

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host[targetHost], port[targetHost]))
            s.settimeout(timeout)
            s.setblocking(0)        #set to unblocking socket operation
            s.send(aiskey)

            logging.info('connected...')
            targetHost += 1
            targetHost = 0 if targetHost >= len(host) else targetHost

            multi_package_ais = []

            while threadFlg:
                try:       
                    recvData = s.recv(1)
                    
                    if len(recvData) == 0:
                        raise Exception
                    else:
                        now = defaultTime

                    data += recvData.decode()

                    if recvData.decode() == '\n':
                        if data.strip()[0:1] == '!':
                            ais_sentence = data.strip()

                            ais = ais_sentence.split(',')
                            total_package = int(ais[1])
                            package_no = int(ais[2])
                            package_id = int(ais[3]) if ais[3] else 0

                            multi_package_ais.append(ais_sentence.replace('\n', ''))

                            if len(multi_package_ais) == total_package:
                                # todo :: extend the code here to send to next data processing
                                sentences = "|".join(multi_package_ais)
                                sentences += '\n'

                                if len(connected_socket)>0 :
                                    for i in connected_socket:
                                        try:
                                            i.sendall(sentences.encode('utf-8'))     
                                        except socket.error as er:
                                            connected_socket.remove(i)
                                            disconnect_socket.append(i) 

                                
                                logging.info(multi_package_ais)
                                multi_package_ais.clear()


                        data = ''

                    if s.fileno() == -1:
                        break
                except:
                    if now == defaultTime:
                        now = datetime.datetime.now()

                    dt = datetime.datetime.now() - now
                    
                    if dt.total_seconds() > timeout:
                        break

                    continue
            
            s.close()
            s.detach()
            logging.info('timeout - process exited!')

            time.sleep(1)
        except KeyboardInterrupt:
            s.close()
            s.detach()
            logging.info('user termination!')    

            break
        except:
            s.detach()

            logging.info('network reconnection....')
            time.sleep(3)

            targetHost += 1
            targetHost = 0 if targetHost >= len(host) else targetHost

            continue


def main():
    # Create a socket object
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Get local machine name
    host = socket.gethostname()

    # Reserve a port for your service
    port = 38388

    # Bind to the port
    sc.bind((host, port))

    # Set the socket to non-blocking mode
    sc.setblocking(0)

    # Wait for client connection
    sc.listen(5)

    # List of sockets to be monitored by select
    sockets_to_monitor = [sc]

    logging.info(f"Server {host} is running on port {port} -- v1.0")
    thread_one = threading.Thread(target=ingress_ais_sentence)
    thread_one.start()

    
    while True:
        try:
            for sockcomm in sockets_to_monitor:
                if sockcomm.fileno() < 0:
                    sockcomm.close()
                    sockcomm.detach()
                    sockets_to_monitor.remove(sockcomm)
                    # logging.info(f"This remote IP is closed :: {sockcomm}")
                    
                    continue
 
            readable, _, _ = select.select(sockets_to_monitor, [], [], 5)

            for sock in readable:
                if sock is sc:
                    # A new client connection is ready to be accepted
                    c, addr = sc.accept()
                    logging.info('Got connection from', addr)
                    sockets_to_monitor.append(c)
                    connected_socket.append(c)
                else:
                    try:
                        # An existing client sent data or closed the connection
                        data = sock.recv(256)
                        logging.info("data::" + data.decode("utf-8"))

                        if data:
                            sock.send(data)

                        else:
                            sock.close()
                            sock.detach()
                            sockets_to_monitor.remove(sock)
                            connected_socket.remove(sock)
                    except:
                            sock.close()
                            sock.detach()
                            sockets_to_monitor.remove(sock)
                            connected_socket.remove(sock)                    


            if len(disconnect_socket) > 0:
                for i in disconnect_socket:
                    disconnect_socket.remove(i)
                    i.close()
                    i.detach()


        except socket.error as e:
            continue

        except KeyboardInterrupt:
            threadFlg = False

            if len(connected_socket) > 0:
                for i in connected_socket:
                    connected_socket.remove(i)
                    i.close()
                    i.detach()

            thread_one.join()

            connected_socket.clear()
            disconnect_socket.clear()
            sc.close()
            sc.detach()


if __name__ == '__main__':
    main()