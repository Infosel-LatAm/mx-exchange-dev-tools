#! /usr/bin/env python
"""
Listens to multicast group for PRODUCTO 18.
If something comes, prints the summary of the information.
"""

import socket
import bmv_utils.parse


def setup_UDP_server(group, port):
    """
    Sets up a udp socket to receive packets on group and port
    :param group: multicast group to bind to
    :param port: multicast port to bind to
    :return: udp_socket
    """
    # Set up a UDP server
    UDP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        UDP_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except AttributeError:
        pass
    UDP_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    UDP_sock.bind((group, port))
    host = socket.gethostbyname(socket.gethostname())
    UDP_sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(host))
    UDP_sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(bmv_utils.parse.BMV_PROD18_GRP_A) + socket.inet_aton(host))
    return UDP_sock


def check_BMV_producto18(group, port):
    """
    Subscribe to the producto 18 of BMV, or supposes is product 18 and attempts to find
    the secuencia and timestamp
    :param group: multicast group to bind to
    :param port: multicast port to bind to
    """
    global counter_msgs  # This variable does not really matter.
    UDP_sock_prod18_A = setup_UDP_server(group, port)
    try:
        udp_packet, addr = UDP_sock_prod18_A.recvfrom(1024)
    except UDP_sock_prod18_A.error as e:
        print('Exception')
    else:
        secuencia, total_mensajes, counter_msgs, paquete = \
            bmv_utils.parse.parse_bmv_udp_packet(udp_packet, counter_msgs)
        print(f"PRODUCTO 18 en {group}:{port} secuencia {secuencia}  "
              f"timestamp {paquete['timestamp']}")


if __name__ == '__main__':
    counter_msgs = {}
    check_BMV_producto18(bmv_utils.parse.BMV_PROD18_GRP_A,
                         bmv_utils.parse.BMV_PROD18_PORT_A)
    check_BMV_producto18(bmv_utils.parse.BMV_PROD18_GRP_B,
                         bmv_utils.parse.BMV_PROD18_PORT_B)


# Paquete de prueba
#
# udp_packet = b"\x00G\x01\x12\x02\x002\x0eW\x00\x00\x01\x83\xf0\x8c\xb0\r\x004P\x00\x00\x07)\x00\x00\x01\x83\xf0\x8c\xae\x18\x00\x00\x02\xb1\x00\x00\x00\x01\x82[x\x80O\x00\x00\x04\x161C\x00\x00\x04\x0f\xd8/P\x80GBM  CITI 4 "
# El PRODUCTO 18 esta en la secuencia 3280471 y con el timestamp 2022-10-19T17:01:35.501000
