#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Listens to multicast group for PRODUCTO 18.
If something comes, prints the summary of the information.
"""

import socket
import bmv_utils.parse


if __name__ == '__main__':
    counter_msgs = {}
    # Set up a UDP server
    UDP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        UDP_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except AttributeError:
        pass
    UDP_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

    UDP_sock.bind((bmv_utils.parse.UDP_PRODUCTO_18, 12121))  # The port is arbitrary?, so I don't define it as constant
    host = socket.gethostbyname(socket.gethostname())
    UDP_sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(host))
    UDP_sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(bmv_utils.parse.UDP_PRODUCTO_18) + socket.inet_aton(host))

    try:
        udp_packet, addr = UDP_sock.recvfrom(1024)
    except UDP_sock.error as e:
        print('Exception')
    else:
        secuencia, total_mensajes, counter_msgs, paquete = \
            bmv_utils.parse.parse_bmv_udp_packet(udp_packet, counter_msgs)
        print(f"El PRODUCTO 18 esta en la secuencia {secuencia} y "
              f"con el timestamp {paquete['timestamp']}")

# Paquete de prueba
#
# udp_packet = b"\x00G\x01\x12\x02\x002\x0eW\x00\x00\x01\x83\xf0\x8c\xb0\r\x004P\x00\x00\x07)\x00\x00\x01\x83\xf0\x8c\xae\x18\x00\x00\x02\xb1\x00\x00\x00\x01\x82[x\x80O\x00\x00\x04\x161C\x00\x00\x04\x0f\xd8/P\x80GBM  CITI 4 "
# El PRODUCTO 18 esta en la secuencia 3280471 y con el timestamp 2022-10-19T17:01:35.501000