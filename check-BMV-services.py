#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Checks multicast groups for PRODUCTO 18 and if something is coming, prints the summary of the information.
"""

import socket
import bmv_utils.parse

# Set up a UDP server
UDPSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
listen_addr = ("", 6501)
UDPSock.bind(listen_addr)

if __name__ == '__main__':
    counter_msgs = {}
    udp_packet, addr = UDPSock.recvfrom(1024)
    secuencia, total_mensajes, counter_msgs, paquete = bmv_utils.parse.parse_bmv_udp_packet(udp_packet, counter_msgs)
    print(f"El PRODUCTO 18 esta en la secuencia {secuencia} y "
          f"con el timestamp {paquete['timestamp']}")

# Paquete de prueba
#
# udp_packet = b"\x00G\x01\x12\x02\x002\x0eW\x00\x00\x01\x83\xf0\x8c\xb0\r\x004P\x00\x00\x07)\x00\x00\x01\x83\xf0\x8c\xae\x18\x00\x00\x02\xb1\x00\x00\x00\x01\x82[x\x80O\x00\x00\x04\x161C\x00\x00\x04\x0f\xd8/P\x80GBM  CITI 4 "
# El PRODUCTO 18 esta en la secuencia 3280471 y con el timestamp 2022-10-19T17:01:35.501000