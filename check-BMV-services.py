#! /usr/bin/env python
"""
Listens to multicast group for PRODUCTO 18.
If something comes, prints the summary of the information.
"""

import socket
import bmv_utils.parse


# Producto 18 PROD
BMV_PROD18_PROD_GROUP_A = "239.100.100.18"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD18_PROD_PORT_A = 12121
BMV_PROD18_PROD_GROUP_B = "239.100.200.18"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD18_PROD_PORT_B = 12122

# Producto 18 DRP
BMV_PROD18_DRP_GROUP_A = "239.150.100.18"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD18_DRP_PORT_A = 12131
BMV_PROD18_DRP_GROUP_B = "239.150.200.18"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD18_DRP_PORT_B = 12132

# Producto 18 TEST
BMV_PROD18_DRP_GROUP_A = "239.200.100.18"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD18_DRP_PORT_A = 12141
BMV_PROD18_DRP_GROUP_B = "239.200.200.18"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD18_DRP_PORT_B = 12142

# Producto 40 PROD
BMV_PROD40_PROD_GROUP_A = "239.100.100.40"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD40_PROD_PORT_A = 12121
BMV_PROD40_PROD_GROUP_B = "239.100.200.40"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD40_PROD_PORT_B = 12122

# Producto 40 DRP
BMV_PROD40_DRP_GROUP_A = "239.150.100.40"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD40_DRP_PORT_A = 12131
BMV_PROD40_DRP_GROUP_B = "239.150.200.40"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD40_DRP_PORT_B = 12132

# Producto 40 TEST
BMV_PROD40_TEST_GROUP_A = "239.200.100.40"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD40_TEST_PORT_A = 12141
BMV_PROD40_TEST_GROUP_B = "239.200.200.40"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD40_TEST_PORT_B = 12142

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
                        socket.inet_aton(group) + socket.inet_aton(host))  # group was bmv_utils.parse.BMV_PROD18_GRP_A
    return UDP_sock


def check_BMV_producto(group, port, label):
    """
    Subscribe to the producto {label} of BMV, or supposes is a BMV product and attempts to find
    the secuencia and timestamp
    :param group: multicast group to bind to
    :param port: multicast port to bind to
    :param label: the label for the product
    """
    global counter_msgs  # This variable does not really matter.
    UDP_sock_prod = setup_UDP_server(group, port)
    try:
        udp_packet, addr = UDP_sock_prod.recvfrom(1024)
    except UDP_sock_prod.error as e:
        print('Exception')
    else:
        paquete = bmv_utils.parse.parse_bmv_udp_packet(udp_packet)
        print(f"{paquete['timestamp']} - sq {paquete['secuencia']} "
              f"{label} {group}:{port} ")


if __name__ == '__main__':
    counter_msgs = {}
    print("PROD")
    print("PRODUCTO 18")
    check_BMV_producto(BMV_PROD18_PROD_GROUP_A, BMV_PROD18_PROD_PORT_A, "Puerto A")
    check_BMV_producto(BMV_PROD18_PROD_GROUP_B, BMV_PROD18_PROD_PORT_B, "Puerto B")
    print("PRODUCTO 40")
    check_BMV_producto(BMV_PROD40_PROD_GROUP_A, BMV_PROD40_PROD_PORT_A, "Puerto A")
    check_BMV_producto(BMV_PROD40_PROD_GROUP_B, BMV_PROD40_PROD_PORT_B, "Puerto B")
    print("DRP")
    print("PRODUCTO 18")
    check_BMV_producto(BMV_PROD18_DRP_GROUP_A, BMV_PROD18_DRP_PORT_A, "Puerto A")
    check_BMV_producto(BMV_PROD18_DRP_GROUP_B, BMV_PROD18_DRP_PORT_B, "Puerto B")
    print("PRODUCTO 40")
    check_BMV_producto(BMV_PROD40_DRP_GROUP_A, BMV_PROD40_DRP_PORT_A, "Puerto A")
    check_BMV_producto(BMV_PROD40_DRP_GROUP_B, BMV_PROD40_DRP_PORT_B, "Puerto B")
    print("TEST")
    print("PRODUCTO 18")
    check_BMV_producto(BMV_PROD18_DRP_GROUP_A, BMV_PROD18_DRP_PORT_A, "Puerto A")
    check_BMV_producto(BMV_PROD18_DRP_GROUP_B, BMV_PROD18_DRP_PORT_B, "Puerto B")
    print("PRODUCTO 40")
    check_BMV_producto(BMV_PROD40_TEST_GROUP_A, BMV_PROD40_TEST_PORT_A, "Puerto A")
    check_BMV_producto(BMV_PROD40_TEST_GROUP_B, BMV_PROD40_TEST_PORT_B, "Puerto B")
    


# Paquete de prueba
#
# udp_packet = b"\x00G\x01\x12\x02\x002\x0eW\x00\x00\x01\x83\xf0\x8c\xb0\r\x004P\x00\x00\x07)\x00\x00\x01\x83\xf0\x8c\xae\x18\x00\x00\x02\xb1\x00\x00\x00\x01\x82[x\x80O\x00\x00\x04\x161C\x00\x00\x04\x0f\xd8/P\x80GBM  CITI 4 "
# El PRODUCTO 18 esta en la secuencia 3280471 y con el timestamp 2022-10-19T17:01:35.501000
