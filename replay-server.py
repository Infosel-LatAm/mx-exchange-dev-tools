#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para responder a solicitudes de retransmisión estilo BMV
"""


import socket
from time import time

# Constants
HEADER_SIZE = 17
LENGTH_SIZE = 2


def main_loop():
    """Waits for connections and returns replay requests"""
    while True:

        # Wait for a connection
        print('Esperando por una conexión...')
        connection, client_address = sock.accept()

        try:
            print(f'Conexión de cliente desde: {str(client_address[0])}')

            # Receive the data in small chunks and retransmit it
            while True:
                data = connection.recv(19)
                if data:
                    print('Información recibida analizando...')
                    if data[0] != 19:
                        print(f'El tamaño de los datos no es el esperado [{str(data[0])}] ignoramos al cliente')
                        print('Cerramos la conexión...')
                        connection.close()
                        break

                    if data[1] != 33:
                        print(f'El tipo de mensaje no es el esperado [{str(data[1])}] ignoramos al cliente')
                        print('Cerramos la conexión...')
                        connection.close()
                        break

                    if data[2] != 18:
                        print(f'El código de grupo no es el esperado [{str(data[2])}] respondemos al cliente B')

                        connection.sendall(fill_login_response('B'))
                        print('Cerramos la conexión...')
                        connection.close()
                        break

                    print(f'Solicitud de sesion grupo: {data[2]} usuario: {str(data[3:9])}, passw: {str(data[9:19])}')
                    print('Respondemos al cliente A')

                    connection.sendall(fill_login_response('A'))

                    data2 = connection.recv(9)

                    if data2:
                        print('Información recibida nuevamente, analizando...')
                        if data2[0] != 9:
                            print(f'El tamaño de los datos no es el esperado [{str(data2[0])}] ignoramos al cliente')
                            print('Cerramos la conexión...')
                            connection.close()
                            break
                        if data2[1] != 35:
                            print(f'El tipo de mensaje no es el esperado [{str(data2[1])}] ignoramos al cliente')
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        if data2[2] != 18:
                            print(f'El código de grupo no es el esperado [{str(data2[2])}] respondemos al cliente B')
                            connection.sendall(fill_replay_response('B', 0, 0, 0))
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        first_message_array = data2[3:7]
                        first_message = int.from_bytes(first_message_array, 'big')

                        if first_message < 0:
                            print(f'El primer mensaje no es válido [{first_message}] respondemos al cliente J')
                            connection.sendall(fill_replay_response('J', 0, 0, 0))
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        quantity_array = data2[7:9]
                        quantity = int.from_bytes(quantity_array, 'big')

                        if quantity < 0:
                            print(f'La cantidad de mensajes no es válida [{quantity}] respondemos al cliente K')
                            connection.sendall(fill_replay_response('K', 0, 0, 0))
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        print(f'Solicitud de re-transmision, grupo: [{data2[2]}], '
                              f'primera secuencia: {first_message}, cantidad: {quantity}')
                        print('Respondemos al cliente solicitud aceptada A')
                        connection.sendall(fill_replay_response('A', data2[2], first_message, quantity))

                        # Aquí enviamos los paquetes
                        for i in range(first_message, first_message + quantity):
                            print(f'Enviando paquete con secuencia inicial: {i}')
                            connection.sendall(fill_replay_packet(i))
                        print('Cerramos la conexión...')
                        connection.close()
                        break
                else:
                    print(f'No se obtuvieron más datos de: {str(client_address[0])}')
                    break
        except (RuntimeError, TypeError, NameError):
            print('Ha ocurrido un error. Terminamos la aplicación')
            return


def fill_login_response(response_status):
    """Returns the correct response for a login"""
    login_response = bytearray(HEADER_SIZE + LENGTH_SIZE + 2)
    # HEADER
    # Length
    length = HEADER_SIZE + LENGTH_SIZE + 2
    login_response[0:2] = length.to_bytes(2, 'big')
    # Total Messages
    login_response[2] = 1
    # Market Data Group
    login_response[3] = 18
    # Session
    login_response[4] = 2
    # Sequence Number
    sequence_number = 0
    login_response[5:9] = sequence_number.to_bytes(4, 'big')
    # Date-Time
    login_response[9:17] = (int(time()) * 1000 + 123).to_bytes(8, 'big')

    # LENGTH
    # Length message
    login_response[HEADER_SIZE:HEADER_SIZE + LENGTH_SIZE] = (int(2)).to_bytes(2, 'big')

    # MESSAGE
    login_response[HEADER_SIZE + LENGTH_SIZE + 0] = 38  # &
    login_response[HEADER_SIZE + LENGTH_SIZE + 1] = str.encode(response_status, 'iso_8859_1')[0]
    return login_response


def fill_replay_response(replay_status, group, first_message, quantity):
    """Returns a replay response"""
    replay_response = bytearray(HEADER_SIZE + LENGTH_SIZE + 9)
    # HEADER
    # Length
    length = HEADER_SIZE + LENGTH_SIZE + 9
    replay_response[0:2] = length.to_bytes(2, 'big')
    # Total Messages
    replay_response[2] = 1
    # Market Data Group
    replay_response[3] = 18
    # Session
    replay_response[4] = 2
    # Sequence Number
    sequence_number = 0
    replay_response[5:9] = sequence_number.to_bytes(4, 'big')
    # Date-Time
    replay_response[9:17] = (int(time()) * 1000 + 123).to_bytes(8, 'big')

    # LENGTH
    # Length message
    replay_response[HEADER_SIZE:HEADER_SIZE + LENGTH_SIZE] = (int(2)).to_bytes(2, 'big')

    # MESSAGE
    replay_response[HEADER_SIZE + LENGTH_SIZE + 0] = 42  # *
    replay_response[HEADER_SIZE + LENGTH_SIZE + 1] = group
    first_message_array = first_message.to_bytes(4, 'big')
    replay_response[HEADER_SIZE + LENGTH_SIZE + 2:HEADER_SIZE + LENGTH_SIZE + 6] = first_message_array
    quantity_array = quantity.to_bytes(2, 'big')
    replay_response[HEADER_SIZE + LENGTH_SIZE + 6:HEADER_SIZE + LENGTH_SIZE + 8] = quantity_array
    replay_response[HEADER_SIZE + LENGTH_SIZE + 8] = str.encode(replay_status, 'iso_8859_1')[0]
    return replay_response


def fill_replay_packet(sequence):
    f"""Returns the correct replay packet for the given {sequence}"""
    replay_packet = bytearray(HEADER_SIZE + LENGTH_SIZE + 52)
    # HEADER
    # Length
    length = HEADER_SIZE + LENGTH_SIZE + 52
    replay_packet[0:2] = length.to_bytes(2, 'big')
    # Total Messages
    replay_packet[2] = 1
    # Market Data Group
    replay_packet[3] = 18
    # Session
    replay_packet[4] = 2
    # Sequence Number
    sequence_number = sequence
    replay_packet[5:9] = sequence_number.to_bytes(4, 'big')
    # Date-Time
    replay_packet[9:17] = (int(time()) * 1000 + 123).to_bytes(8, 'big')

    # LENGTH
    # Length message
    replay_packet[HEADER_SIZE:HEADER_SIZE + LENGTH_SIZE] = (int(52)).to_bytes(2, 'big')

    # MESSAGE
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 0] = 80  # P Message type
    # Instrument Number
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 1:HEADER_SIZE + LENGTH_SIZE + 5] = (int(12345)).to_bytes(4, 'big')
    # Trade Time
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 5:HEADER_SIZE + LENGTH_SIZE + 13] = (int(time()) * 1000).to_bytes(8, 'big')
    # Volume
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 13:HEADER_SIZE + LENGTH_SIZE + 17] = (int(200)).to_bytes(4, 'big')
    # Price
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 17:HEADER_SIZE + LENGTH_SIZE + 25] = (int(1050000000)).to_bytes(8, 'big')
    # Tipo de concertacion
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 25] = 67  # C
    # Trade Number
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 26:HEADER_SIZE + LENGTH_SIZE + 30] = sequence_number.to_bytes(4, 'big')
    # Price Setter
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 30] = 1
    # Operation type
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 31] = 67  # C
    # Amount
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 32:HEADER_SIZE + LENGTH_SIZE + 40] = (int(210000000000)).to_bytes(8, 'big')
    # Buy
    buy_str = 'GBM  '
    buy_array = str.encode(buy_str, 'iso_8859_1')
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 40:HEADER_SIZE + LENGTH_SIZE + 45] = buy_array
    # Sell
    sell_str = 'HSBC '
    sell_array = str.encode(sell_str, 'iso_8859_1')
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 45:HEADER_SIZE + LENGTH_SIZE + 50] = sell_array
    # Settlement
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 50] = 50  # 2
    # Auction indicator
    replay_packet[HEADER_SIZE + LENGTH_SIZE + 50] = 32  # space
    return replay_packet


# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the port
port = 10000
server_address = ('localhost', port)
print('Escuchando en el puerto: %d' % port)
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)
main_loop()
