#! /usr/bin/env python
"""
Script para simular solicitudes de retransmisión estilo BMV
"""
import socket


def fill_login_structure():
    """Returns a byte array with the structure for a login"""
    login_structure = bytearray(19)

    # Longitud
    login_structure[0] = 19
    # Tipo Mensaje
    login_structure[1] = 33  # !
    # Grupo Marketdata
    login_structure[2] = 18
    # Usuario
    usuario_str = 'INFS01'
    usuario_array = str.encode(usuario_str, 'iso_8859_1')
    login_structure[3:9] = usuario_array
    # Password
    password_str = '1234567890'
    password_array = str.encode(password_str, 'iso_8859_1')
    login_structure[9:20] = password_array
    return login_structure


def fill_replay_structure():
    """Return the structure of a replay with the correct fields set."""
    replay_structure = bytearray(9)
    # Longitud
    replay_structure[0] = 9
    # Tipo Mensaje
    replay_structure[1] = 35
    # Grupo Marketdata
    replay_structure[2] = 18
    # Primer Mensaje
    primer_mensaje = 123
    primer_mensaje_array = primer_mensaje.to_bytes(4, 'big')
    replay_structure[3:7] = primer_mensaje_array
    # Cantidad
    cantidad = 5
    cantidad_array = cantidad.to_bytes(2, 'big')
    replay_structure[7:9] = cantidad_array
    return replay_structure


HEADER_SIZE = 17
LENGTH_SIZE = 2

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server listens.
port = 10000
server_address = ('localhost', port)
print('connecting to ' + server_address[0] + ' port ' + str(port))
sock.connect(server_address)


def process_received_info():
    """While possible, read from the socket and process"""
    while True:
        data3 = sock.recv(HEADER_SIZE)  # Intentamos leer el encabezado
        if data3:
            if len(data3) == HEADER_SIZE:
                length = int.from_bytes(data3[0:2], 'big')
                total_messages = int(data3[2])
                sequence = int.from_bytes(data3[5:9], 'big')
                print(f'Se recibe encabezado...'
                      f'Longitud: {length}, Mensajes: {total_messages}, '
                      f'Secuencia inicial {sequence}')
                data4 = sock.recv(length - HEADER_SIZE)  # Intentamos el contenido del paquete
                if data4:
                    if len(data4) == length - HEADER_SIZE:
                        print('Recibimos el contenido del paquete')
                        continue
                    else:
                        print('Recibimos el contenido del paquete con tamaño erróneo')
                        break
                else:
                    print('No pudo obtener el contenido del paquete')
                    break
        else:
            print('No se obtuvieron más datos')
            break


try:
    # Send data
    print('Enviando solicitud de login...')

    sock.sendall(fill_login_structure())

    data = sock.recv(HEADER_SIZE + LENGTH_SIZE + 2)
    if len(data) == HEADER_SIZE + LENGTH_SIZE + 2:
        print(f"Respuesta recibida, mensaje: '{str(data[HEADER_SIZE + LENGTH_SIZE + 0])}'  "
              f"respuesta: '{str(data[HEADER_SIZE + LENGTH_SIZE + 1])}'")
        if data[HEADER_SIZE + LENGTH_SIZE + 1] == 65:  # A

            sock.sendall(fill_replay_structure())
            data2 = sock.recv(HEADER_SIZE + LENGTH_SIZE + 9)
            if len(data2) == HEADER_SIZE + LENGTH_SIZE + 9:
                if data2[HEADER_SIZE + LENGTH_SIZE + 0] == 42:  # *
                    if data2[HEADER_SIZE + LENGTH_SIZE + 8] == 65:  # A
                        print(f'El Status de la respuesta es el esperado [{str(data2[8])}], leemos los paquetes')
                        process_received_info()
                    else:
                        print(f'El Status de la respuesta a la solicitud de Replay no es una A [{str(data2[8])}]')
                else:
                    print(f'El tipo de mensaje no es el esperado [{str(data2[0])}]')
            else:
                print(f'El tamaño de los datos no es el esperado [{str(data2[0])}]')
        else:
            print(f'El status de la respuesta a la solicitud de Login no fue una A [{str(data[1])}]')
    else:
        print('Respuesta recibida, longitud diferente')

finally:
    print('Cerrando Socket...')
    sock.close()
