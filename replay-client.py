# -*- coding: utf-8 -*-
"""
Script para solicitar
retransmisión estilo BMV

2022-10-28
"""

import socket
import sys

loginStructure = bytearray(19)
replayStructure = bytearray(9)

def fill_login_structure():
    # Longitud
    loginStructure[0] = 19
    # Tipo Mensaje
    loginStructure[1] = 33 # !
    # Grupo Marketdata
    loginStructure[2] = 18
    # Usuario
    usuarioStr = 'INFS01'
    usuarioArray = str.encode(usuarioStr, 'iso_8859_1')
    loginStructure[3:9] = usuarioArray
    # Password
    passwordStr = '1234567890'
    passwordArray = str.encode(passwordStr, 'iso_8859_1')
    loginStructure[9:20] = passwordArray
    return

def fill_replay_structure():
    # Longitud
    replayStructure[0] = 9
    # Tipo Mensaje
    replayStructure[1] = 35 # #
    # Grupo Marketdata
    replayStructure[2] = 18
    # Primer Mensaje
    primerMensaje = 123
    primerMensajeArray = primerMensaje.to_bytes(4, 'big')
    replayStructure[3:7] = primerMensajeArray
    # Cantidad
    cantidad = 5
    cantidadArray = cantidad.to_bytes(2, 'big')
    replayStructure[7:9] = cantidadArray
    return

headerSize = 17
lengthSize = 2

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server is listening
port = 10000
server_address = ('localhost', port)
print('connecting to ' + server_address[0] + ' port ' + str(port))
sock.connect(server_address)

try:

    # Send data
    print('Enviando solicitud de login...')
    fill_login_structure()
    sock.sendall(loginStructure)

    data = sock.recv(headerSize+lengthSize+2)
    if len(data) == headerSize+lengthSize+2:
        print('Respuesta recibida, mensaje: ' + str(data[headerSize+lengthSize+0]) + ' respuesta: ' + str(data[headerSize+lengthSize+1]))
        if data[headerSize+lengthSize + 1] == 65:  # A
            fill_replay_structure()
            sock.sendall(replayStructure)
            data2 = sock.recv(headerSize+lengthSize+9)
            if len(data2) == headerSize+lengthSize+9:
                if data2[headerSize+lengthSize+0] == 42:  # *
                    if data2[headerSize+lengthSize+8] == 65:  # A
                        print('El Status de la respuesta es el esperado [' + str(data2[8]) + '], leemos los paquetes')
                        while True:
                            data3 = sock.recv(headerSize) # Intentamos leer el encabezado
                            if data3:
                                if len(data3) == headerSize:
                                    length = int.from_bytes(data3[0:2], 'big')
                                    totalMessages = int(data3[2])
                                    sequence = int.from_bytes(data3[5:9], 'big')
                                    print('Se recibe encabezado...Longitud: %d, Mensajes: %d, Secuencia inicial %d' % (length, totalMessages, sequence))
                                    data4 = sock.recv(length - headerSize)  # Intentamos el contenido del paquete
                                    if data4:
                                        if len(data4) == length - headerSize:
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
                    else:
                        print('El Status de la respuesta a la solicitud de Replay no es una A [' + str(data2[8]) + ']')
                else:
                    print('El tipo de mensaje no es el esperado [' + str(data2[0]) + ']')
            else:
                print('El tamaño de los datos no es el esperado [' + str(data2[0]) + ']')
        else:
            print('El status de la respuesta a la solicitud de Login no fue una A [' + str(data[1]) + ']')
    else:
        print('Respuesta recibida, longitud diferente')

finally:
    print('Cerrando Socket...')
    sock.close()