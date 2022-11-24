"""
Utilities to parse BMV messages from multicast.
"""
from io import BufferedReader

import dpkt
import json
import struct
from datetime import datetime

BMV_PROD_18_GRP_A = "239.100.100.18"  # Grupo UDP para producto 18 de BMV, feed A
BMV_PROD18_PORT_A = 12121
BMV_PROD18_GRP_B = "239.100.200.18"  # Grupo UDP para producto 18 de BMV, feed B
BMV_PROD18_PORT_B = 12122

HEADER_SIZE = 17

# Based on the documentation available here
# http://tecnologia.bmv.com.mx:6503/especificacion/multicast/msg/structure/header.html
# http://tecnologia.bmv.com.mx:6503/especificacion/index.html

#  Notes On Conversions rules
#
# * in BMV all integer fields are big-endian, and have a sign.

#
# BMV format definitions based on the conversion rules above.
#
BMV_TIMESTAMP_FORMAT = ">q"
BMV_INT8_FORMAT = ">b"
BMV_INT16_FORMAT = ">h"
BMV_INT32_FORMAT = ">i"
BMV_PRECIO4_FORMAT = ">i"
BMV_PRECIO8_FORMAT = ">q"


def parse_alfa(bytes_array: bytes):
    """Parses an array of bytes as a string as specified by BMV"""
    # All ALPHA fields are ISO 8859-1, left aligned and filled on the right with spaces.
    return bytes_array.decode("iso-8859-1").rstrip()


def parse_bmv_timestamp1(bytes_array: bytes) -> datetime:
    """Parses a timestamp of type 1 (Solo Fecha) as specified by BMV"""
    assert len(bytes_array) == 8, "Only apply to arrays of 8 bytes"
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, "Format must have as well 8 bytes"
    data = struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]
    timestamp = datetime.fromtimestamp(data // 1000)  # Eliminate 3 last digits that signify milliseconds
    return timestamp  # Return only the date.


def parse_bmv_timestamp2(bytes_array: bytes) -> datetime:
    """Parses a timestamp of type 2 (Fecha y Hora con precision de segundos) as specified by BMV"""
    assert len(bytes_array) == 8, "Only apply to arrays of 8 bytes"
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, "Format shall have 8 bytes long"
    data = struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]
    timestamp = datetime.fromtimestamp(data // 1000)  # Eliminate 3 last digits that signify milliseconds
    return timestamp  # Return date and time


def parse_bmv_timestamp3(bytes_array: bytes) -> datetime:
    """Parses a timestamp of type 3 (Fecha y Hora con precision de milisegundos) as specified by BMV"""
    assert len(bytes_array) == 8, "Only apply to arrays of 8 bytes"
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, "Format length is 8 bytes"
    data = struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]
    timestamp = datetime.fromtimestamp(data // 1000)  # Eliminate 3 last digits that signify milliseconds
    timestamp = timestamp.replace(microsecond=(data % 1000) * 1000)
    return timestamp  # Return date, time and seconds with milliseconds precision.


def parse_bmv_int8(bytes_array: bytes) -> int:
    """Parses 1 byte as an integer as specified by BMV"""
    assert len(bytes_array) == 1, "Only apply to arrays of 1 byte"
    assert struct.calcsize(BMV_INT8_FORMAT) == 1, "Format must have as well 1 bytes"
    return struct.unpack(BMV_INT8_FORMAT, bytes_array)[0]


def parse_bmv_int16(bytes_array: bytes) -> int:
    """Parses 2 bytes as an integer as specified by BMV"""
    assert len(bytes_array) == 2, "Only apply to arrays of 2 bytes"
    assert struct.calcsize(BMV_INT16_FORMAT) == 2, "Format must have as well 2 bytes"
    return struct.unpack(BMV_INT16_FORMAT, bytes_array)[0]


def parse_bmv_int32(bytes_array: bytes) -> int:
    """Parses 4 bytes as an integer as specified by BMV"""
    assert len(bytes_array) == 4, "Only apply to arrays of 4 bytes"
    assert struct.calcsize(BMV_INT32_FORMAT) == 4, "Format must have as well 4 bytes"
    return struct.unpack(BMV_INT32_FORMAT, bytes_array)[0]


def parse_bmv_precio4(bytes_array: bytes) -> float:
    """Parses 4 bytes as a 'precio' with 4 decimal digits. As specified by BMV"""
    assert len(bytes_array) == 4, "Only apply to arrays of 4 bytes"
    assert struct.calcsize(BMV_PRECIO4_FORMAT) == 4, "Format must have as well 4 bytes"
    precio = float(struct.unpack(BMV_PRECIO4_FORMAT, bytes_array)[0])
    return precio / 1000.0


def parse_bmv_precio8(bytes_array: bytes) -> float:
    """Parses 4 byte as a 'precio' with 8 decimal digits. As specified by BMV"""
    assert len(bytes_array) == 8, "nly apply to arrays of 8 bytes"
    assert struct.calcsize(BMV_PRECIO8_FORMAT) == 8, "Format must have as well 4 bytes"
    precio = float(struct.unpack(BMV_PRECIO8_FORMAT, bytes_array)[0])
    return precio / 100000000.0


def parse_bmv_mensaje_P(bytes_array: bytes) -> dict:
    """Parses an array of 52 bytes as a 'Mensaje P' as specified by BMV"""
    msg_P: dict
    assert len(bytes_array) == 52, f"Mensaje P debe tener 52 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == 'P', "This parsing only works for mensaje P"
    msg_P = {"key": 0, "timestamp": '-', "fechaHora": '-', "tipoMensaje": 'P',
             "numeroInstrumento": parse_bmv_int32(bytes_array[1:5]),
             "horaHecho": parse_bmv_timestamp2(bytes_array[5:13]).isoformat(),
             "volumen": parse_bmv_int32(bytes_array[13:17]),
             "precio": parse_bmv_precio8(bytes_array[17:25]),
             "tipoConcertacion": parse_alfa(bytes_array[25:26]),
             "folioHecho": parse_bmv_int32(bytes_array[26:30]),
             "fijaPrecio": parse_alfa(bytes_array[30:31]) == '1',
             "tipoOperacion": parse_alfa(bytes_array[31:32]),
             "importe": parse_bmv_precio8(bytes_array[32:40]),
             "compra": parse_alfa(bytes_array[40:45]),
             "vende": parse_alfa(bytes_array[45:50]),
             "liquidacion": parse_alfa(bytes_array[50:51]),
             "indicadorSubasta": parse_alfa(bytes_array[51:52])
             }
    assert msg_P['numeroInstrumento'] > 0, f"Numero instrumento {msg_P['numeroInstrumento']} debe ser mayor a cero"
    assert msg_P['volumen'] > 0, "El Volumen de un hecho siempre debe ser mayor a cero"
    assert msg_P['precio'] > 0, "El precio de un hecho siempre debe ser mayor a cero"
    #assert msg_P['tipoConcertacion'] in ('C', 'O', 'H', 'D', 'M', 'P', 'X', 'v', 'w', '%', 'x', 'y', 'A', 'B', 'E', 'F', 'J', 'K', 'L', 'N', 'Q'
    assert msg_P['folioHecho'] > 0, "El folio del hecho debe ser mayor a cero"
    assert msg_P['tipoOperacion'] in ('E', 'C', 'B', 'D', 'W', 'X'), \
        f"El tipo de operacion '{msg_P['tipoOperacion']}' no es conocido"  # X es nuevo
    assert msg_P['importe'] > 0, "El importe de un hecho siempre debe ser mayor a cero"
    assert msg_P['liquidacion'] in ('M', '2', '4', '7', '9', '1'), "El tipo de liquidacion no es conocido"
    assert msg_P['indicadorSubasta'] in ('P', 'S', ' ', 'N', ''), \
        f"'{msg_P['indicadorSubasta']}' no es un tipo de subasta conocido"  # '' es nuevo.
    return msg_P


def parse_bmv_udp_packet(packet_data: bytes, counter_msgs: dict) -> (int, int, dict, dict):
    """Parses an udp packet as containing a header and 1 or more messages, as specified by BMV"""
    paquete = {}
    longitud = parse_bmv_int16(packet_data[:2])
    # assert longitud == len(packet_data), "Longitud $(longitud) must be equal to the packet size $(len(packet.data))"
    paquete['longitud'] = longitud
    total_mensajes = parse_bmv_int8(packet_data[2:3])
    # assert total_mensajes >= 0, "We need a 0 or positive number"
    paquete['total_mensajes'] = total_mensajes
    paquete['grupo_market_data'] = parse_bmv_int8(packet_data[3:4])
    assert paquete['grupo_market_data'] == 18, "We only deal with grupo 18 BMV messages"
    paquete['sesion'] = parse_bmv_int8(packet_data[4:5])
    # http://tecnologia.bmv.com.mx:6503/especificacion/multicast/msg/structure/catalogs.html#cat_grupo_market_data
    # assert 0 <= paquete['sesion'] <= 40, "La sesion debe estar entre 1, y 40"
    paquete['secuencia'] = secuencia = parse_bmv_int32(packet_data[5:9])
    timestamp = parse_bmv_timestamp3(packet_data[9:HEADER_SIZE])
    mensajes = []
    start = HEADER_SIZE
    for i in range(0, total_mensajes):
        # Longitude does not include the longitude field
        longitud_msg = parse_bmv_int16(packet_data[start:start + 2])
        tipo_mensaje = parse_alfa(packet_data[start + 2:start + 3])
        if tipo_mensaje not in counter_msgs:
            counter_msgs[tipo_mensaje] = {}
            counter_msgs[tipo_mensaje]['total'] = 1
            counter_msgs[tipo_mensaje]['bytes'] = longitud_msg
        else:
            counter_msgs[tipo_mensaje]['total'] += 1
            counter_msgs[tipo_mensaje]['bytes'] += longitud_msg
        if tipo_mensaje == 'P':
            mensaje_p = parse_bmv_mensaje_P(packet_data[start + 2:start + longitud_msg + 2])
            mensaje_p['key'] = f"{timestamp.strftime('%Y%m%d')}-{secuencia + i}"
            mensaje_p['fechaHora'] = timestamp.isoformat(timespec='milliseconds')
            mensaje_p['timestamp'] = timestamp.now().isoformat()
            mensajes.append(mensaje_p)
        else:
            pass  # We do not deal with any other message types.
        start += longitud_msg + 2  # add 2 to account the longitude field
    paquete['mensajes'] = mensajes
    return paquete['secuencia'], paquete['total_mensajes'], counter_msgs, paquete


def parse_pcap_file(input_file: BufferedReader, output_file) -> dict:
    """Parses a complete cap file assuming it has only udp packets from BMV 'producto 18'"""
    pcap = dpkt.pcap.Reader(input_file)
    # We will keep basic statistics of how many messages we process per each type.
    counter_msgs = {}

    last_secuencia = None
    for timestamp, pkt in pcap:
        eth = dpkt.ethernet.Ethernet(pkt)
        if eth.type == dpkt.ethernet.ETH_TYPE_IP:
            ip = eth.data
            if ip.p == dpkt.ip.IP_PROTO_UDP:  # Comprobar que vengan de la direccion correcta
                udp_packet = ip.data
                secuencia, total_mensajes, counter_msgs, paquete = parse_bmv_udp_packet(udp_packet.data, counter_msgs)
                if not last_secuencia:
                    last_secuencia = secuencia + total_mensajes
                else:
                    if last_secuencia < secuencia:  # ToDo - check the pcaps
                        print(f"Salto de secuencia: de {last_secuencia} a {secuencia}")
                    elif last_secuencia > secuencia:
                        print(f"Mensajes en desorden: {last_secuencia} a {secuencia}")
                    last_secuencia = secuencia + total_mensajes
                for mensaje in paquete['mensajes']:
                    print(json.dumps(mensaje), file=output_file)
    return counter_msgs
