"""
Utilities to parse BMV messages from multicast.
"""
from io import BufferedReader

import dpkt
import json
import struct
from datetime import datetime

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
BMV_INT64_FORMAT = ">q"
BMV_PRECIO4_FORMAT = ">i"
BMV_PRECIO8_FORMAT = ">q"


#
# BMV catalogos
#
BMV_TIPOS_VALOR = ('1','1A', '1I', 'CF', '1R', '1B', '0', '1E', '1C', 'FF', 'FE', '41', '3', 'FH')
BMV_BOLSA_ORIGEN = ("M","I")
BMV_TIPOS_CONCERTACION = ('C', 'O', 'H', 'D', 'M', 'P', 'X', 'v', 'w', '%', 'x', 'y', 'A', 'B', 'E', 'F', 'J', 'K', 'L', 'N', 'Q')
BMV_TIPOS_OPERACION = ('E', 'C', 'B', 'D', 'W', 'X')
BMV_TIPOS_LIQUIDACION = ('M', '2', '4', '7', '9', '1')
BMV_TIPOS_SUBASTA = ('P', 'S', ' ', 'N', '')

# Catálogo de referencia
# https://tecnologia.bmv.com.mx/especificacion/multicast/msg/structure/catalogs.html#cat_referencia
BMV_CATALOGO_REFERENCIA = ("AN", "AJ", "", "VA")
BMV_BURSATILIDAD = ("AL", "ME", "BA", "MI", "RC", "NU")
BMV_MERCADOS = ("L", "G", "D", "F", "E", "M", "X", "V", "J", "B", "T", "I", "W", " ")



#
# BMV Data types parsing
#

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


def parse_bmv_int64(bytes_array: bytes) -> int:
    """Parses 8 bytes as an integer as specified by BMV"""
    assert len(bytes_array) == 8, "Only apply to arrays of 8 bytes"
    assert struct.calcsize(BMV_INT64_FORMAT) == 8, "Format must have as well 4 bytes"
    return struct.unpack(BMV_INT64_FORMAT, bytes_array)[0]


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


#
# Producto 18 Messages
#

def parse_bmv_mensaje_M(bytes_array: bytes) -> dict:
    """Parses an array of 21 bytes as a 'mensaje M' as specified by BMV"""
    msg_M: dict
    assert len(bytes_array) == 21, f"Mensaje M debe tener 9 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == 'M', "This parsing only works for 'mensaje M'"
    msg_M = {"key": 0, "tipoMensaje": tipo_mensaje,
             "precioPromedioPonderado": parse_bmv_precio8(bytes_array[9:17]),
             "volatilidad": parse_bmv_precio8(bytes_array[9:17])
             }
    assert int(msg_M['numeroInstrumento']) > 0, f"Numero instrumento {msg_M['numeroInstrumento']} debe ser mayor a cero"
    assert int(msg_M['folioHecho']) > 0.0, "El folio del hecho debe ser mayor a cero"
    assert float(msg_M['precioPromedioPonderado']) > 0.0, "El Precio Promedio Ponderado debe ser mayor a cero"
    assert float(msg_M['volatilidad']) > 0.0, "La Volatilidad debe ser mayor a cero"
    # Done with checks
    return msg_M


def parse_bmv_mensaje_H(bytes_array: bytes) -> dict:
    """Parses an array of 9 bytes as a 'Mensaje H' as specified by BMV"""
    msg_H: dict
    assert len(bytes_array) == 9, f"Mensaje H debe tener 9 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == 'H', "This parsing only works for mensaje H"
    msg_H = {"key": 0, "tipoMensaje": tipo_mensaje,
             "numeroInstrumento": parse_bmv_int32(bytes_array[1:5]),
             "folioHecho": parse_bmv_int32(bytes_array[5:9]),
             }
    assert int(msg_H['numeroInstrumento']) > 0, f"Numero instrumento {msg_H['numeroInstrumento']} debe ser mayor a cero"
    assert int(msg_H['folioHecho']) > 0, "El folio del hecho debe ser mayor a cero"
    # Done with checks
    return msg_H


def parse_bmv_mensaje_O(bytes_array: bytes) -> dict:
    """Parses an array of 19 bytes as a 'Mensaje O' as specified by BMV"""
    msg_O: dict
    assert len(bytes_array) == 19, f"Mensaje O debe tener 19 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == 'O', "This parsing only works for mensaje O"
    msg_O = {"key": 0, "tipoMensaje": tipo_mensaje,
             "numeroInstrumento": parse_bmv_int32(bytes_array[1:5]),
             "volumen": parse_bmv_int32(bytes_array[5:9]),
             "precio": parse_bmv_precio8(bytes_array[9:17]),
             "sentido": parse_alfa(bytes_array[17:18]),
             "tipo": parse_alfa(bytes_array[18:19])
             }
    assert int(msg_O['numeroInstrumento']) > 0, f"Numero instrumento {msg_O['numeroInstrumento']} debe ser mayor a cero"
    assert int(msg_O['volumen']) >= 0, "El Volumen acumulado debe ser cero o mayor"
    assert float(msg_O['precio']) >= 0.0, f"El precio  {msg_O['precio']} debe ser mayor o igual a cero"
    assert msg_O['sentido'] in ('C', 'V'), f"El sentido '{msg_O['sentido']}' no es conocido"
    assert msg_O['tipo'] in ('C', 'H', 'P', 'N'), f"El tipo '{msg_O['tipo']}' no es conocido"  
    # Done with checks
    return msg_O

def parse_bmv_mensaje_E(bytes_array: bytes) -> dict:
    """Parses an array of 65 bytes as a 'Mensaje E' as specified by BMV"""
    msg_E: dict
    assert len(bytes_array) == 65, f"Mensaje E debe tener 65 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == 'E', "This parsing only works for mensaje E"
    msg_E = {"key": 0, "tipoMensaje": tipo_mensaje,
             "numeroInstrumento": parse_bmv_int32(bytes_array[1:5]),
             "numeroOperaciones": parse_bmv_int32(bytes_array[5:9]),
             "volumen": parse_bmv_int64(bytes_array[9:17]),
             "importe": parse_bmv_precio8(bytes_array[17:25]),
             "apertura": parse_bmv_precio8(bytes_array[25:33]),
             "maximo": parse_bmv_precio8(bytes_array[33:41]),
             "minimo": parse_bmv_precio8(bytes_array[41:49]),
             "promedio": parse_bmv_precio8(bytes_array[49:57]),
             "last": parse_bmv_precio8(bytes_array[57:65])
             }
    assert int(msg_E['numeroInstrumento']) > 0, f"Numero instrumento {msg_E['numeroInstrumento']} debe ser mayor a cero"
    assert int(msg_E['numeroOperaciones']) >= 0, "El numero de operaciones es cero o mayor"
    assert int(msg_E['volumen']) >= 0, "El Volumen acumulado debe ser cero o mayor"
    assert float(msg_E['importe']) >= 0.0, "El importe acumulado debe ser cero o mayor"
    assert float(msg_E['apertura']) >= 0.0, f"El precio de apertura {msg_E['apertura']} debe ser mayor o igual a cero"
    assert float(msg_E['maximo']) >= 0.0, f"El precio maximo {msg_E['maximo']} debe ser mayor o igual a cero"
    assert float(msg_E['minimo']) >= 0.0, f"El precio minimo {msg_E['minimo']} debe ser mayor o igual a cero"
    assert float(msg_E['promedio']) >= 0.0, f"El precio promedio {msg_E['promedio']} debe ser mayor o igual a cero"
    assert float(msg_E['last']) > 0.0, f"El ultimo precio {msg_E['last']} debe ser mayor a cero"
    # Done with checks
    return msg_E



def parse_bmv_mensaje_P(bytes_array: bytes) -> dict:
    """Parses an array of 52 bytes as a 'Mensaje P' as specified by BMV"""
    msg_P: dict
    assert len(bytes_array) == 52, f"Mensaje P debe tener 52 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == 'P', "This parsing only works for mensaje P"
    msg_P = {"key": 0, "tipoMensaje": tipo_mensaje,
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
    assert int(msg_P['numeroInstrumento']) > 0, f"Numero instrumento {msg_P['numeroInstrumento']} debe ser mayor a cero"
    assert int(msg_P['volumen']) > 0, "El Volumen de un hecho siempre debe ser mayor a cero"
    assert float(msg_P['precio']) > 0.0, "El precio de un hecho siempre debe ser mayor a cero"
    assert msg_P['tipoConcertacion'] in BMV_TIPOS_CONCERTACION
    assert int(msg_P['folioHecho']) > 0, "El folio del hecho debe ser mayor a cero"
    assert msg_P['tipoOperacion'] in BMV_TIPOS_OPERACION, \
        f"El tipo de operacion '{msg_P['tipoOperacion']}' no es conocido"  # X es nuevo
    assert float(msg_P['importe']) > 0.0, "El importe de un hecho siempre debe ser mayor a cero"
    assert msg_P['liquidacion'] in BMV_TIPOS_LIQUIDACION, \
        f"El tipo de liquidacion {msg_P['liquidacion']} no es conocido"
    assert msg_P['indicadorSubasta'] in BMV_TIPOS_SUBASTA, \
        f"'{msg_P['indicadorSubasta']}' no es un tipo de subasta conocido"  # '' es nuevo.
    return msg_P


#
# Producto 40 Messages
#


def parse_bmv_mensaje_ca(bytes_array: bytes) -> dict:
    """Parses an array of 113 bytes as a 'catalogo ca' as specified by BMV"""
    cat_ca: dict
    assert len(bytes_array) == 113, f"Catalogo ca debe tener 113 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_alfa(bytes_array[0:2])  # This is two bytes for Producto 40.
    assert tipo_mensaje == 'ca', "This parsing only works for mensaje ca"
    cat_ca = {"key": 0, "tipoMensaje": tipo_mensaje,
             "numeroInstrumento": parse_bmv_int32(bytes_array[2:6]),
             "tipoValor": parse_alfa(bytes_array[6:8]),
             "emisora": parse_alfa(bytes_array[8:15]),
             "serie": parse_alfa(bytes_array[15:21]),
             "ultimoPrecio": parse_bmv_precio8(bytes_array[21:29]),
             "PPP": parse_bmv_precio8(bytes_array[29:37]),
             "precioCierre": parse_bmv_precio8(bytes_array[37:45]),
             "fechaReferencia": parse_bmv_timestamp1(bytes_array[45:53]).isoformat(),
             "referencia": parse_alfa(bytes_array[53:55]),
             "cuponVigente": parse_bmv_int16(bytes_array[55:57]),
             "bursatilidad": parse_alfa(bytes_array[57:59]),
             "bursatilidadNumerica": parse_bmv_precio4(bytes_array[59:63]),
             "ISIN": parse_alfa(bytes_array[63:75]),
             "mercado": parse_alfa(bytes_array[75:76]),
             "valoresInscritos": parse_bmv_int64(bytes_array[76:84]),
             "importeBloques": parse_bmv_precio8(bytes_array[84:92]),
             "bolsaOrigen": parse_alfa(bytes_array[92:93]),
             # There's a filler of 20 bytes, from 93 to 113 that we ignore.
             }
    assert int(cat_ca['numeroInstrumento']) > 0, f"Numero instrumento {cat_ca['numeroInstrumento']} debe ser mayor a cero."
    # assert cat_ca['tipoValor'] in BMV_TIPOS_VALOR, f"El tipo de valor {cat_ca['tipoValor']} no esta en el catálogo."
    if cat_ca['tipoValor'] not in BMV_TIPOS_VALOR:
        print(f"Tipo de valor {cat_ca['tipoValor']}")
    assert float(cat_ca['ultimoPrecio']) >= 0.0, "El ultimo precio debe >= 0.0"
    assert float(cat_ca['PPP']) >= 0.0, "El PPP debe ser >= 0.0"
    assert float(cat_ca['precioCierre']) >= 0.0, "El precio de cierre debe ser >= 0.0"
    # assert cat_ca['fechaReferencia'] is True, "ToDo"
    assert cat_ca['referencia'] in BMV_CATALOGO_REFERENCIA, f"La Referencia {cat_ca['referencia']} no esta en el catálogo."
    assert float(cat_ca['cuponVigente']) >= 0, "El cupon vigente debe ser mayor o igual a cero."
    assert cat_ca['bursatilidad'] in BMV_BURSATILIDAD, f"La bursatilidad '{cat_ca['bursatilidad']}' no es conocida."  
    assert float(cat_ca['bursatilidadNumerica']) >= 0.0, "La Bursatilidad Numérica debe ser >= 0.0"
    assert cat_ca['mercado'] in BMV_MERCADOS, f"El mercado {cat_ca['mercado']} no es conocido."
    assert float(cat_ca['valoresInscritos']) > 0, "Los valores inscritos deben ser mayores a cero." 
    assert float(cat_ca['importeBloques']) >= 0.0, "El importe de bloques debe ser >= 0.0"
    assert cat_ca['bolsaOrigen'] != None, "La Bolsa origen debe estar definida."
    if cat_ca['bolsaOrigen'] not in BMV_BOLSA_ORIGEN:
        print(f"Bolsa origen {cat_ca['bolsaOrigen']}")
    return cat_ca



def parse_by_message_type(grupo_market_data: int, to_parse: bytes) -> dict|None:
    """Given a tipo_mensaje we assume matches the bytes array, we call the appropiate parsing function
    Returns:
        mensaje: A dictionary with the parsed fields
    """
    mensaje = None
    tipo_mensaje: str
    # Select how big is the tipo_mensaje field based on the grupo_market_data
    if grupo_market_data == 18:
        tipo_mensaje = parse_alfa(to_parse[0:1])
    elif grupo_market_data == 40:
        tipo_mensaje = parse_alfa(to_parse[0:2])
    # Based on the tipo_mensaje, parse the message
    match tipo_mensaje:
        case 'P':
            mensaje = parse_bmv_mensaje_P(to_parse)
        case 'E':
         mensaje = parse_bmv_mensaje_E(to_parse)
        case 'H':
            mensaje = parse_bmv_mensaje_H(to_parse)
        case 'O':
            mensaje = parse_bmv_mensaje_O(to_parse)
        case 'M':
            mensaje = parse_bmv_mensaje_M(to_parse)
        case 'ca':
            mensaje = parse_bmv_mensaje_ca(to_parse)
    return mensaje       
        

def parse_bmv_udp_packet(packet_data: bytes) -> dict:
    """Parses an udp packet as containing a header and 1 or more messages, as specified by BMV"""
    paquete = {}
    longitud:int = parse_bmv_int16(packet_data[:2])
    assert longitud == len(packet_data), f"Longitud {longitud} must be equal to the packet size {len(packet_data)})"
    paquete['longitud'] = longitud
    total_mensajes = parse_bmv_int8(packet_data[2:3])
    assert total_mensajes >= 0, "We need a 0 or positive number"
    paquete['total_mensajes'] = total_mensajes
    paquete['grupo_market_data'] = parse_bmv_int8(packet_data[3:4])
    assert paquete['grupo_market_data'] in (18,40), "We only deal with grupo 18 or grupo 40 BMV messages"
    paquete['sesion'] = parse_bmv_int8(packet_data[4:5])
    # http://tecnologia.bmv.com.mx:6503/especificacion/multicast/msg/structure/catalogs.html#cat_grupo_market_data
    assert 0 <= paquete['sesion'] <= 40, "La sesion debe estar entre 0, y 40"
    paquete['secuencia'] = secuencia = parse_bmv_int32(packet_data[5:9])
    assert 0 <= paquete['secuencia'], "La secuencia debe ser mayor a cero."
    paquete['timestamp'] = timestamp = parse_bmv_timestamp3(packet_data[9:HEADER_SIZE])
    mensajes = []
    start = HEADER_SIZE
    for i in range(0, total_mensajes):
        # Longitude does not include the longitude field
        longitud_msg = parse_bmv_int16(packet_data[start:start + 2])
        to_parse = packet_data[start + 2:start + longitud_msg + 2]
        mensaje = parse_by_message_type(paquete['grupo_market_data'], to_parse)
        if mensaje:
            mensaje['key'] = f"{timestamp.strftime('%Y%m%d')}-{secuencia + i}"
            mensaje['fechaHora'] = timestamp.isoformat(timespec='milliseconds')
            mensaje['timestamp'] = timestamp.now().isoformat()
            mensaje['longitud'] = longitud_msg
            mensajes.append(mensaje)
        start += longitud_msg + 2  # add 2 to account the longitude field
    paquete['mensajes'] = mensajes
    return paquete


def parse_bmv_pcap_file(input_file: BufferedReader, output_file) -> dict:
    """Parses a complete cap file assuming it has only udp packets from BMV 'producto 18' or 'producto 40'"""
    pcap = dpkt.pcap.Reader(input_file)
    # We will keep basic statistics of how many messages we process per each type.
    counter_msgs = {}

    last_sequence = None
    for timestamp, pkt in pcap:
        try:
            eth:dpkt.ethernet.Ethernet = dpkt.ethernet.Ethernet(pkt)
            ip:dpkt.ip.IP = eth.data
            if ip.p == dpkt.ip.IP_PROTO_UDP:  # Make sure is UDP.    
                udp_packet = ip.data
                last_sequence = process_bmv_udp_packet(output_file, counter_msgs, last_sequence, udp_packet)
        except Exception as e:
            # We will try to continue parsing the file, even if we have an error
            # Until now we find that the last sequence is incomplete or corrupted so 
            # we try to ignored it.
            print(e)
            print(f"Unexpected error on sequence {last_sequence}, trying to continue...")
            continue
    print(f"Last found sequence is {last_sequence}")
    return counter_msgs

def process_bmv_udp_packet(output_file, counter_msgs, last_sequence, udp_packet) -> int:
    paquete = parse_bmv_udp_packet(udp_packet.data)
    if not last_sequence:
        last_sequence = paquete['secuencia'] + paquete['total_mensajes']
        print(f"Primera secuencia es {last_sequence}")
    else:
        if last_sequence < paquete['secuencia']:  # ToDo - check the pcaps
            print(f"Salto de secuencia: de {last_sequence} a {paquete['secuencia']}")
        elif last_sequence > paquete['secuencia']:
            print(f"Mensajes en desorden: {last_sequence} a {paquete['secuencia']}")
        last_sequence = paquete['secuencia'] + paquete['total_mensajes']
    for mensaje in paquete['mensajes']:
        tipo_msg = mensaje['tipoMensaje']
        if tipo_msg not in counter_msgs:
            counter_msgs[tipo_msg] = {}
            counter_msgs[tipo_msg]['total'] = 1
            counter_msgs[tipo_msg]['bytes'] = mensaje['longitud']
        else:
            counter_msgs[tipo_msg]['total'] += 1
            counter_msgs[tipo_msg]['bytes'] += mensaje['longitud']
        print(json.dumps(mensaje), file=output_file)
    return last_sequence
