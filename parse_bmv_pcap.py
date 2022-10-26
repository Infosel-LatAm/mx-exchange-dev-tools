# set bash later
import dpkt
import json
import struct
import sys

# note on networking info.
# This doesn't deal with ETH_TYPE_IP6, but it should be possible
#

BMV_TIMESTAMP_FORMAT = ">q"
BMV_INT8_FORMAT = ">b"
BMV_INT16_FORMAT = ">h"
BMV_INT32_FORMAT = ">i"
BMV_PRECIO4_FORMAT = ">i"
BMV_PRECIO8_FORMAT = ">q"

# Some statistics
counter_msgs = {}


def parse_bmv_alfa(bytes_array: bytes):
    # All ALPHA fields are ISO 8859-1, left aligned and filled on the right with spaces.
    # assert bytes_array is bytes, "To parse it correctly it must be used on a bytes object"
    return bytes_array.decode("iso-8859-1").rstrip()


def parse_bmv_timestamp1(bytes_array):  # TODO
    # All integer fields are ordered under the big-endian system and have a sign.
    # hack ctime despues de quitarse 3 digitoos (Julian)
    assert len(bytes_array) == 8, "Can only be applied to arrays of 8 bytes"
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, "Format must be as well 8 bytes"
    return struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]


def parse_bmv_timestamp2(bytes_array):  # TODO
    # All integer fields are ordered under the big-endian system and have a sign.
    # hack ctime despues de quitarse 3 digitios (Julian)
    assert len(bytes_array) == 8, "Can only be applied to arrays of 8 bytes"
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, "Format must be as well 8 bytes"
    return struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]


def parse_bmv_int8(bytes_array):
    # All integer fields are ordered under the big-endian system and have a sign.
    assert len(bytes_array) == 1, "Can only be applied to arrays of 1 bytes"
    assert struct.calcsize(BMV_INT8_FORMAT) == 1, "Format must be as well 1 bytes"
    return struct.unpack(BMV_INT8_FORMAT, bytes_array)[0]


def parse_bmv_int16(bytes_array):
    # All integer fields are ordered under the big-endian system and have a sign.
    assert len(bytes_array) == 2, "Can only be applied to arrays of 2 bytes"
    assert struct.calcsize(BMV_INT16_FORMAT) == 2, "Format must be as well 2 bytes"
    return struct.unpack(BMV_INT16_FORMAT, bytes_array)[0]


def parse_bmv_int32(bytes_array):
    # All integer fields are ordered under the big-endian system and have a sign.
    assert len(bytes_array) == 4, "Can only be applied to arrays of 4 bytes"
    assert struct.calcsize(BMV_INT32_FORMAT) == 4, "Format must be as well 4 bytes"
    return struct.unpack(BMV_INT32_FORMAT, bytes_array)[0]


def parse_bmv_precio4(bytes_array):
    # All integer fields are ordered under the big-endian system and have a sign.
    assert len(bytes_array) == 4, "Can only be applied to arrays of 4 bytes"
    assert struct.calcsize(BMV_PRECIO4_FORMAT) == 4, "Format must be as well 4 bytes"
    precio = float(struct.unpack(BMV_PRECIO4_FORMAT, bytes_array)[0])
    return precio / 1000.0


def parse_bmv_precio8(bytes_array):
    # All integer fields are ordered under the big-endian system and have a sign.
    assert len(bytes_array) == 8, "Can only be applied to arrays of 4 bytes"
    assert struct.calcsize(BMV_PRECIO8_FORMAT) == 8, "Format must be as well 4 bytes"
    precio = float(struct.unpack(BMV_PRECIO8_FORMAT, bytes_array)[0])
    return precio / 10000000.0


def parse_bmv_mensaje_P(bytes_array):
    assert len(bytes_array) == 52, f"Mensaje P debe tener 52 bytes y tiene ${len(bytes_array)}"
    tipo_mensaje = parse_bmv_alfa(bytes_array[:1])
    assert tipo_mensaje == 'P', "This parsing only works for mensaje P"
    msg_P = {}
    # Expliclity review each conversion
    msg_P["numero_instrumento"] = parse_bmv_int32(bytes_array[1:5])
    msg_P["hora_hecho"] = parse_bmv_timestamp2(bytes_array[5:13])
    msg_P["volumen"] = parse_bmv_int32(bytes_array[13:17])
    msg_P["precio"] = parse_bmv_precio8(bytes_array[17:25])
    msg_P["tipo_concertacion"] = parse_bmv_alfa(bytes_array[25:26])
    msg_P["folio_del_hecho"] = parse_bmv_int32(bytes_array[26:30])
    msg_P["fija_precio"] = parse_bmv_alfa(bytes_array[30:31])
    msg_P["tipo_operacion"] = parse_bmv_alfa(bytes_array[31:32])
    msg_P["importe"] = parse_bmv_precio8(bytes_array[32:40])
    msg_P["compra"] = parse_bmv_alfa(bytes_array[40:45])
    msg_P["vende"] = parse_bmv_alfa(bytes_array[45:50])
    msg_P["liquidacion"] = parse_bmv_alfa(bytes_array[50:51])
    msg_P["subasta"] = parse_bmv_alfa(bytes_array[51:52])
    return msg_P


def parse_bmv_udp_packet(packet):
    packet_dict = {}
    longitud = parse_bmv_int16(packet.data[:2])
    assert longitud == len(packet.data), "Longitud $(longitud) must be the same as the packet size $(len(packet.data))"
    packet_dict['longitud'] = longitud
    total_mensajes = parse_bmv_int8(packet.data[2:3])
    assert total_mensajes > 0, "There must be at least one message on the packet"
    packet_dict['total_mensajes'] = total_mensajes
    packet_dict['grupo_market_data'] = parse_bmv_int8(packet.data[3:4])
    assert packet_dict['grupo_market_data'] == 18, "We only deal with grupo 18 BMV messages"
    packet_dict['sesion'] = parse_bmv_int8(packet.data[4:5])
    assert packet_dict['sesion'] == 2, "We have never seen another sesion than 2"
    numero_secuencia = parse_bmv_int32(packet.data[5:9])
    packet_dict['numero_secuencia']: numero_secuencia
    packet_dict['timestamp'] = parse_bmv_timestamp1(packet.data[9:17])
    mensajes = []
    start = 17
    for i in range(0, total_mensajes):
        longitud_msg = parse_bmv_int16(packet.data[start:start + 2])  # Longitude does not include the longitude field
        tipo_mensaje = parse_bmv_alfa(packet.data[start + 2:start + 3])
        if tipo_mensaje not in counter_msgs:
            counter_msgs[tipo_mensaje] = 1
        else:
            counter_msgs[tipo_mensaje] += 1
        if tipo_mensaje == 'P':
            mensaje_p = parse_bmv_mensaje_P(packet.data[start + 2:start + longitud_msg + 2])
            mensaje_p['longitud_msg']: longitud_msg
            mensajes.append(mensaje_p)
        else:
            mensajes.append({"tipo_mensaje": str(tipo_mensaje), "longitud_msg": longitud_msg})
            # We could add ,
            # "payload": base64.b64encode(packet.data[start+3:start+longitud]).decode('utf-8')
        start += longitud_msg + 2  # the 2 is to account the longitude field
    packet_dict['mensajes'] = mensajes
    return numero_secuencia, total_mensajes, json.dumps(packet_dict)


def parse_pcap_file(input_file, output_file):
    pcap = dpkt.pcap.Reader(input_file)

    last_secuencia = None
    for timestamp, pkt in pcap:
        eth = dpkt.ethernet.Ethernet(pkt)
        if eth.type == dpkt.ethernet.ETH_TYPE_IP:
            ip = eth.data
            if ip.p == dpkt.ip.IP_PROTO_UDP:  # Comprobar que vengan de la direccion correcta
                udp_packet = ip.data
                numero_secuencia, total_mensajes, json_data = parse_bmv_udp_packet(udp_packet)
                if not last_secuencia:
                    last_secuencia = numero_secuencia + total_mensajes
                else:
                    if last_secuencia < numero_secuencia:
                        print(f"Salto de secuencia de ${last_secuencia} a ${numero_secuencia}")
                    elif last_secuencia > numero_secuencia:
                        print(f"Mensajes en desorden? ${last_secuencia} a ${numero_secuencia}")
                    last_secuencia = numero_secuencia + total_mensajes
                print(json_data, file=output_file)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('[Error] Usage: parse_bmv_pcap.py [pcapfile.pcap] [output.json]')
        sys.exit(1)
    pcap_filename = sys.argv[1]
    json_output_filename = sys.argv[2]
    parse_pcap_file(open(pcap_filename, 'rb'), open(json_output_filename, 'wt'))
    print(counter_msgs)
