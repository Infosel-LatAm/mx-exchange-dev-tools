'''
Utilities to parse BMV messages from multicast.
'''
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
BMV_TIMESTAMP_FORMAT = '>q'
BMV_INT8_FORMAT = '>b'
BMV_INT16_FORMAT = '>h'
BMV_INT32_FORMAT = '>i'
BMV_INT64_FORMAT = '>q'
BMV_PRECIO4_FORMAT = '>i'
BMV_PRECIO8_FORMAT = '>q'


#
# BMV catalogos
#
BMV_TIPOS_VALOR = ('1','1A', '1I', 'CF', '1R', '1B', '0', '1E', '1C', 'FF', 'FE', '41', '3', 'FH')
BMV_BOLSA_ORIGEN = ('M','I')
BMV_TIPOS_CONCERTACION = ('C', 'O', 'H', 'D', 'M', 'P', 'X', 'v', 'w', '%', 'x', 'y', 'A', 'B', 'E', 'F', 'J', 'K', 'L', 'N', 'Q')
BMV_TIPOS_OPERACION = ('E', 'C', 'B', 'D', 'W', 'X')
BMV_TIPOS_LIQUIDACION = ('M', '2', '4', '7', '9', '1')
BMV_TIPOS_SUBASTA = ('P', 'S', ' ', 'N', '')

BMV_TIPOS_WARRANT = ('C','V')
BMV_TIPOS_BOLSA_ORIGEN = ('M', 'I')
BMV_TIPOS_OPERA_TASA_PRECIO=['P','T']
BMV_TIPOS_OPCION = ('C', 'P')
BMV_TIPOS_VENCIMIENTO_DIARIO = ('1', '0')
BMV_TIPOS_ESTRATEGIA = ('R', 'E', 'F')

# Catálogo de referencia
# https://tecnologia.bmv.com.mx/especificacion/multicast/msg/structure/catalogs.html#cat_referencia
BMV_CATALOGO_REFERENCIA = ('AN', 'AJ', '', 'VA')
BMV_BURSATILIDAD = ('AL', 'ME', 'BA', 'MI', 'RC', 'NU')
BMV_MERCADOS = ('L', 'G', 'D', 'F', 'E', 'M', 'X', 'V', 'J', 'B', 'T', 'I', 'W', ' ')



#
# BMV Data types parsing
#

def parse_alfa(bytes_array: bytes):
    '''Parses an array of bytes as a string as specified by BMV'''
    # All ALPHA fields are ISO 8859-1, left aligned and filled on the right with spaces.
    return bytes_array.decode('iso-8859-1').rstrip()


def parse_bmv_timestamp1(bytes_array: bytes) -> datetime:
    '''Parses a timestamp of type 1 (Solo Fecha) as specified by BMV'''
    assert len(bytes_array) == 8, 'Only apply to arrays of 8 bytes'
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, 'Format must have as well 8 bytes'
    data = struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]
    timestamp = datetime.fromtimestamp(data // 1000)  # Eliminate 3 last digits that signify milliseconds
    return timestamp  # Return only the date.


def parse_bmv_timestamp2(bytes_array: bytes) -> datetime:
    '''Parses a timestamp of type 2 (Fecha y Hora con precision de segundos) as specified by BMV'''
    assert len(bytes_array) == 8, 'Only apply to arrays of 8 bytes'
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, 'Format shall have 8 bytes long'
    data = struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]
    timestamp = datetime.fromtimestamp(data // 1000)  # Eliminate 3 last digits that signify milliseconds
    return timestamp  # Return date and time


def parse_bmv_timestamp3(bytes_array: bytes) -> datetime:
    '''Parses a timestamp of type 3 (Fecha y Hora con precision de milisegundos) as specified by BMV'''
    assert len(bytes_array) == 8, 'Only apply to arrays of 8 bytes'
    assert struct.calcsize(BMV_TIMESTAMP_FORMAT) == 8, 'Format length is 8 bytes'
    data = struct.unpack(BMV_TIMESTAMP_FORMAT, bytes_array)[0]
    timestamp = datetime.fromtimestamp(data // 1000)  # Eliminate 3 last digits that signify milliseconds
    timestamp = timestamp.replace(microsecond=(data % 1000) * 1000)
    return timestamp  # Return date, time and seconds with milliseconds precision.


def parse_bmv_int8(bytes_array: bytes) -> int:
    '''Parses 1 byte as an integer as specified by BMV'''
    assert len(bytes_array) == 1, 'Only apply to arrays of 1 byte'
    assert struct.calcsize(BMV_INT8_FORMAT) == 1, 'Format must have as well 1 bytes'
    return struct.unpack(BMV_INT8_FORMAT, bytes_array)[0]


def parse_bmv_int16(bytes_array: bytes) -> int:
    '''Parses 2 bytes as an integer as specified by BMV'''
    assert len(bytes_array) == 2, 'Only apply to arrays of 2 bytes'
    assert struct.calcsize(BMV_INT16_FORMAT) == 2, 'Format must have as well 2 bytes'
    return struct.unpack(BMV_INT16_FORMAT, bytes_array)[0]


def parse_bmv_int32(bytes_array: bytes) -> int:
    '''Parses 4 bytes as an integer as specified by BMV'''
    assert len(bytes_array) == 4, 'Only apply to arrays of 4 bytes'
    assert struct.calcsize(BMV_INT32_FORMAT) == 4, 'Format must have as well 4 bytes'
    return struct.unpack(BMV_INT32_FORMAT, bytes_array)[0]


def parse_bmv_int64(bytes_array: bytes) -> int:
    '''Parses 8 bytes as an integer as specified by BMV'''
    assert len(bytes_array) == 8, 'Only apply to arrays of 8 bytes'
    assert struct.calcsize(BMV_INT64_FORMAT) == 8, 'Format must have as well 4 bytes'
    return struct.unpack(BMV_INT64_FORMAT, bytes_array)[0]


def parse_bmv_precio4(bytes_array: bytes) -> float:
    '''Parses 4 bytes as a 'precio' with 4 decimal digits. As specified by BMV'''
    assert len(bytes_array) == 4, 'Only apply to arrays of 4 bytes'
    assert struct.calcsize(BMV_PRECIO4_FORMAT) == 4, 'Format must have as well 4 bytes'
    precio = float(struct.unpack(BMV_PRECIO4_FORMAT, bytes_array)[0])
    return precio / 1000.0


def parse_bmv_precio8(bytes_array: bytes) -> float:
    '''Parses 4 byte as a 'precio' with 8 decimal digits. As specified by BMV'''
    assert len(bytes_array) == 8, 'nly apply to arrays of 8 bytes'
    assert struct.calcsize(BMV_PRECIO8_FORMAT) == 8, 'Format must have as well 4 bytes'
    precio = float(struct.unpack(BMV_PRECIO8_FORMAT, bytes_array)[0])
    return precio / 100000000.0


#
# Utility assert functions
#


def assert_nonzero_float(source, field):
    assert float(source[field]) >= 0.0, f"{field} {source['field']} must be >= 0.0"


def assert_positive_integer(source, field):
    assert int(source[field]) > 0, f"{field} {source['field']} must be a positive integer."


def assert_in_catalog(source, field, catalog):
    assert source[field] in catalog, f"{field} {source['field']} unknown in the catalog."


def assert_is_valid_price(source, field):
    assert float(source[field]) >= 0.0, f"{field} must be >= 0.0 to be a valid price."


def check_message_type(bytes_array, expected_type, expected_length):
    '''message type checks JUST THE FIRST byte to figure out the type of the message'''
    assert len(bytes_array) == expected_length, f'{expected_type} has length ${len(bytes_array)} but must have {expected_length} '
    tipo_mensaje = parse_alfa(bytes_array[:1])
    assert tipo_mensaje == expected_type, f'This parsing only works for mensaje {expected_type}'
    print(f'Message type is {tipo_mensaje}')
    return tipo_mensaje


def check_catalog_type(bytes_array, expected_type, expected_length):
    '''catalog type checks the first two bytes to figure out the type of the message'''
    assert len(bytes_array) == expected_length, f'{expected_type} has length ${len(bytes_array)} but must have {expected_length} '
    tipo_mensaje = parse_alfa(bytes_array[:2])
    assert tipo_mensaje == expected_type, f'This parsing only works for catalogo {expected_type}'
    return tipo_mensaje

#
# Producto 18 Messages
#

def parse_bmv_mensaje_M(bytes_array: bytes) -> dict:
    '''Parses an array of 21 bytes as a 'mensaje M' as specified by BMV'''
    tipo_mensaje = check_message_type(bytes_array, 'M', 9)
    msg_M = {'key': None, 'tipoMensaje': tipo_mensaje,
             'numeroInstrumento': parse_bmv_int32(bytes_array[1:5]),
             'precioPromedioPonderado': parse_bmv_precio8(bytes_array[5:13]),
             'volatilidad': parse_bmv_precio8(bytes_array[13:21])
             }
    assert_positive_integer(msg_M, 'numeroInstrumento')
    assert_is_valid_price(msg_M, 'precioPromedioPonderado')
    assert_nonzero_float(msg_M, 'volatilidad')
    # Done with checks
    return msg_M


def parse_bmv_mensaje_H(bytes_array: bytes) -> dict:
    '''Parses an array of 9 bytes as a 'Mensaje H' as specified by BMV'''
    tipo_mensaje = check_message_type(bytes_array, 'H', 9)
    msg_H = {'key': None, 'tipoMensaje': tipo_mensaje,
             'numeroInstrumento': parse_bmv_int32(bytes_array[1:5]),
             'folioHecho': parse_bmv_int32(bytes_array[5:9]),
             }
    assert_positive_integer(msg_H, 'numeroInstrumento')
    assert_positive_integer(msg_H, 'folioHecho')
    return msg_H



def parse_bmv_mensaje_O(bytes_array: bytes) -> dict:
    '''Parses an array of 19 bytes as a 'Mensaje O' as specified by BMV'''
    tipo_mensaje = check_message_type(bytes_array, 'O', 19)
    msg_O = {'key': None, 'tipoMensaje': tipo_mensaje,
             'numeroInstrumento': parse_bmv_int32(bytes_array[1:5]),
             'volumen': parse_bmv_int32(bytes_array[5:9]),
             'precio': parse_bmv_precio8(bytes_array[9:17]),
             'sentido': parse_alfa(bytes_array[17:18]),
             'tipo': parse_alfa(bytes_array[18:19])
             }
    assert_positive_integer(msg_O, 'numeroInstrumento')
    assert_positive_integer(msg_O, 'volumen')
    assert_is_valid_price(msg_O, 'precio')
    BMV_SENTIDO_OPERACION = ('C', 'V')
    BMV_TIPO_OPERACION_MSG_O = ('C', 'H', 'P', 'N')
    assert_in_catalog(msg_O, 'sentido', BMV_SENTIDO_OPERACION)
    assert_in_catalog(msg_O, 'tipo', BMV_TIPO_OPERACION_MSG_O)
    return msg_O

def parse_bmv_mensaje_E(bytes_array: bytes) -> dict:
    '''Parses an array of 65 bytes as a 'Mensaje E' as specified by BMV'''
    tipo_mensaje = check_message_type(bytes_array, 'E', 65)
    msg_E = {'key': None, 'tipoMensaje': tipo_mensaje,
             'numeroInstrumento': parse_bmv_int32(bytes_array[1:5]),
             'numeroOperaciones': parse_bmv_int32(bytes_array[5:9]),
             'volumen': parse_bmv_int64(bytes_array[9:17]),
             'importe': parse_bmv_precio8(bytes_array[17:25]),
             'apertura': parse_bmv_precio8(bytes_array[25:33]),
             'maximo': parse_bmv_precio8(bytes_array[33:41]),
             'minimo': parse_bmv_precio8(bytes_array[41:49]),
             'promedio': parse_bmv_precio8(bytes_array[49:57]),
             'last': parse_bmv_precio8(bytes_array[57:65])
             }
    assert_positive_integer(msg_E, 'numeroInstrumento')
    assert_positive_integer(msg_E, 'numeroOperaciones')
    assert_positive_integer(msg_E, 'volumen')
    assert_is_valid_price(msg_E, 'importe')
    assert_is_valid_price(msg_E, 'apertura')
    assert_is_valid_price(msg_E, 'maximo')
    assert_is_valid_price(msg_E, 'minimo')
    assert_is_valid_price(msg_E, 'promedio')
    assert_is_valid_price(msg_E, 'last')
    return msg_E



def parse_bmv_mensaje_P(bytes_array: bytes) -> dict:
    '''Parses an array of 52 bytes as a 'Mensaje P' as specified by BMV'''
    tipo_mensaje = check_message_type(bytes_array, 'P', 52)
    msg_P = {'key': None, 'tipoMensaje': tipo_mensaje,
             'numeroInstrumento': parse_bmv_int32(bytes_array[1:5]),
             'horaHecho': parse_bmv_timestamp2(bytes_array[5:13]).isoformat(),
             'volumen': parse_bmv_int32(bytes_array[13:17]),
             'precio': parse_bmv_precio8(bytes_array[17:25]),
             'tipoConcertacion': parse_alfa(bytes_array[25:26]),
             'folioHecho': parse_bmv_int32(bytes_array[26:30]),
             'fijaPrecio': parse_alfa(bytes_array[30:31]) == '1',
             'tipoOperacion': parse_alfa(bytes_array[31:32]),
             'importe': parse_bmv_precio8(bytes_array[32:40]),
             'compra': parse_alfa(bytes_array[40:45]),
             'vende': parse_alfa(bytes_array[45:50]),
             'liquidacion': parse_alfa(bytes_array[50:51]),
             'indicadorSubasta': parse_alfa(bytes_array[51:52])
             }
    assert_positive_integer(msg_P, 'numeroInstrumento')
    assert_positive_integer(msg_P, 'volumen')
    assert_is_valid_price(msg_P, 'precio')
    assert_in_catalog(msg_P, 'tipoConcertacion', BMV_TIPOS_CONCERTACION)
    assert_positive_integer(msg_P, 'folioHecho')
    assert_in_catalog(msg_P, 'tipoOperacion', BMV_TIPOS_OPERACION)
    # X es nuevo
    assert_nonzero_float(msg_P, 'importe')
    assert_in_catalog(msg_P, 'liquidacion', BMV_TIPOS_LIQUIDACION)
    assert_in_catalog(msg_P, 'indicadorSubasta', BMV_TIPOS_SUBASTA) 
    # '' es nuevo.
    return msg_P


#
# Producto 40 Messages
#


def parse_bmv_catalogo_ca(bytes_array: bytes) -> dict:
    '''Parses an array of 113 bytes as a 'catalogo ca' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'ca', 113)
    cat_ca = {'key': None, 'tipoMensaje': tipo_mensaje,
             'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
             'tipoValor': parse_alfa(bytes_array[6:8]),
             'emisora': parse_alfa(bytes_array[8:15]),
             'serie': parse_alfa(bytes_array[15:21]),
             'ultimoPrecio': parse_bmv_precio8(bytes_array[21:29]),
             'PPP': parse_bmv_precio8(bytes_array[29:37]),
             'precioCierre': parse_bmv_precio8(bytes_array[37:45]),
             'fechaReferencia': parse_bmv_timestamp1(bytes_array[45:53]).isoformat(),
             'referencia': parse_alfa(bytes_array[53:55]),
             'cuponVigente': parse_bmv_int16(bytes_array[55:57]),
             'bursatilidad': parse_alfa(bytes_array[57:59]),
             'bursatilidadNumerica': parse_bmv_precio4(bytes_array[59:63]),
             'ISIN': parse_alfa(bytes_array[63:75]),
             'mercado': parse_alfa(bytes_array[75:76]),
             'valoresInscritos': parse_bmv_int64(bytes_array[76:84]),
             'importeBloques': parse_bmv_precio8(bytes_array[84:92]),
             'bolsaOrigen': parse_alfa(bytes_array[92:93]),
             # There's a filler of 20 bytes, from 93 to 113 that we ignore.
             }
    assert_positive_integer(cat_ca, 'numeroInstrumento')
    assert_in_catalog(cat_ca,'tipoValor', BMV_TIPOS_VALOR)
    assert_is_valid_price(cat_ca, 'ultimoPrecio')
    assert_is_valid_price(cat_ca, 'PPP')
    assert_is_valid_price(cat_ca, 'precioCierre')
    # assert cat_ca['fechaReferencia'] is True, 'ToDo'
    assert_in_catalog(cat_ca,'referencia', BMV_CATALOGO_REFERENCIA)
    assert_positive_integer(cat_ca, 'cuponVigente')
    assert_in_catalog(cat_ca,'bursatilidad', BMV_BURSATILIDAD)
    assert_nonzero_float(cat_ca, 'bursatilidadNumerica')
    assert_in_catalog(cat_ca,'mercado', BMV_MERCADOS)
    assert_positive_integer(cat_ca, 'valoresInscritos')
    assert_nonzero_float(cat_ca, 'importeBloques')
    assert cat_ca['bolsaOrigen'] != None, 'La Bolsa origen debe estar definida.'
    assert_in_catalog(cat_ca,'bolsaOrigen', BMV_BOLSA_ORIGEN)
    return cat_ca


def parse_bmv_catalogo_ce(bytes_array:bytes) -> dict:
    '''Parses an array of 103 bytes as a 'catalogo ce' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'ce', 103)
    cat_ce = {'key': None, 'tipoMensaje': tipo_mensaje,
              'numeroTrac': parse_bmv_int32(bytes_array[2:6]),
              'nombreTrac': parse_alfa(bytes_array[6:14]),
              'emisoraSubyascente': parse_alfa(bytes_array[14:21]),
              'serieSubyacente': parse_alfa(bytes_array[21:27]),
              'titulos': parse_bmv_int64(bytes_array[27:35]),
              'titulosExcluidos': parse_bmv_int64(bytes_array[35:43]),
              'precio': parse_bmv_precio8(bytes_array[43:51]),
              'componenteEfectivo': parse_bmv_precio8(bytes_array[51:59]),
              'valorExcluido': parse_bmv_precio8(bytes_array[59:67]),
              'numeroCertificados': parse_bmv_int64(bytes_array[67:75]),
              'precioTeorico': parse_bmv_precio8(bytes_array[75:83]),
    }
    assert_positive_integer(cat_ce, 'numeroTrac')
    assert_is_valid_price(cat_ce, 'titulos')
    assert_is_valid_price(cat_ce, 'titulosExcluidos')
    assert_is_valid_price(cat_ce, 'precio')
    assert_is_valid_price(cat_ce, 'componenteEfectivo')
    assert_is_valid_price(cat_ce, 'valorExcluido')
    assert_positive_integer(cat_ce, 'numeroCertificados')
    assert_is_valid_price(cat_ce, 'precioTeorico')
    return cat_ce
              
              
              

def parse_bmv_catalogo_cc(bytes_array:bytes) -> dict:
    '''Parses an array of 89 bytes as a 'catalogo cc' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'cc', 89)
    cat_cc = {'key': None, 'tipoMensaje': tipo_mensaje,
            'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
            'tipoValor': parse_alfa(bytes_array[6:8]),
            'emisora': parse_alfa(bytes_array[8:15]),
            'serie': parse_alfa(bytes_array[15:21]),
            'tipoWarrant': parse_alfa(bytes_array[21:22]),
            'fechaVencimiento': parse_bmv_timestamp2(bytes_array[22:30]).isoformat(),
            'precioEjercicio': parse_bmv_precio8(bytes_array[30:38]),
            'precioReferencia': parse_bmv_precio8(bytes_array[38:46]),
            'fechaReferencia': parse_bmv_timestamp2(bytes_array[46:54]).isoformat(),
            'referencia': parse_alfa(bytes_array[54:56]),
            'ISIN': parse_alfa(bytes_array[56:68]),
            'bolsaOrigen': parse_alfa(bytes_array[68:69]),
            # There's a filler of 20 bytes, from 69 to 89 that we ignore.
    }
    assert_positive_integer(cat_cc, 'numeroInstrumento')
    assert_in_catalog(cat_cc,'tipoValor', BMV_TIPOS_VALOR)
    assert_in_catalog(cat_cc,'tipoWarrant', BMV_TIPOS_WARRANT)
    assert_is_valid_price(cat_cc, 'precioEjercicio')
    assert_is_valid_price(cat_cc, 'precioReferencia')
    assert_in_catalog(cat_cc,'bolsaOrigen', BMV_TIPOS_BOLSA_ORIGEN)
    return cat_cc


def parse_bmv_catalogo_cf(bytes_array:bytes) -> dict:
    '''Parses an array of 100 bytes as a 'catalogo cf' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'cf', 100)
    cat_cf = {'key': None, 'tipoMensaje': tipo_mensaje,
              'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
              'tipoValor': parse_alfa(bytes_array[6:8]),
              'emisora': parse_alfa(bytes_array[8:15]),
              'serie': parse_alfa(bytes_array[15:21]),
              'sector': parse_alfa(bytes_array[21:22]),
              'subsector': parse_alfa(bytes_array[22:23]),
              'ramo': parse_alfa(bytes_array[23:24]),
              'subramo': parse_alfa(bytes_array[24:25]),
              'operadora': parse_alfa(bytes_array[25:35]),
              'precioReferencia': parse_bmv_precio8(bytes_array[35:43]),
              'fechaReferencia': parse_bmv_timestamp1(bytes_array[43:51]).isoformat(),
              'referencia': parse_alfa(bytes_array[51:53]),
              'ISIN': parse_alfa(bytes_array[53:65]),
              'calificacion': parse_alfa(bytes_array[65:80]),
              # FIlling of 20 from 80 to 100
    }
    assert_positive_integer(cat_cf, 'numeroInstrumento')
    assert_in_catalog(cat_cf,'tipoValor', BMV_TIPOS_VALOR)
    assert_is_valid_price(cat_cf, 'precioReferencia')
    return cat_cf
    
    

def parse_bmv_catalogo_cb(bytes_array:bytes) -> dict:
    '''Parses an array of 126 bytes as a 'catalogo cb' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'cb', 126)
    cat_cb = {'key': None, 'tipoMensaje': tipo_mensaje,
              'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
              'tipoValor': parse_alfa(bytes_array[6:8]),
              'emisora': parse_alfa(bytes_array[8:15]),
              'emision': parse_alfa(bytes_array[15:21]),
              'fechaEmision': parse_bmv_timestamp1(bytes_array[21:29]).isoformat(),
              'fechaVencimiento': parse_bmv_timestamp1(bytes_array[29:37]).isoformat(),
              'precioOtasaReferencia': parse_bmv_precio8(bytes_array[37:45]),
              'fechaReferencia': parse_bmv_timestamp1(bytes_array[45:53]).isoformat(),
              'referencia': parse_alfa(bytes_array[53:55]),
              'diasPlazo': parse_bmv_int16(bytes_array[55:57]),
              'cuponOperiodo': parse_bmv_int16(bytes_array[57:59]),
              'ISIN': parse_alfa(bytes_array[59:71]),
              'mercado': parse_alfa(bytes_array[71:72]),
              'valorNominalActual': parse_bmv_precio8(bytes_array[72:80]),
              'valorNominalOriginal': parse_bmv_precio8(bytes_array[80:88]),
              'accionesEnCirculacion': parse_bmv_int64(bytes_array[88:96]),
              'montoColocado': parse_bmv_int64(bytes_array[96:104]),
              'operaTasaPrecio': parse_alfa(bytes_array[104:105]),
              'bolsaOrigen': parse_alfa(bytes_array[105:106]),
              # Filler of 20 from 106 to 126
    }
    assert_positive_integer(cat_cb, 'numeroInstrumento')
    assert_in_catalog(cat_cb,'tipoValor', BMV_TIPOS_VALOR)
    assert_is_valid_price(cat_cb, 'precioOtasaReferencia')
    assert_is_valid_price(cat_cb, 'valorNominalActual')
    assert_is_valid_price(cat_cb, 'valorNominalOriginal')
    assert_positive_integer(cat_cb, 'accionesEnCirculacion')
    assert_positive_integer(cat_cb, 'montoColocado')
    assert_in_catalog(cat_cb,'operaTasaPrecio', BMV_TIPOS_OPERA_TASA_PRECIO)
    assert_in_catalog(cat_cb,'bolsaOrigen', BMV_TIPOS_BOLSA_ORIGEN)
    return cat_cb
           
    
def parse_bmv_catalogo_cy(bytes_array:bytes) -> dict:
    '''Parses an array of 65 bytes as a 'catalogo cy' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'cy', 65)
    cat_cy = {'key': None, 'tipoMensaje': tipo_mensaje,
              'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
              'emisora': parse_alfa(bytes_array[6:13]),
              'serie': parse_alfa(bytes_array[13:19]),
              'tipoValor': parse_alfa(bytes_array[19:21]),
              'emisoraSubyacente': parse_alfa(bytes_array[21:28]),
              'serieSubyacente': parse_alfa(bytes_array[28:34]),
              'tipoValorSubyacente': parse_alfa(bytes_array[34:36]),
              'numeroValoresInscritos': parse_bmv_int64(bytes_array[36:44]),
              'bolsaOrigen': parse_alfa(bytes_array[44:45]),
              # Filler of 20 from 45 to 65
    }
    assert_positive_integer(cat_cy, 'numeroInstrumento')
    assert_in_catalog(cat_cy,'tipoValor', BMV_TIPOS_VALOR)
    assert_in_catalog(cat_cy,'tipoValorSubyacente', BMV_TIPOS_VALOR)
    assert_positive_integer(cat_cy, 'numeroValoresInscritos')
    assert_in_catalog(cat_cy,'bolsaOrigen', BMV_TIPOS_BOLSA_ORIGEN)
    return cat_cy
           
    
def parse_bmv_catalogo_cd(bytes_array:bytes) -> dict:
    '''Parses an array of 115 bytes as a 'catalogo cd' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'cd', 115)
    cat_cd = {'key': None, 'tipoMensaje': tipo_mensaje,
              'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
              'tipoValor': parse_alfa(bytes_array[6:8]),
              'clase': parse_alfa(bytes_array[8:15]),
              'vencimiento': parse_alfa(bytes_array[15:21]),
              'tipoOpcion': parse_alfa(bytes_array[21:22]),
              'precioEjercicio': parse_bmv_precio8(bytes_array[22:30]),
              'puja': parse_bmv_precio8(bytes_array[30:38]),
              'precioLiquidacionDiaAnterior': parse_bmv_precio8(bytes_array[38:46]),
              'ultimaFechaOperacion': parse_bmv_timestamp1(bytes_array[46:54]).isoformat(),
              'fechaVencimiento': parse_bmv_timestamp1(bytes_array[54:62]).isoformat(),
              'contratosAbiertos': parse_bmv_int32(bytes_array[62:66]),
              'tamanoContrato': parse_bmv_int32(bytes_array[66:70]),
              'codigoProducto': parse_alfa(bytes_array[70:82]), 
              'vencimientoDiario': parse_alfa(bytes_array[82:83]),
              'clavePrecioEjercicio': parse_alfa(bytes_array[83:89]),
              'codigoCFI': parse_alfa(bytes_array[89:95]),
              # Filler from 95 to 115
    }
    assert_positive_integer(cat_cd, 'numeroInstrumento')
    assert_in_catalog(cat_cd,'tipoValor', BMV_TIPOS_VALOR)
    assert_in_catalog(cat_cd,'tipoOpcion', BMV_TIPOS_OPCION)
    assert_is_valid_price(cat_cd, 'precioEjercicio')
    assert_is_valid_price(cat_cd, 'puja')
    assert_is_valid_price(cat_cd, 'precioLiquidacionDiaAnterior')
    assert_positive_integer(cat_cd, 'contratosAbiertos')
    assert_positive_integer(cat_cd, 'tamanoContrato')
    assert_in_catalog(cat_cd,'vencimientoDiario', BMV_TIPOS_VENCIMIENTO_DIARIO)
    return cat_cd



def parse_bmv_catalogo_cg(bytes_array:bytes) -> dict:
    '''Parses an array of 76 bytes as a 'catalogo cg' as specified by BMV'''
    tipo_mensaje = check_catalog_type(bytes_array, 'cg', 76)
    cat_cg = {'key': None, 'tipoMensaje': tipo_mensaje,
              'numeroInstrumento': parse_bmv_int32(bytes_array[2:6]),
              'tipoValor': parse_alfa(bytes_array[6:8]),
              'clase': parse_alfa(bytes_array[8:15]),
              'vencimiento': parse_alfa(bytes_array[15:21]),
              'tipoEstrategia': parse_alfa(bytes_array[21:22]),
              'puja': parse_bmv_precio8(bytes_array[22:30]),
              'ultimaFechaOperacion': parse_bmv_timestamp1(bytes_array[30:38]).isoformat(),
              'fechaVencimiento': parse_bmv_timestamp1(bytes_array[38:46]).isoformat(),
              'identificadorPataCorta': parse_bmv_int32(bytes_array[46:50]),
              'identificadorPataLarga': parse_bmv_int32(bytes_array[50:54]),
              'periodicidad': parse_bmv_int8(bytes_array[54:55]),
              'numeroVencimientos': parse_bmv_int8(bytes_array[55:56]),
              # Filler of 20 from 56 to 76
    }
    assert_positive_integer(cat_cg, 'numeroInstrumento')
    assert_in_catalog(cat_cg,'tipoValor', BMV_TIPOS_VALOR)
    assert_in_catalog(cat_cg,'tipoEstrategia', BMV_TIPOS_ESTRATEGIA)   
    assert_is_valid_price(cat_cg, 'puja')
    return cat_cg


def parse_by_message_type(grupo_market_data: int, to_parse: bytes) -> dict|None:
    '''Given a tipo_mensaje we assume matches the bytes array, we call the appropiate parsing function
    Returns:
        mensaje: A dictionary with the parsed fields
    '''
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
            mensaje = parse_bmv_catalogo_ca(to_parse)
        case 'cb':
            mensaje = parse_bmv_catalogo_cb(to_parse)
        case 'cc':
            mensaje = parse_bmv_catalogo_cc(to_parse)
        case 'cd':
            mensaje = parse_bmv_catalogo_cd(to_parse)
        case 'ce':
            mensaje = parse_bmv_catalogo_ce(to_parse)
        case 'cf':
            mensaje = parse_bmv_catalogo_cf(to_parse)
        case 'cg':
            mensaje = parse_bmv_catalogo_cg(to_parse)
        case 'cy':
            mensaje = parse_bmv_catalogo_cy(to_parse)
        
            
    return mensaje       
        

def parse_bmv_udp_packet(packet_data: bytes) -> dict:
    '''Parses an udp packet as containing a header and 1 or more messages, as specified by BMV'''
    paquete = {}
    longitud:int = parse_bmv_int16(packet_data[:2])
    assert longitud == len(packet_data), f'Longitud {longitud} must be equal to the packet size {len(packet_data)})'
    paquete['longitud'] = longitud
    total_mensajes = parse_bmv_int8(packet_data[2:3])
    assert total_mensajes >= 0, 'We need a 0 or positive number'
    paquete['total_mensajes'] = total_mensajes
    paquete['grupo_market_data'] = parse_bmv_int8(packet_data[3:4])
    assert paquete['grupo_market_data'] in (18,40), 'We only deal with grupo 18 or grupo 40 BMV messages'
    paquete['sesion'] = parse_bmv_int8(packet_data[4:5])
    # http://tecnologia.bmv.com.mx:6503/especificacion/multicast/msg/structure/catalogs.html#cat_grupo_market_data
    assert 0 <= paquete['sesion'] <= 40, 'La sesion debe estar entre 0, y 40'
    paquete['secuencia'] = secuencia = parse_bmv_int32(packet_data[5:9])
    assert 0 <= paquete['secuencia'], 'La secuencia debe ser mayor a cero.'
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
    '''Parses a complete cap file assuming it has only udp packets from BMV 'producto 18' or 'producto 40''''
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
            print(f'Unexpected error on sequence {last_sequence}, trying to continue...')
            last_sequence = None
            continue
    print(f'Last found sequence is {last_sequence}')
    return counter_msgs

def process_bmv_udp_packet(output_file, counter_msgs, last_sequence, udp_packet) -> int:
    paquete = parse_bmv_udp_packet(udp_packet.data)
    if not last_sequence:
        last_sequence = paquete['secuencia'] + paquete['total_mensajes']
        print(f'Primera secuencia es {last_sequence}')
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
