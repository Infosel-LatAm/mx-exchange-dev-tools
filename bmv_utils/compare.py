#! /usr/bin/env python
"""Compare two json files, one is the expected output, the other is the actual output.
Whenever you find a timestamp, compare it with the dateutil.parser library functions, use asserts to compare the two.
"""

import sys
import dateutil.parser
import json

def compare_bmv_producto_jsons(expected_output, actual_output):
    """Compare two  bmv producto json files, one is the expected output, the other is the actual output.
    Whenever you find a timestamp, compare it with the dateutil.parser library functions, 
    otherwise, use asserts to compare the two.
    """
    with open(expected_output, 'r') as expected_file:
        with open(actual_output, 'r') as actual_file:
            '''Read each line from expected_output and actual_output, compare them.'''
            for expected_line in expected_file:
                actual_line = actual_file.readline()
                '''Compare each field in the line, if it is a timestamp, use dateutil.parser to parse it and compare.'''
                expected_json = json.loads(expected_line)
                try:
                    actual_json = json.loads(actual_line)
                    # Key is important because is the way to identfy the message received from BMV
                    assert 'key' in actual_json, f"'key' not in actual_output file: {actual_output}"
                    # tipoMensaje is important because is the way we can identify the type of message received from BMV and its fields.
                    assert 'tipoMensaje' in actual_json, f"'tipoMensaje' not in actual_output file: {actual_output}"
                    if expected_json['tipoMensaje'] == 'ca':
                        compare_bmv_catalogo_ca(expected_json, actual_json)
                    elif expected_json['tipoMensaje'] == 'P':
                        compare_bmv_message_P(expected_json, actual_json)
                    else:
                        print(f"{expected_json['key']}: Unknown message type: {expected_json['tipoMensaje']}")
                except AssertionError as e:
                    print(f"Assertion failed: {e}")
                except:
                    print(f"{expected_json['key']}: An error ocurred")



def assert_eq_json_field(expected_json: dict, actual_json: dict, field: str) -> None:
    assert expected_json[field] == actual_json[field], f"key {expected_json['key']}: {field} {expected_json[field]} != {actual_json[field]}"


def compare_bmv_mensaje_M(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'precioPromedioPonderado')
    assert_eq_json_field(expected, actual, 'volatilidad')
    return True

def compare_bmv_mensaje_H(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'folioHecho')
    return True
    
def compare_bmv_mensaje_O(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'volumen')
    assert_eq_json_field(expected, actual, 'precio')
    assert_eq_json_field(expected, actual, 'sentido')
    assert_eq_json_field(expected, actual, 'tipo')
    return True
    
def compare_bmv_mensaje_E(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'numeroOperaciones')
    assert_eq_json_field(expected, actual, 'volumen')
    assert_eq_json_field(expected, actual, 'importe')
    assert_eq_json_field(expected, actual, 'apertura')
    assert_eq_json_field(expected, actual, 'maximo')
    assert_eq_json_field(expected, actual, 'minimo')
    assert_eq_json_field(expected, actual, 'promedio')
    assert_eq_json_field(expected, actual, 'last')
    return True

def compare_bmv_message_P(expected: dict, actual: dict) -> bool:
    '''Compare two p messages, one is the expected output, the other is the actual output.'''
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'fechaHora')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert dateutil.parser.isoparse(expected['horaHecho']) == dateutil.parser.isoparse(actual['horaHecho']), f"key {key}: horaHecho {expected['horaHecho']} != {actual['horaHecho']}"
    assert_eq_json_field(expected, actual, 'volumen')
    assert_eq_json_field(expected, actual, 'precio')
    assert_eq_json_field(expected, actual, 'tipoConcertacion')
    assert_eq_json_field(expected, actual, 'folioHecho')
    assert_eq_json_field(expected, actual, 'fijaPrecio')
    assert_eq_json_field(expected, actual, 'tipoOperacion')
    assert_eq_json_field(expected, actual, 'importe')
    assert_eq_json_field(expected, actual, 'compra')
    assert_eq_json_field(expected, actual, 'vende')
    assert_eq_json_field(expected, actual, 'liquidacion')
    assert_eq_json_field(expected, actual, 'indicadorSubasta')
    return True
    


def compare_bmv_catalogo_ca(expected: dict, actual: dict) -> bool:
    '''Compare two ca messages, one is the expected output, the other is the actual output.'''
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'emisora')
    assert_eq_json_field(expected, actual, 'serie')
    assert_eq_json_field(expected, actual, 'ultimoPrecio')
    assert_eq_json_field(expected, actual, 'PPP')
    assert_eq_json_field(expected, actual, 'precioCierre')
    assert dateutil.parser.isoparse(expected['fechaReferencia']) == dateutil.parser.isoparse(actual['fechaReferencia']), f"key {key}: fechaReferencia {expected['tipoMensaje']} != {actual['tipoMensaje']}"
    assert_eq_json_field(expected, actual, 'referencia')
    assert_eq_json_field(expected, actual, 'cuponVigente')
    assert_eq_json_field(expected, actual, 'bursatilidad')
    assert_eq_json_field(expected, actual, 'bursatilidadNumerica')
    assert_eq_json_field(expected, actual, 'ISIN')
    assert_eq_json_field(expected, actual, 'mercado')
    assert_eq_json_field(expected, actual, 'valoresInscritos')
    assert_eq_json_field(expected, actual, 'importeBloques')
    assert_eq_json_field(expected, actual, 'bolsaOrigen')
    return True
    

def compare_bmv_catalogo_ce(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'numeroTrac')
    assert_eq_json_field(expected, actual, 'nombreTrac')
    assert_eq_json_field(expected, actual, 'emisoraSubyascente')
    assert_eq_json_field(expected, actual, 'serieSubyacente')
    assert_eq_json_field(expected, actual, 'titulos')
    assert_eq_json_field(expected, actual, 'titulosExcluidos')
    assert_eq_json_field(expected, actual, 'precio')
    assert_eq_json_field(expected, actual, 'componenteEfectivo')
    assert_eq_json_field(expected, actual, 'valorExcluido')
    assert_eq_json_field(expected, actual, 'numeroCertificados')
    assert_eq_json_field(expected, actual, 'precioTeorico')
    return True


def compare_bmv_catalogo_cf(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'emisora')
    assert_eq_json_field(expected, actual, 'serie')
    assert_eq_json_field(expected, actual, 'sector')
    assert_eq_json_field(expected, actual, 'subsector')
    assert_eq_json_field(expected, actual, 'ramo')
    assert_eq_json_field(expected, actual, 'subramo')
    assert_eq_json_field(expected, actual, 'operadora')
    assert_eq_json_field(expected, actual, 'precioReferencia')
    assert_eq_json_field(expected, actual, 'fechaReferencia')
    assert_eq_json_field(expected, actual, 'referencia')
    assert_eq_json_field(expected, actual, 'ISIN')
    assert_eq_json_field(expected, actual, 'calificacion')
    return True



def compare_bmv_catalogo_cc(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'emisora')
    assert_eq_json_field(expected, actual, 'serie')
    assert_eq_json_field(expected, actual, 'tipoWarrant')
    assert_eq_json_field(expected, actual, 'fechaVencimiento')
    assert_eq_json_field(expected, actual, 'precioEjercicio')
    assert_eq_json_field(expected, actual, 'precioReferencia')
    assert_eq_json_field(expected, actual, 'fechaReferencia')
    assert_eq_json_field(expected, actual, 'referencia')
    assert_eq_json_field(expected, actual, 'ISIN')
    assert_eq_json_field(expected, actual, 'bolsaOrigen')
    return True



def compare_bmv_catalogo_cb(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'emisora')
    assert_eq_json_field(expected, actual, 'emision')
    assert_eq_json_field(expected, actual, 'fechaEmision')
    assert_eq_json_field(expected, actual, 'fechaVencimiento')
    assert_eq_json_field(expected, actual, 'precioOtasaReferencia')
    assert_eq_json_field(expected, actual, 'fechaReferencia')
    assert_eq_json_field(expected, actual, 'referencia')
    assert_eq_json_field(expected, actual, 'diasPlazo')
    assert_eq_json_field(expected, actual, 'cuponOperiodo')
    assert_eq_json_field(expected, actual, 'ISIN')
    assert_eq_json_field(expected, actual, 'mercado')
    assert_eq_json_field(expected, actual, 'valorNominalActual')
    assert_eq_json_field(expected, actual, 'valorNominalOriginal')
    assert_eq_json_field(expected, actual, 'accionesEnCirculacion')
    assert_eq_json_field(expected, actual, 'montoColocado')
    assert_eq_json_field(expected, actual, 'operaTasaPrecio')
    assert_eq_json_field(expected, actual, 'bolsaOrigen')
    return True


def compare_bmv_catalogo_cy(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'emisora')
    assert_eq_json_field(expected, actual, 'serie')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'emisoraSubyacente')
    assert_eq_json_field(expected, actual, 'serieSubyacente')
    assert_eq_json_field(expected, actual, 'tipoValorSubyacente')
    assert_eq_json_field(expected, actual, 'numeroValoresInscritos')
    assert_eq_json_field(expected, actual, 'bolsaOrigen')
    return True
        
    
def compare_bmv_catalogo_cd(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'clase')
    assert_eq_json_field(expected, actual, 'vencimiento')
    assert_eq_json_field(expected, actual, 'tipoOpcion')
    assert_eq_json_field(expected, actual, 'precioEjercicio')
    assert_eq_json_field(expected, actual, 'puja')
    assert_eq_json_field(expected, actual, 'precioLiquidacionDiaAnterior')
    assert_eq_json_field(expected, actual, 'ultimaFechaOperacion')
    assert_eq_json_field(expected, actual, 'fechaVencimiento')
    assert_eq_json_field(expected, actual, 'contratosAbiertos')
    assert_eq_json_field(expected, actual, 'tamanoContrato')
    assert_eq_json_field(expected, actual, 'codigoProducto')
    assert_eq_json_field(expected, actual, 'vencimientoDiario')
    assert_eq_json_field(expected, actual, 'clavePrecioEjercicio')
    assert_eq_json_field(expected, actual, 'codigoCFI')
    return True



def compare_bmv_catalogo_cg(expected: dict, actual: dict) -> bool:
    key = actual['key']
    assert_eq_json_field(expected, actual, 'key')
    assert_eq_json_field(expected, actual, 'tipoMensaje')
    assert_eq_json_field(expected, actual, 'numeroInstrumento')
    assert_eq_json_field(expected, actual, 'tipoValor')
    assert_eq_json_field(expected, actual, 'clase')
    assert_eq_json_field(expected, actual, 'vencimiento')
    assert_eq_json_field(expected, actual, 'tipoEstrategia')
    assert_eq_json_field(expected, actual, 'puja')
    assert_eq_json_field(expected, actual, 'ultimaFechaOperacion')
    assert_eq_json_field(expected, actual, 'fechaVencimiento')
    assert_eq_json_field(expected, actual, 'identificadorPataCorta')
    assert_eq_json_field(expected, actual, 'identificadorPataLarga')
    assert_eq_json_field(expected, actual, 'periodicidad')
    assert_eq_json_field(expected, actual, 'numeroVencimientos')
    return True



if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('[Error] Usage: compare_producto40_json.py expected.json actual.json')
        print('Compare two json files, one is the expected output, the other is the actual output.')
        sys.exit(1)
    expected_output = sys.argv[1]
    actual_output = sys.argv[2]
    compare_bmv_producto_jsons(expected_output, actual_output)
            
            
    