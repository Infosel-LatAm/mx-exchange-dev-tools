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
                try:
                    expected_json = json.loads(expected_line)
                    actual_json = json.loads(actual_line)
                    # Key is important because is the way to identfy the message received from BMV
                    assert 'key' in actual_json, f"'key' not in actual_output file: {actual_output}"
                    # tipoMensaje is important because is the way we can identify the type of message received from BMV and its fields.
                    assert 'tipoMensaje' in actual_json, f"'tipoMensaje' not in actual_output file: {actual_output}"
                    if expected_json['tipoMensaje'] == 'ca':
                        compare_ca_messages(expected_json, actual_json)
                    elif expected_json['tipoMensaje'] == 'P':
                        compare_ca_messages(expected_json, actual_json)
                    else:
                        print(f"{expected_json['key']}: Unknown message type: {expected_json['tipoMensaje']}")
                except AssertionError as e:
                    print(f"Assertion failed: {e}")
                except:
                    print(f"{expected_json['key']}: An error ocurred")



def assert_eq_json_field(expected_json: dict, actual_json: dict, field: str) -> None:
    assert expected_json[field] == actual_json[field], f"key {expected_json['key']}: {field} {expected_json[field]} != {actual_json[field]}"


def compare_ca_messages(expected: dict, actual: dict) -> None:
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

def compare_cb_messages(expected: dict, actual: dict) -> None:
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





def compare_p_message(expected: dict, actual: dict) -> None:
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
    
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('[Error] Usage: compare_producto40_json.py expected.json actual.json')
        print('Compare two json files, one is the expected output, the other is the actual output.')
        sys.exit(1)
    expected_output = sys.argv[1]
    actual_output = sys.argv[2]
    compare_bmv_producto_jsons(expected_output, actual_output)
            
            
    
