"""Compare two  bmv producto json files, one is the expected output, the other is the actual output.
Whenever you find a timestamp, compare it with the dateutil.parser library functions, 
otherwise, use asserts to compare the two.
"""

import json
from  bmv_utils.compare import *
import sys

def compare_bmv_producto_jsons(expected_output, actual_output):
    is_equal = True
    with open(expected_output, 'r') as expected_file:
        with open(actual_output, 'r') as actual_file:
            '''Read each line from expected_output and actual_output, compare them.'''
            for expected_line in expected_file:
                actual_line = actual_file.readline()
                '''Compare each field in the line, if it is a timestamp, use dateutil.parser to parse it and compare.'''
                expected_json = json.loads(expected_line)
                try:
                    actual_json = json.loads(actual_line)
                    is_equal = compare_bmv_message_json(expected_json, actual_json) and is_equal
                except AssertionError as e:
                    print(f"Assertion failed: {e}")
                    is_equal = False
                except:
                    print(f"{expected_json['key']}: An error ocurred")
                    is_equal = False
    return is_equal


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('[Error] Usage: compare_bmv_jsons.py expected.json actual.json')
        print('Compare two json files, one is the expected output, the other is the actual output.')
        sys.exit(1)
    expected_output = sys.argv[1]
    actual_output = sys.argv[2]
    is_equal = compare_bmv_producto_jsons(expected_output, actual_output)
    print(f"{expected_output} is equal to {actual_output}? {is_equal}")
            