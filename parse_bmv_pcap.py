#! /usr/bin/env python
"""
Reads a pcap file from BMV and generates messages inside the pcap file in json format.
"""
import sys
import bmv_utils.parse

# Note on networking info.
# This doesn't deal with ETH_TYPE_IP6, but is possible.
#


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('[Error] Usage: parse_bmv_pcap.py [file.pcap] [output.json]')
        sys.exit(1)
    pcap_filename = sys.argv[1]
    json_output_filename = sys.argv[2]
    counter_msgs = bmv_utils.parse.parse_bmv_pcap_file(open(pcap_filename, 'rb'), open(json_output_filename, 'wt'))
    for key in counter_msgs:
        counter_msgs[key]['avg size'] = counter_msgs[key]['bytes'] / counter_msgs[key]['total']
    print(counter_msgs)
