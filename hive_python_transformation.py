#!/usr/bin/python
"""
Author: Hua Wang
"""
import sys
from operator import itemgetter
import pytz
from datetime import datetime

def stripLine(line):
    """strip and remove brackets,braces"""
    line = line.strip()
    line = str(line).replace("[", '').replace(']', '').replace('{', '').replace('},', ';').replace('}', '').replace('"','')
    return line

def buildListOfDict (some_string):
    """Take string that was array of structs in Hive and build into list of dict in python"""
    arr = []
    some_list = some_string.split(';')
    for i in range(len(some_list)):
        arr.append(dict(item.split(":") for item in some_list[i].split(",")))
    return arr

def convert_to_date (timestamp_ms):
    """Take a string of UTC timestamp and convert into date in new york timezone"""
    ts = long(timestamp_ms)/1e3
    utc_tz = pytz.timezone('UTC')
    utc_dt = utc_tz.localize(datetime.utcfromtimestamp(ts))
    ny_tz = pytz.timezone('America/New_York')
    ny_dt = ny_tz.normalize(utc_dt.astimezone(ny_tz)).strftime('%Y-%m-%d %H:%M:%S')
    return ny_dt

def generate_output(path_list,conversion_type):
    ts_first_touch = convert_to_date(path[0]['ts'])
    ts_conversion = convert_to_date(path[-1]['ts'])
    device_first_touch = path[0]['device_type']
    device_last_touch = path[-2]['device_type']
    device_conversion = path[-1]['device_type']
    conversion_pixel_id = path[-1]['pixel']
    creative_id = path[0]['creative_id']
    path_length = len(path)
    # compute touches by device
    desktop_count,tablet_count,mobile_count = 0,0,0
    for i in range(len(path_list)-1):
        if path_list[i]['device_type'] == 'Desktop':
            desktop_count += 1
        elif path_list[i]['device_type'] == 'Tablet':
            tablet_count += 1
        elif path_list[i]['device_type'] == 'Mobile':
            mobile_count += 1

    output_list = [ts_first_touch, device_first_touch, str(creative_id), str(conversion_pixel_id), ts_conversion, device_last_touch, device_conversion,str(path_length), str(desktop_count), str(tablet_count), str(mobile_count),conversion_type]
    return output_list

# starts streaming
if __name__ == '__main__':
    #with open ('cross_sample.txt') as f:
        #for line in f:
    for line in sys.stdin:
            strip_line = stripLine(line)

            #provide input fields here - in the actual order
            ip,imp,pf,click = strip_line.split('\t')

            #transform hive complex fields into python list of dicts
            imp_dict_list = buildListOfDict(imp)
            sorted_imp = sorted(imp_dict_list, key=itemgetter('ts'))
            pf_dict_list = buildListOfDict(pf)
            sorted_pf = sorted(pf_dict_list, key=itemgetter('ts'), reverse=True)
            # first impression and last pf
            last_pf_ts = long(sorted_pf[0]['ts'])
            first_imp_ts = long(sorted_imp[0]['ts'])

            # determine conversion type
            conversion_type = 'View-Through'
            if len(click) > 1:
                clk_dict_list = buildListOfDict(click)
                sorted_clk = sorted(clk_dict_list, key=itemgetter('ts'))
                first_clk_ts = long(sorted_clk[0]['ts'])
                if first_clk_ts < last_pf_ts:
                    conversion_type = 'Click-Through' #to make this hack simple

            # compare the pixel fire and impression for each ID and create attribution path
            if last_pf_ts > first_imp_ts: #conversions can be attributed to earlier impression(s)
                for p in sorted_pf:
                    path = []
                    for i in sorted_imp:
                        if long(p['ts']) > long(i['ts']):
                            path.append(i)
                    if len(path) > 0:
                        path.append(p)
                        output = generate_output(path, conversion_type)
                        print '\t'.join(output)
                    else:
                        del path






































