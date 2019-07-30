#!/usr/bin/env python

#imports go here
import sys
import json
import pandas as pd
import pprint
import os


outdir  = '/path/to/output/directory'

def main():
    input_json_file =  sys.argv[1]
    parse_json(input_json_file)

def isjsonline(line):
    if '{' in line and '}' in line:
        return True

def fixit(line):
    index = line.find("{")
    fixed = line[index:]
    f2 = fixed.replace('"modelName":[','"modelName","x":[')
    return f2


def parse_json(input_json):
    name = os.path.basename(input_json)
    with open(input_json) as fin:
        df_vir = pd.DataFrame()
        for raw_line in fin:
            if isjsonline(raw_line):
                fixed_json = fixit(raw_line)
                data = json.loads(fixed_json)
                if 'str3//outData' not in data['str1'][1]['str2']:
                    continue
                else:
                    jinput = data['str1'][1]['str2']['str3']          
                    vi_df = pd.DataFrame.from_dict(jinput)
                    cval_name = vi_df['varName']
                    shu_df2 = vi_df['inVal']
                    shu_df3 = pd.DataFrame.from_dict(shu_df2)
                    shu_df3 = shu_df3.T
                    shu_df3.columns = cval_name
                    time_one = data['str1'][0]['str2_out']['INPUT']['PARS']['h_event']['timestamp_one']
                    time_two =  data['str1'][0]['str2_in']['INPUT']['PARS']['timestamp_two']  
                    id = data['str1'][1]['str2_out']['id_char']
                    warn = ','.join(data['str1'][1]['str2_out']['warnings'])
                    shu_df3['time_one'] = time_one
                    shu_df3['time_two'] = time_two
                    shu_df3['id'] = id
                    shu_df3['warnings'] = warn
                    shu_df3['probability_score'] = data['str1'][1]['str2_out']['probability_score']
                    shu_df3['predict'] = data['str1'][1]['str2_out']['predict']
                    vi = vi_df['varName'].map(str) + '(' + vi_df['inVal'].map(str) + ',' + vi_df['varImp'].map(str) + ')'
                    vidat = pd.DataFrame.from_dict(vi)
                    vidat = vidat.T
                    vidat.columns = 'rank_' + vi_df['varImpRank'].map(str)
                    #print(raw_line)
                    vi_shudat = pd.concat([shu_df3.reset_index(drop=True), vidat], axis=1)
                    df_vir = pd.concat([df_vir, vi_shudat])
        #print(df_vir.head(10))
        outfile = outdir + name + '_py.csv'
        df_vir.to_csv(outfile, encoding='utf-8', index=False)                           
    
if __name__ == "__main__":
    main()
