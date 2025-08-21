import json 
import pandas as pd
import glob
import re
import os


def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col : col[sorting_key])
    return [col['column_name'] for col in columns]



def convert_csv_to_json(schemas, ds_name, base_loc, folderName, fileName, new_base_loc):
    columns = get_column_names(schemas=schemas,ds_name=ds_name)
    original_file_loc = f'{base_loc}/{folderName}/{fileName}'
    data = pd.read_csv(original_file_loc, header=None, names=columns)
    print(f'Length of raw data for {ds_name} = {len(data)}')
    json_file_loc = f'{new_base_loc}/{folderName}'
    os.makedirs(json_file_loc,exist_ok=True)

    data.to_json(f'{json_file_loc}/{fileName}', orient='records', lines=True)
    json_data = pd.read_json(f'{json_file_loc}/{fileName}', lines=True)
    print(f'Length of json data for {ds_name} = {len(json_data)}')
    
    return json_data

def process_files():
    base_path = os.environ.get('SRC_BASE_PATH')
    new_base_path = os.environ.get('TGT_BASE_PATH')
    schemas = json.load(open(file=f'{base_path}/schemas.json')) 
    src_file_names = glob.glob(f'{base_path}/*/part-*')
    if len(src_file_names) == 0: 
        raise NameError('No files found under {base_path}')

    for file in src_file_names:
        split_data = re.split('[/\\\]',file)
        convert_csv_to_json(
            schemas=schemas,
            ds_name=split_data[2],
            base_loc=base_path,
            folderName=split_data[2],
            fileName=split_data[3],
            new_base_loc=new_base_path
        )

if __name__ == '__main__':
    process_files()