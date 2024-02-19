import glob
import re
import pandas as pd
import json  
import sys
from decouple import config
import multiprocessing



def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_dets = schemas[ds_name]
    columns = sorted(column_dets, key= lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns, chunksize=10000)
    return df_reader

def to_sql(df, db_con_uri, ds_name):
    df.to_sql(
        ds_name,
        db_con_uri,
        if_exists='append',
        index=False,
        method='multi'
    )


def db_loader(src_files, db_con_uri, ds_name):
    schemas = json.load(open(f'{src_files}/schemas.json'))
    files = glob.glob(f'{src_files}/{ds_name}/part-*')
    
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    
    for filename in files:
        df_reader = read_csv(filename, schemas)
        for idx, df in enumerate(df_reader):
            print(f'Popoulating chunk {idx} of {ds_name}')
            to_sql(df, db_con_uri, ds_name)


def process_dataset(args):
    src_files = args[0]
    db_con_uri = args[1]
    ds_name = args[2]
    try:
        print(f'Processing {ds_name}')
        db_loader(src_files, db_con_uri, ds_name)
    except NameError as ne:
        print(ne)
        pass
    finally:
        print(f'Data processing of {ds_name} is complete')


def process_files(ds_names=None):
    src_files = config('SRC_FILES')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')
    db_name =config('DB_NAME')
    db_user =config('DB_USER')
    db_pass =config('DB_PASS')
    db_con_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_files}/schemas.json'))
    if ds_names is None:  
        ds_names = list(schemas.keys())
    pprocess = len(ds_names) if len(ds_names) < 10 else 10
    pool = multiprocessing.Pool(pprocess)
    pd_args = []
    for ds_name in ds_names:
        pd_args.append((src_files, db_con_uri, ds_name))
    pool.map(process_dataset, pd_args)



if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()