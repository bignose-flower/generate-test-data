import click
import sqlalchemy
import os
import pandas as pd
import random
import string
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from sqlalchemy import text
from tabulate import tabulate

# ログディレクトリの設定
log_dir = './log'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# ログ設定
app_name = "test-data-generator"
log_filename = os.path.join(log_dir, datetime.datetime.now().strftime(f'{app_name}_%Y%m%d.log'))
log_handler = TimedRotatingFileHandler(log_filename, when='midnight', interval=1, backupCount=7)
log_handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s] %(message)s', datefmt='%Y/%m/%d %H:%M:%S'))
log_handler.suffix = "%Y%m%d%H%M%S"
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        log_handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# シングルクォートが必要なデータ型リスト
QUOTED_DATA_TYPES = [
    'CHAR', 'VARCHAR', 'TEXT', 'NVARCHAR', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR'
]

# シングルクォートが不要なデータ型リスト
NON_QUOTED_DATA_TYPES = [
    'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL', 'BOOLEAN', 'BIT'
]

def create_db_url(config_file,password):
    with open(config_file, 'r') as file:
        config = json.load(file)

    db_info = config['db_information_config']
    
    db_type = db_info.get('db_type')
    username = db_info.get('username')
    host = db_info.get('host')
    port = db_info.get('port')
    database = db_info.get('database')

    if db_type == 'sqlserver':
        db_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    elif db_type == 'postgresql':
        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    else:
        raise ValueError("Unsupported database type")

    return db_url

def get_database_info(engine):
    with engine.connect() as connection:
        result = connection.execute(text("SELECT @@VERSION"))
        for row in result:
            return row[0]

@click.group()
def cli():
    pass

def get_data_type_without_length(data_type):
    return data_type.split('(')[0]

def generate_random_data(data_type, length, sequence):
    if data_type in ['INTEGER', 'BIGINT', 'SMALLINT', 'SERIAL', 'BIGSERIAL']:
        return sequence
    elif data_type in ['VARCHAR', 'CHAR', 'TEXT']:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    elif data_type == 'DATE':
        start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
        end_date = datetime.strptime('2020-12-31', '%Y-%m-%d')
        random_date = start_date + (end_date - start_date) * random.random()
        return random_date.date()
    elif data_type == 'TIMESTAMP':
        start_datetime = datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        end_datetime = datetime.strptime('2020-12-31 23:59:59', '%Y-%m-%d %H:%M:%S')
        random_datetime = start_datetime + (end_datetime - start_datetime) * random.random()
        return random_datetime
    elif data_type == 'BOOLEAN':
        return random.choice([True, False])
    elif data_type in ['NUMERIC', 'FLOAT']:
        return round(random.uniform(0, 10000), 2)
    else:
        return None

@cli.command('get_metadata')
@click.option('--config-file', prompt='Config JSON File', help='The JSON file with configuration information.')
@click.option('--password', prompt='Password for login Database', help='Password for login Database.')
def get_metadata(config_file,password):
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
    except Exception as e:
        logger.error("Error loading config file: %s", e)
        return
    
    # 接続先URLの生成
    db_url = create_db_url(config_file,password)

    db_info = config['db_information_config']
    generate_data_config = config['generate_data_config']
    
    table_name = db_info['table_name']
    metadata_output_file = generate_data_config.get('metadata_output_file', 'metadata.csv')
    transposed_output_file = generate_data_config.get('transposed_output_file', 'transposed_metadata.csv')

    try:
        engine = sqlalchemy.create_engine(db_url)
        db_info = get_database_info(engine)
        logger.info(f"Database information: {db_info}")
        inspector = sqlalchemy.inspect(engine)

        logger.info("Database connection established and table loaded successfully.")
    except Exception as e:
        logger.error("Error connecting to the database or loading the table: %s", e)
    
    try:
        constraints = {}
        # Getting primary keys
        primary_keys = inspector.get_pk_constraint(table_name)['constrained_columns']
        for col in primary_keys:
            if col not in constraints:
                constraints[col] = []
            constraints[col].append('PRIMARY KEY')
        logger.info("Primary keys retrieved: %s", primary_keys)
        
        # Getting unique constraints
        
        if "SQL Server" not in db_info:
            unique_constraints = inspector.get_unique_constraints(table_name)
            for uc in unique_constraints:
                for col in uc['column_names']:
                    if col not in constraints:
                        constraints[col] = []
                    constraints[col].append('UNIQUE')
            logger.info("Unique constraints retrieved.")
        
        # Getting check constraints
        if "SQL Server" not in db_info:
            check_constraints = inspector.get_check_constraints(table_name)
            for cc in check_constraints:
                for col in cc['column_names']:
                    if col not in constraints:
                        constraints[col] = []
                    constraints[col].append(f"CHECK ({cc['sqltext']})")
            logger.info("Check constraints retrieved.")
        
        # Getting not null constraints
        columns = inspector.get_columns(table_name)
        for col in columns:
            if not col['nullable']:
                if col['name'] not in constraints:
                    constraints[col['name']] = []
                constraints[col['name']].append('NOT NULL')
        logger.info("Not null constraints retrieved.")
        
        metadata_df = pd.DataFrame({
            'column_id': [col['name'] for col in columns],
            'data_type': [str(col['type']) for col in columns],
            'data_type_without_length': [get_data_type_without_length(str(col['type'])) for col in columns],
            'constraints': [';'.join(constraints.get(col['name'], [])) for col in columns]
        })
    
        logger.info(f'\n{metadata_df}')
        metadata_df.to_csv(metadata_output_file, index=False)
        logger.info(f'Metadata has been written to {metadata_output_file}')

        # Transpose the DataFrame
        transposed_df = metadata_df.T
        transposed_df.to_csv(transposed_output_file, header=False,index=False)
        logger.info(f'Transposed metadata has been written to {transposed_output_file}')

    except Exception as e:
        logger.error("Error retrieving metadata or writing to CSV: %s", e)

@cli.command('generate_test_data')
@click.option('--config-file', prompt='Config JSON File', help='The JSON file with configuration information.')
@click.option('--password', prompt='Password for login Database', help='Password for login Database.')
def generate_test_data(config_file,password):
    with open(config_file, 'r') as file:
        config = json.load(file)
    
    db_info = config['db_information_config']
    generate_data_config = config['generate_data_config']
    
    db_url = db_info['db_url']
    table_name = db_info['table_name']
    count = generate_data_config['count']
    output_file = generate_data_config['output_file']
    sql_file = generate_data_config['sql_file']
    
    fixed_values = generate_data_config.get('fixed_values', {})
    
    engine = sqlalchemy.create_engine(db_url)
    metadata = sqlalchemy.MetaData(bind=engine)
    table = sqlalchemy.Table(table_name, metadata, autoload_with=engine)
    
    data = []
    for i in range(count):
        row = {}
        for column in table.columns:
            if column.name in fixed_values:
                row[column.name] = fixed_values[column.name]
            else:
                length = column.type.length if hasattr(column.type, 'length') else 10
                row[column.name] = generate_random_data(column.type.__class__.__name__, length, i+1)
        data.append(row)
    
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    
    with open(sql_file, 'w') as f:
        for row in data:
            columns = ', '.join(row.keys())
            values = ', '.join(f"'{v}'" for v in row.values())
            f.write(f"INSERT INTO {table_name} ({columns}) VALUES ({values});\n")

@cli.command('generate_insert_statements')
@click.option('--config-file', prompt='Config JSON File', help='The JSON file with configuration information.')
def generate_insert_statements(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    
    db_info = config['db_information_config']
    create_insert_config = config['create_insert_config']
    
    table_name = db_info['table_name']
    input_file = create_insert_config['input_file']
    output_file = create_insert_config['insert_output_file']
    
    df = pd.read_csv(input_file,keep_default_na=False)
    print(df)
    data_types = df.iloc[0].to_list()  # 2行目のデータ型を取得
    with open(output_file, 'w') as f:
        for i, row in df.iterrows():
            if i == 0:
                continue  # データ型の行をスキップ
            columns = ', '.join(df.columns)
            values = []
            for value, data_type in zip(row, data_types):
                if value == "":
                    values.append(str('NULL'))
                    continue
                if '(' in data_type:
                    data_type = data_type.split('(')[0]  # 長さ情報を除去
                if ' ' in data_type:
                    data_type = data_type.split(' ')[0]  # 照合順序を除去
                if data_type.upper() in QUOTED_DATA_TYPES:
                    values.append(f"'{value}'")
                elif data_type.upper() in NON_QUOTED_DATA_TYPES:
                    values.append(str(value))
                else:
                    values.append(str('NULL'))
            f.write(f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(values)});\n")
    logger.info(f'Transposed metadata has been written to {output_file}')

@cli.command('visualize_csv')
@click.argument('input_file')
@click.option('--width', default=20, help='Column width for better readability.')
def visualize_csv(input_file, width):
    """Visualize the CSV file with better column width."""
    try:
        df = pd.read_csv(input_file)
        print(tabulate(df, headers='keys', tablefmt='grid', showindex=False, maxcolwidths=width))
    except Exception as e:
        logger.error("Error reading or visualizing the CSV file: %s", e)

def execute_sql_file(db_url, sql_file_path):
    engine = sqlalchemy.create_engine(db_url)
    with engine.connect() as connection:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_commands = file.read().split(';')
            for command in sql_commands:
                if command.strip():
                    try:
                        connection.execute(text(command))
                        logger.info(f"Executed SQL command: {command}")
                    except Exception as e:
                        logger.error(f"Error executing SQL command: {command}\n{e}")


@cli.command('execute_sql')
@click.option('--config-file', prompt='Config JSON File', help='The JSON file with configuration information.')
@click.option('--password', prompt='Password for login Database', help='Password for login Database.')
@click.option('--sql-file', prompt='SQL File Path', help='The path to the SQL file to be executed.')
def execute_sql(config_file, password, sql_file):
    db_url = create_db_url(config_file, password)
    execute_sql_file(db_url, sql_file)

if __name__ == '__main__':
    cli()
