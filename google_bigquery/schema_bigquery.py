from google.cloud import bigquery

# Configurações
project_id = 'encoded-density-405819'
dataset_id = 'api_fogo_cruzado'
table_id = 'ocorrencias'
credentials_path = '/home/pedro/Geral/Estudos/Projeto_Plataforma_DataScience/fogo_cruzado/arquivos_complementares/encoded-density-405819-b95eedf3c89b.json'

# Configurando credenciais
client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

# Definindo o esquema da tabela
schema = [
    bigquery.SchemaField('id', 'STRING'),
    bigquery.SchemaField('address', 'STRING'),
    bigquery.SchemaField('stateName', 'STRING'),
    bigquery.SchemaField('regionName', 'STRING'),
    bigquery.SchemaField('cityName', 'STRING'),
    bigquery.SchemaField('neighborhood', 'STRING'),
    bigquery.SchemaField('localityName', 'STRING'),
    bigquery.SchemaField('latitude', 'FLOAT'),
    bigquery.SchemaField('longitude', 'FLOAT'),
    bigquery.SchemaField('date', 'TIMESTAMP'),
    bigquery.SchemaField('victimsAge', 'INTEGER'),
    bigquery.SchemaField('victimsGenre', 'STRING'),
    bigquery.SchemaField('victimsRace', 'STRING'),
    bigquery.SchemaField('victimsPlace', 'STRING'),
    bigquery.SchemaField('victimsQualifications', 'STRING'),
]

# Criando a tabela
table_ref = client.dataset(dataset_id).table(table_id)
table = bigquery.Table(table_ref, schema=schema)
client.create_table(table)

print(f'Tabela {table_id} criada com sucesso.')
