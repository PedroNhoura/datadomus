import pandas as pd
from returned_data import FogoCruzadoData
from google.cloud import bigquery

# Configurações
project_id = 'encoded-density-405819'
dataset_id = 'api_fogo_cruzado'
table_id = 'ocorrencias'
credentials_path = '/home/pedro/Geral/Estudos/Projeto_Plataforma_DataScience/fogo_cruzado/arquivos_complementares/encoded-density-405819-b95eedf3c89b.json'  # Substitua pelo caminho do seu arquivo JSON de credenciais

# Configurando credenciais
client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

# Obtendo dados da API (substitua por seus próprios métodos)
fogo_cruzado_data = FogoCruzadoData("crixusalter@gmail.com", "DDpedro@123")
dados_ocorrencias = fogo_cruzado_data.obter_dados_ocorrencias(take=2000)

# Filtrando colunas desejadas
colunas_desejadas = ['id', 'address', 'state', 'city', 'latitude', 'longitude', 'contextInfo', 'date', 'victims']
dados_ocorrencias_filtrado = dados_ocorrencias[colunas_desejadas]

# Enviando DataFrame para o BigQuery
table_ref = f'{project_id}.{dataset_id}.{table_id}'
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(dados_ocorrencias_filtrado, table_ref, job_config=job_config)
job.result()  # Aguarda a conclusão do job

print(f'Dados inseridos na tabela {table_ref}')