import pandas as pd
from returned_data_fogocruzado import FogoCruzadoData
from google.cloud import bigquery
from tqdm import tqdm

class LoadingToBigQuery:
    def __init__(self, project_id, dataset_id, table_id, credentials_path):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.credentials_path = credentials_path
        self.client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

    def check_duplicates(self, dataframe):
        unique_ids = list(dataframe['id'].unique())
        query = f'SELECT id FROM `{self.project_id}.{self.dataset_id}.{self.table_id}` WHERE id IN UNNEST(@ids)'
        query_params = [bigquery.ArrayQueryParameter("ids", "STRING", unique_ids)]
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = self.client.query(query, job_config=job_config)
        duplicate_ids = set(row.id for row in query_job)
        return duplicate_ids

    def load_dataframe_to_bigquery(self, dataframe, batch_size=1000):
        duplicate_ids = self.check_duplicates(dataframe)
        inserted_ids = []
        duplicate_inserts = []

        total_batches = len(dataframe) // batch_size + (1 if len(dataframe) % batch_size > 0 else 0)
        tqdm_batch_iterator = tqdm(range(0, len(dataframe), batch_size), total=total_batches, desc="Progresso")

        for i in tqdm_batch_iterator:
            batch_df = dataframe.iloc[i:i + batch_size]

            for _, row in batch_df.iterrows():
                if row['id'] in duplicate_ids:
                    duplicate_inserts.append(row['id'])
                else:
                    inserted_ids.append(row['id'])
                    table_ref = f'{self.project_id}.{self.dataset_id}.{self.table_id}'
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    job = self.client.load_table_from_dataframe(pd.DataFrame([row]), table_ref, job_config=job_config)
                    job.result()

            tqdm_batch_iterator.set_postfix({"IDs duplicados": len(duplicate_inserts), "IDs inseridos": len(inserted_ids)})

        print(f'Total de resultados consultados: {len(dataframe)}')

# Uso
if __name__ == "__main__":
    project_id = 'encoded-density-405819'
    dataset_id = 'api_fogo_cruzado'
    table_id = 'ocorrencias'
    credentials_path = '/home/pedro/Geral/Estudos/Projeto_Plataforma_DataScience/fogo_cruzado/arquivos_complementares/encoded-density-405819-53122b1023b6.json'

    fogo_cruzado_data = FogoCruzadoData("crixusalter@gmail.com", "DDpedro@123")
    
    # Defina as datas desejadas
    initial_date = "2023-01-25"
    final_date = "2023-01-30"

    dados_ocorrencias = fogo_cruzado_data.obter_dados_ocorrencias(
        id_state="b112ffbe-17b3-4ad0-8f2a-2038745d1d14",
        initial_date=initial_date,
        final_date=final_date
    )

    colunas_desejadas = ['id', 'address', 'state', 'city', 'latitude', 'longitude', 'contextInfo', 'date', 'victims']
    dados_ocorrencias_filtrado = dados_ocorrencias[colunas_desejadas]

    loader = LoadingToBigQuery(project_id, dataset_id, table_id, credentials_path)
    loader.load_dataframe_to_bigquery(dados_ocorrencias_filtrado)
    print(f'Total de resultados consultados: {len(dados_ocorrencias)}')
