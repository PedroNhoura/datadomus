import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from connection import FogoCruzadoAPI

class FogoCruzadoData:
    def __init__(self, email, password):
        self.api = FogoCruzadoAPI(email, password)

    def obter_dados_ocorrencias(self, take=20, id_state="b112ffbe-17b3-4ad0-8f2a-2038745d1d14"):
        url = f"https://api-service.fogocruzado.org.br/api/v2/occurrences?order=ASC&page=1&take={take}&idState={id_state}"
        response = self.api.request_with_token(url)

        if response.status_code == 200:
            data = response.json()['data']
            tabela_dados = pd.DataFrame(data)
            return tabela_dados
        else:
            print(f"A solicitação falhou com o status {response.status_code}")
            return None

    def criar_geodataframe(self, dados):
        # Verifica se as colunas de latitude e longitude existem nos dados
        if 'latitude' in dados.columns and 'longitude' in dados.columns:
            # Cria a geometria Point usando as colunas de latitude e longitude
            geometry = [Point(float(lon), float(lat)) for lat, lon in zip(dados['latitude'], dados['longitude'])]
            
            # Cria o GeoDataFrame
            gdf = gpd.GeoDataFrame(dados, geometry=geometry, crs="EPSG:4326")
            return gdf
        else:
            print("As colunas de latitude e/ou longitude não foram encontradas nos dados.")
            return None

# Uso
email = "crixusalter@gmail.com"
password = "DDpedro@123"


fogo_cruzado_data = FogoCruzadoData(email, password)

# # Obtendo os dados de ocorrências
dados_ocorrencias = fogo_cruzado_data.obter_dados_ocorrencias(take=2000)

print(dados_ocorrencias.info())

