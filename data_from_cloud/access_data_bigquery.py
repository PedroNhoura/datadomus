from google.cloud import bigquery
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import folium
from folium.plugins import MarkerCluster

# Configurar suas credenciais do Google Cloud
# Substitua 'seu-arquivo-de-credenciais.json' pelo caminho do seu arquivo de credenciais.
# Consulte a documentação do Google Cloud para obter informações sobre como criar essas credenciais.
client = bigquery.Client.from_service_account_json('/home/pedro/Geral/Estudos/Projeto_Plataforma_DataScience/fogo_cruzado/arquivos_complementares/encoded-density-405819-53122b1023b6.json')

# Query para o BigQuery
query = """
SELECT
  ocorrencia_id,
  address,
  state_id,
  state_name,
  city_name,
  latitude,
  longitude,
  date,
  situation,
  age,
  genre_name,
  race,
  idade_vitima,
  nome_corporacao
FROM
  encoded-density-405819.states.rio_de_janeiro
"""

# Executar a consulta e obter os resultados
df = client.query(query).to_dataframe()

# Converter colunas para float
df['latitude'] = df['latitude'].astype(float)
df['longitude'] = df['longitude'].astype(float)

# Transformar DataFrame em GeoDataFrame
geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry)

# Criar um mapa centrado no Rio de Janeiro
map_center = [-22.9068, -43.1729]
mymap = folium.Map(location=map_center, zoom_start=12)

# Adicionar um cluster de marcadores ao mapa
marker_cluster = MarkerCluster().add_to(mymap)

# Adicionar marcadores ao cluster
for index, row in gdf.iterrows():
    folium.Marker([row['latitude'], row['longitude']], popup=row['ocorrencia_id']).add_to(marker_cluster)

# Exibir o mapa
mymap

html_path = '/home/pedro/Geral/Estudos/Projeto_Plataforma_DataScience/fogo_cruzado/data_from_cloud/interactive_map.html'


mymap.save(html_path)

gdf.info()