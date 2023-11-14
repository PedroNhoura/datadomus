import pandas as pd
from main import FogoCruzadoAPI

def obter_dados_ocorrencias(api, take=20, id_state="b112ffbe-17b3-4ad0-8f2a-2038745d1d14"):
    # Construindo a URL com os parâmetros desejados
    url = f"https://api-service.fogocruzado.org.br/api/v2/occurrences?order=ASC&page=1&take={take}&idState={id_state}"

    # Fazendo a solicitação para obter os dados de ocorrências
    response = api.request_with_token(url)

    # Verificando se a solicitação foi bem-sucedida (status 200)
    if response.status_code == 200:
        # Convertendo a resposta JSON para um DataFrame pandas
        data = response.json()['data']
        tabela_dados = pd.DataFrame(data)
        return tabela_dados
    else:
        print(f"A solicitação falhou com o status {response.status_code}")
        return None

# Uso
email = "crixusalter@gmail.com"
password = "DDpedro@123"
api = FogoCruzadoAPI(email, password)

# Obtendo os dados de ocorrências
dados_ocorrencias = obter_dados_ocorrencias(api, take=500)

# Verificando se os dados foram obtidos com sucesso
# if dados_ocorrencias is not None:
#     # Imprimindo informações e a primeira linha do DataFrame
print(dados_ocorrencias.info())
print(dados_ocorrencias.address.iloc[0])  # Exemplo de impressão de uma coluna específica (address)



# achar o index do json onde estao as coordenadas para cria um GEODATAFRAME 