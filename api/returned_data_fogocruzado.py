# returned_data.py
import pandas as pd
from connection import FogoCruzadoAPI

class FogoCruzadoData:
    def __init__(self, email, password):
        self.api = FogoCruzadoAPI(email, password)

    def obter_dados_ocorrencias(self, id_state, initial_date=None, final_date=None):
        url = f"https://api-service.fogocruzado.org.br/api/v2/occurrences?order=ASC&page=1&idState={id_state}"

        if initial_date and final_date:
            url += f"&initialdate={initial_date}&finaldate={final_date}"

        response = self.api.request_with_token(url)

        if response.status_code == 200:
            data = response.json().get('data', [])
            tabela_dados = pd.DataFrame(data)
            return tabela_dados
        else:
            raise Exception(f"A solicitação falhou com o status {response.status_code}")
