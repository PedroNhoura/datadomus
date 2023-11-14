#api fogo cruzado 


#conectando com a api 

import requests
import time

class FogoCruzadoAPI:
    def __init__(self, email, password):
        self.api_url = "https://api-service.fogocruzado.org.br/api/v2/auth"
        self.token = None
        self.expires_in = None
        self.last_token_time = None
        self.email = email
        self.password = password
        self.authenticate()

    def authenticate(self):
        response = requests.post(f"{self.api_url}/login", json={"email": self.email, "password": self.password})
        if response.status_code == 201:
            data = response.json()['data']
            self.token = data['accessToken']
            self.expires_in = data['expiresIn']
            self.last_token_time = time.time()
        else:
            raise Exception("Authentication failed")

    def refresh_token(self):
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.post(f"{self.api_url}/refresh", headers=headers)
        if response.status_code == 201:
            data = response.json()['data']
            self.token = data['accessToken']
            self.expires_in = data['expiresIn']
            self.last_token_time = time.time()
        else:
            raise Exception("Token refresh failed")

    def token_expired(self):
        if time.time() - self.last_token_time >= self.expires_in:
            return True
        return False

    def request_with_token(self, endpoint):
        if self.token_expired():
            self.refresh_token()
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(endpoint, headers=headers)
        return response

# # Uso
# email = "crixusalter@gmail.com"
# password = "DDpedro@123"
# api = FogoCruzadoAPI(email, password)

# # Exemplo de como fazer uma requisição
# # response = api.request_with_token("https://api-service.fogocruzado.org.br/algum-endpoint")