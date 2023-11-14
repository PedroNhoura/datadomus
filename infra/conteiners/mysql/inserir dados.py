# inserir_dados.py

import mysql.connector
import json

def conectar_mysql():
    return mysql.connector.connect(
        host="localhost",
        user="usuario",  # Seu usuário
        password="senha_do_usuario",  # Sua senha
        database="nome_do_banco"  # Nome do banco de dados
    )

def inserir_dados(data):
    conn = conectar_mysql()
    cursor = conn.cursor()
    
    for item in data['data']:
        # Aqui você define a query de inserção baseada na estrutura do seu banco de dados
        query = "INSERT INTO nome_da_tabela (coluna1, coluna2, ...) VALUES (%s, %s, ...)"
        values = (item['campo1'], item['campo2'], ...)  # Substitua pelos campos relevantes

        cursor.execute(query, values)

    conn.commit()
    cursor.close()
    conn.close()

# Supondo que você tenha os dados em um arquivo JSON
with open('dados_api.json', 'r') as file:
    data = json.load(file)

inserir_dados(data)
