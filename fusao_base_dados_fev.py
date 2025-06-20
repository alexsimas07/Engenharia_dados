import json
import csv

from processamento_dados import Dados

# Tópicos de funções para o pipeline de dados
def leitura_json(path_json):
    dados_json = []
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json 

def leitura_csv(path_csv):
    dados_csv = []
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            dados_csv.append(row)

        return dados_csv
    
def leitura_dados(path, tipo_arquivo):
    dados = []

    if tipo_arquivo == 'json':
        return leitura_json(path)
    elif tipo_arquivo == 'csv':
        return leitura_csv(path)
    return dados

def get_columns(dados):
    return list(dados[0].keys())

def rename_columns(dados, key_mapping):
    new_dados_csv =[]

    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv

def size_data(dados):
    return len(dados)

def join(dadosA, dadosB):
    combined_list = []
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return combined_list

def transformando_dados_tabela(dados, nomes_colunas):
    dados_combinados_tabela = [nomes_colunas]

    for row in dados:
        linha = []
        for coluna in nomes_colunas:
            linha.append(row.get(coluna, 'Indisponivel'))
        dados_combinados_tabela.append(linha)
    return dados_combinados_tabela

def salvando_dados(dados, path):
    with open(path_dados_combinados, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)


# Iniciando a leitura e visualizando como está retornando
dados_json = leitura_dados(path_json, 'json')
nome_colunas_json = get_columns(dados_json)
tamanho_json = size_data(dados_json)
print(f"Nome colunas json:{nome_colunas_json}")
print(f"Tamanho dos dados json:{tamanho_json}")

dados_csv = leitura_dados(path_csv, 'csv')
nome_colunas_csv = get_columns(dados_csv)
tamanho_csv = size_data(dados_csv)
print(f"Nome colunas csv:{nome_colunas_csv}")
print(f"Tamanho dos dados csv:{tamanho_csv}")



# Transformação dos dados
key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
               'Data da Venda': 'Data da Venda'}


dados_csv = rename_columns(dados_csv, key_mapping)
nome_colunas_csv = get_columns(dados_csv)
print(f"Nome colunas combinados:{nome_colunas_csv}")

dados_fusao = join(dados_csv,dados_json)
nome_colunas_fusao = get_columns(dados_fusao)
tamanho_fusao = size_data(dados_fusao)
print(f"Nome colunas Fusão:{nome_colunas_fusao}")
print(f"Tamanho dos dados Fusão:{tamanho_fusao}")

# Salvando os dados

dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)

path_dados_combinados = 'data_processed/dados_combinados.csv'

salvando_dados(dados_fusao_tabela, path_dados_combinados)
print(f"Dados salvos em: {path_dados_combinados}")

# Definindo os caminhos dos arquivos
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

dados_empresaA = Dados(path_json, 'json')
print(f"Nome das colunas empresa A:{dados_empresaA.nome_colunas}")
print(f"Quantidade de linhas: {dados_empresaA.tamanho_dados}")

dados_empresaB = Dados(path_csv, 'csv')
print(f"Nome das colunas empresa B: {dados_empresaB.nome_colunas}")
print(f"Quantidade de linhas: {dados_empresaB.tamanho_dados}")

key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
               'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)
print(dados_empresaB.nome_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f"Nome das colunas empresa B: {dados_fusao.nome_colunas}")
print(f"Quantidade de linhas: {dados_fusao.tamanho_dados}")

# Load
path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(f"Dados salvos em: {path_dados_combinados}")


