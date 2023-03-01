
import os
import yfinance as yf
import pandas as pd
import requests
import zipfile
import time
import shutil

start_time = time.time()

WORKDIR = os.getcwd()

PATHBASEDADOS = WORKDIR + "/_Arquivos/BASE DE DADOS"
PATHDADOS = WORKDIR + "/_Arquivos/DADOS/"
PATHZIP = WORKDIR + "/_Arquivos/Zips/"
OUTEXCEL = WORKDIR + "/_Arquivos/Output Excel/"


# apagar arquivos para puxar dados de novo


folders = [PATHBASEDADOS, PATHDADOS, PATHZIP]

for folder in folders:
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

# -------- Puxando dados das ações com Yfinance

lista_ativos = ["ABEV3", "ITUB4", "PETR4", "VALE3"]

for ticker in lista_ativos:
    tickers = yf.Ticker(ticker + ".SA")
    hist = tickers.history(start='2022-01-01', end='2022-12-31')
    hist.index = hist.index.tz_localize(None)
    # adiconando coluna com o nome do produto
    hist['PRODUTO'] = ticker
    filename = f"{PATHDADOS}/{ticker}.csv"
    hist.to_csv(filename)

# ------Armazenando dados acoes na "BASE DE DADOS"


# Criar uma lista de nomes de arquivos CSV para ler, adicionando a extensão ".csv" a cada elemento da lista "lista_ativos"
lista_csv = [ticker + '.csv' for ticker in lista_ativos]

# Ler cada arquivo CSV na lista "lista_csv" e armazenar o resultado em uma nova lista "lista_dfs"
lista_dfs = [pd.read_csv(f'{PATHDADOS}/{csv_name}') for csv_name in lista_csv]

# Concatenar todos os DataFrames em "lista_dfs" em um único DataFrame "df_acoes"
df_acoes = pd.concat(lista_dfs)

# Salvar o DataFrame "df_acoes" como um arquivo JSON com cada registro como um objeto JSON separado
df_acoes.to_json(f'{PATHBASEDADOS}/dados_ACOES.json', orient='records')

# Puxando dados do fundo


# ------------puxando base de dados do fundo

# URLs base para baixar os arquivos
url_base = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202201.zip"

# Nomes dos arquivos
file_names = [f"inf_diario_fi_2022{str(item + 1).zfill(2)}.zip" for item in range(12)]

# Loop para baixar os arquivos
for i, file_name in enumerate(file_names):
    url = url_base[:len(url_base) - 6] + file_name[len(file_name) - 6:]
    response = requests.get(url)
    with open(F"{PATHZIP}/" + file_name, 'wb') as f:
        f.write(response.content)

# descomprimindo ZIP


# Caminho para o diretório onde estão os arquivos zip
path = f"{PATHZIP}/"

# Lista de arquivos zip no diretório
zip_files = [f for f in os.listdir(path) if f.endswith('.zip')]

# Loop para descompactar cada arquivo zip
for file_name in zip_files:
    with zipfile.ZipFile(os.path.join(path, file_name), 'r') as zip_ref:
        zip_ref.extractall(path)

    os.remove(os.path.join(path, file_name))

# ### tratando dados

# ------------------tratando base de dados do fundo

csv_file_names = [file_name.replace('.zip', '.csv') for file_name in file_names]

for i, csv in enumerate(csv_file_names):
    # carregar o DataFrame a partir de um arquivo CSV
    tabela = pd.read_csv(f"{PATHZIP}/{csv}", sep=';')

    # filtrar apenas as linhas onde a coluna 'CNPJ_FUNDO' tem o valor 41.707.420/0001-90'
    tabela_filtrada = tabela.loc[tabela['CNPJ_FUNDO'] == '41.707.420/0001-90']
    # salvando em um diretorio especifico
    tabela_filtrada.to_csv(
        f'{PATHDADOS}/novo_{csv}', index=False)
    csv_input = pd.read_csv(f'{PATHDADOS}/novo_{csv}')
    csv_input['PRODUTO'] = "FUNDO DE INVESTIMENTO"
    csv_input.to_csv(f'{PATHDADOS}/novo_{csv}', index=False)

# ---------Armazenando dados na "BASE DE DADOS"


csv_file_names_excluir = [f"novo_{csv_file}" for csv_file in csv_file_names]

lista_dfs = [pd.read_csv(f'{PATHDADOS}{csv_name}') for csv_name in csv_file_names_excluir]

df_fundo = pd.concat(lista_dfs)

df_fundo.to_csv(f'{PATHDADOS}/CONSTANTIA F.I. MULTIMERCADO CRÉDITO PRIVADO.csv', index=False)

df_fundo.to_json(f'{PATHBASEDADOS}/dados_FUNDO.json', orient='records')


# -------deletando os csv
directory = PATHDADOS

for file_name in os.listdir(directory):
    if "novo_inf_diario_fi_2022" in file_name:
        file_path = os.path.join(directory, file_name)
        os.remove(file_path)

# ------- Construindo dados de rentabilidade acões


# Transformando data de str para datetime
df_acoes['Date'] = pd.to_datetime(df_acoes['Date'])

# Criando filtro para data
filtro = (df_acoes['Date'] >= pd.to_datetime('2022-01-01')) & (df_acoes['Date'] <= pd.to_datetime('2022-12-31'))

# Filtra primeira e ultima data
primeira_data = df_acoes.loc[filtro, 'Date'].min()
ultima_data = df_acoes.loc[filtro, 'Date'].max()

# Construindo dados de rentabilidade acões
cols = ['PRODUTO', 'Close']

df_rent_acoes = df_acoes.loc[df_acoes['Date'] == primeira_data, cols].merge(
    df_acoes.loc[df_acoes['Date'] == ultima_data, cols], on='PRODUTO')

df_rent_acoes.columns = ['PRODUTO', 'InicialPrice', 'FinalPrice']

df_rent_acoes['Rentabilidade'] = [(y / x) - 1 for x, y in
                                  zip(df_rent_acoes['InicialPrice'], df_rent_acoes['FinalPrice'])]

# -------Construindo dados de rentabilidade fundo


# Transformando data de str para datetime
df_fundo['DT_COMPTC'] = pd.to_datetime(df_fundo['DT_COMPTC'])

# Criando filtro para data
filtro = (df_fundo['DT_COMPTC'] >= pd.to_datetime('2022-01-01')) & (
            df_fundo['DT_COMPTC'] <= pd.to_datetime('2022-12-31'))

# Filtra primeira e ultima dafiltrota
primeira_data = df_fundo.loc[filtro, 'DT_COMPTC'].min()
ultima_data = df_fundo.loc[filtro, 'DT_COMPTC'].max()

# Construindo dados de rentabilidade fundo
cols = ['PRODUTO', 'VL_QUOTA']

df_rent_fundo = df_fundo.loc[df_fundo['DT_COMPTC'] == primeira_data, cols].merge(
    df_fundo.loc[df_fundo['DT_COMPTC'] == ultima_data, cols], on='PRODUTO')

df_rent_fundo.columns = ['PRODUTO', 'InicialPrice', 'FinalPrice']

df_rent_fundo['Rentabilidade'] = [(y / x) - 1 for x, y in
                                  zip(df_rent_fundo['InicialPrice'], df_rent_fundo['FinalPrice'])]


df_rentabilidade = pd.concat([df_rent_acoes, df_rent_fundo], axis=0)


rentabilidade_list = df_rentabilidade['Rentabilidade'].tolist()
prod_list = df_rentabilidade['PRODUTO'].tolist()
rentabilidades = dict(zip(prod_list, rentabilidade_list))


# ---------Calcular o retorno da carteira, ponderando as posições de cada ativo e somando-as.---------------

valor_carteira = 10000000

procentagem_carteira = {'PETR4': 0.25, 'VALE3': 0.25, 'ITUB4': 0.1, 'ABEV3': 0.1, 'FUNDO DE INVESTIMENTO': 0.3}

retorno_total = sum(peso * (1 + rentabilidades[ativo]) * valor_carteira for ativo, peso in procentagem_carteira.items())

print(f"O retorno da carteira foi de R$ {retorno_total:.2f}")

for ativo, peso in procentagem_carteira.items():
    retorno_ativo = peso * (1 + rentabilidade_list[prod_list.index(ativo)]) * valor_carteira
    print(f"Retorno de {ativo}: R$ {retorno_ativo:.2f}")

# ------- salvando dados em Data frames


# Dados iniciais
valor_inicial = 10000000

# Calcula os retornos de cada ativo
retornos = {ativo: procentagem_carteira[ativo] * (1 + rentabilidades[ativo]) * valor_inicial for ativo in
            procentagem_carteira}

# Calcula o valor final de cada ativo
valores_finais = {ativo: valor_inicial * (1 + rentabilidades[ativo]) for ativo in rentabilidades}

# Adiciona o valor total da carteira
valores_finais['VALOR TOTAL CARTEIRA'] = sum(retornos.values())

# Cria o DataFrame
df2 = pd.DataFrame({
    'PETR4': [valor_inicial * procentagem_carteira['PETR4'], retornos['PETR4']],
    'VALE3': [valor_inicial * procentagem_carteira['VALE3'], retornos['VALE3']],
    'ITUB4': [valor_inicial * procentagem_carteira['ITUB4'], retornos['ITUB4']],
    'ABEV3': [valor_inicial * procentagem_carteira['ABEV3'], retornos['ABEV3']],
    'FUNDO': [valor_inicial * procentagem_carteira['FUNDO DE INVESTIMENTO'], retornos['FUNDO DE INVESTIMENTO']],
    'VALOR TOTAL CARTEIRA': [valor_inicial, valores_finais['VALOR TOTAL CARTEIRA']]
}, index=['valor inicial', 'valor final'])


df1 = pd.DataFrame(df_rentabilidade)

# ---criando excel com data frames
with pd.ExcelWriter(F'{OUTEXCEL}Dados Carteira.xlsx') as writer:
    df1.to_excel(writer, sheet_name='Ativos', index=False)
    df2.to_excel(writer, sheet_name='Retorno Carteira')

end_time = time.time()
elapsed_time = end_time - start_time

print("Tempo de execução:", elapsed_time, "segundos")
