# --- Instalação de bibliotecas ---

import pandas as pd
import os

# --- Definição de diretórios ---

caminho_base = os.path.dirname(os.path.abspath(__file__))

caminho_totalbus_v = os.path.join(caminho_base, 'Corporativo/Vendas')
caminho_totalbus_c = os.path.join(caminho_base, 'Corporativo/Cancelados')
caminho_j3 = os.path.join(caminho_base, 'J3')

# --- Processamento de arquivos Totalbus ---

def processamento_totalbus(diretorio_totalbus):

    colunas_totalbus = [
        'EMPRESA', 'NUMERO BILHETE', 'DATA HORA VENDA', 'STATUS BILHETE', 'TARIFA',
        'PEDAGIO', 'TAXA_EMB', 'TOTAL DO BILHETE', 'AGENCIA ORIGINAL', 'ID TRANSACAO',
        'ID TRANSACAO ORIGINAL', 'NOME PASSAGEIRO', 'POLTRONA', 'VALOR MULTA', 'DATA HORA VIAGEM',
        'DATA HORA VENDA PARA CANC.'
        ]

    lista_vazia = []

    arquivos_totalbus = [
        os.path.join(diretorio_totalbus, f) for f in os.listdir(diretorio_totalbus) if f.endswith(('.csv', '.xls', '.xlsx'))
    ]

    for arquivo in arquivos_totalbus:
        if arquivo.endswith(('.csv')):
            try:
                df_temp = pd.read_csv(arquivo, sep=';', encoding='latin-1', usecols=colunas_totalbus)
            except Exception as e:
                print(f'SISTEMA: Erro ao processar o arquivo do Totalbus "{df_temp}" - {e}')
        else:
            print(f'SISTEMA: Arquivo no formato inválido. Verifique a extensão! - {e}')

        if df_temp is not None:
            lista_vazia.append(df_temp)

    if lista_vazia:
        try:
            df_totalbus = pd.concat(lista_vazia)
        except Exception as e:
            print(f'SISTEMA: Erro ao consolidar o arquivo.')

    return df_totalbus



# --- Processando dataframes --- #

df_totalbus_v = processamento_totalbus(caminho_totalbus_v)
df_totalbus_c = processamento_totalbus(caminho_totalbus_c)

# agrupando dataframes

df_totalbus = pd.concat([df_totalbus_v, df_totalbus_c])



# extratopago = vendas
# extratoalterados = cancelados
# extratocanceladoonline = cancelados
# extratoscanceladooffline = estorno