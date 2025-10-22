# --- Instalação de bibliotecas ---

import pandas as pd
import os
import numpy as np

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
                nome_arquivo = os.path.basename(arquivo)
                print(f'SISTEMA: Importando o arquivo do Totalbus "{nome_arquivo}".')
                df_temp = pd.read_csv(arquivo, sep=';', encoding='latin-1', usecols=colunas_totalbus)
            except Exception as e:
                print(f'SISTEMA: Erro ao processar o arquivo do Totalbus "{df_temp}" - {e}')
        else:
            print(f'SISTEMA: Arquivo no formato inválido. Verifique a extensão! - {e}')

        if df_temp is not None:
            df_temp['Origem'] = nome_arquivo
            lista_vazia.append(df_temp)

    if lista_vazia:
        try:
            df_totalbus = pd.concat(lista_vazia, ignore_index=True)
        except Exception as e:
            print(f'SISTEMA: Erro ao consolidar o arquivo.')

    # ajustando o tipo das datas
    df_totalbus['DATA HORA VENDA'] = pd.to_datetime(df_totalbus['DATA HORA VENDA'], errors='coerce')

    # ajustando o tipo str nas colunas
    tipo_str = [
        'EMPRESA', 'STATUS BILHETE', 'AGENCIA ORIGINAL', 'ID TRANSACAO', 'ID TRANSACAO ORIGINAL', 'NOME PASSAGEIRO', 'POLTRONA',
        'Origem'
    ]

    tipo = {}
    tipo.update({col: str for col in tipo_str})

    # ajustando o tipo de valor das colunas
    df_totalbus['TARIFA'] = df_totalbus['TARIFA'].astype(str).str.replace(',', '.', regex=False).astype(float)
    df_totalbus['PEDAGIO'] = df_totalbus['PEDAGIO'].astype(str).str.replace(',', '.', regex=False).astype(float)
    df_totalbus['TAXA_EMB'] = df_totalbus['TAXA_EMB'].astype(str).str.replace(',', '.', regex=False).astype(float)
    df_totalbus['VALOR MULTA'] = df_totalbus['VALOR MULTA'].astype(str).str.replace(',', '.', regex=False).astype(float)

    return df_totalbus


def processamento_j3(diretorio_j3):

    colunas_j3 = [
        'Data Venda', 'Data Cancelamento', 'Tarifa', 'Seguro', 'Pedágio', 'Taxa de Embarque', 'Outros',
        'Assento', 'Nome Passageiro', 'Numero Bilhete', 'Data Viagem', 'Estorno Tarifa', 'Estorno Taxa', 'Estorno Total'
        ]

    abas_j3_dados = ['Extrato Pago', 'Extrato Alterados', 'Extrato Cancelado Online', 'Extrato Cancelado Offline']

    lista_vazia = []

    arquivos_j3 = [
        os.path.join(diretorio_j3, f) for f in os.listdir(diretorio_j3) if f.endswith(('.csv', '.xls', '.xlsx'))
    ]

    for arquivo in arquivos_j3:
        if arquivo.endswith(('.xlsx')):
            try:
                nome_arquivo = os.path.basename(arquivo)
                print(f'SISTEMA: Importando o arquivo de J3 "{nome_arquivo}".')
                df_temp = pd.read_excel(arquivo, sheet_name=None, header=1)
                
            except Exception as e:
                print(f'SISTEMA: Erro ao processar o arquivo da J3 "{df_temp}" - {e}')
        else:
            print(f'SISTEMA: Arquivo no formato inválido. Verifique a extensão! - {e}')

        if df_temp is not None:
            for nome_aba, df_aba  in df_temp.items():
                if nome_aba in abas_j3_dados:
                    try:
                        df_aba_filtrado = df_aba[colunas_j3].copy()
                        df_aba_filtrado['Origem'] = nome_arquivo

                        if nome_aba == 'Extrato Pago': df_aba_filtrado['Status'] = 'V'
                        elif nome_aba == 'Extrato Alterados': df_aba_filtrado['Status'] = 'C'
                        elif nome_aba == 'Extrato Cancelado Online': df_aba_filtrado['Status'] = 'C'
                        elif nome_aba == 'Extrato Cancelado Offline': df_aba_filtrado['Status'] = 'E'

                        lista_vazia.append(df_aba_filtrado)
                    except Exception as e:
                        print(f'SISTEMA: Erro ao processar o arquivo da J3 "{nome_aba}".')

    if lista_vazia:
        try:
            df_j3 = pd.concat(lista_vazia, ignore_index=True)
        except Exception as e:
            print(f'SISTEMA: Erro ao consolidar o arquivo.')

    # incluindo a empresa
    empresa_condicao = [
        df_j3['Origem'].str.contains('VGL', na=False),
        df_j3['Origem'].str.contains('EPIL', na=False),
        df_j3['Origem'].str.contains('BS', na=False),
        df_j3['Origem'].str.contains('ESA', na=False)
    ]

    empresa_resultado =[
        'Viação Garcia',
        'Princesa do Ivaí',
        'Brasil Sul',
        'Santo Anjo'
    ]

    df_j3['Empresa'] = np.select(empresa_condicao, empresa_resultado, pd.NaT)

    # definindo as colunas de datas
    tipo_data = ['Data Venda', 'Data Cancelamento', 'Data Viagem']
    for i in tipo_data:
        df_j3[i] = pd.to_datetime(df_j3[i], errors='coerce')

    # definindo os tipos das colunas
    tipo_valor = ['Tarifa', 'Seguro', 'Pedágio',
       'Taxa de Embarque', 'Outros', 'Estorno Tarifa', 'Estorno Taxa',
       'Estorno Total']

    tipo_str = [
        'Assento', 'Nome Passageiro', 'Numero Bilhete', 'Origem', 'Status', 'Empresa'
    ]

    tipos = {}
    tipos.update({col: float for col in tipo_valor})
    tipos.update({col: str for col in tipo_str})

    df_j3.astype(tipos)

    return df_j3

# --- Processando dataframes --- #

df_totalbus_v = processamento_totalbus(caminho_totalbus_v)
df_totalbus_c = processamento_totalbus(caminho_totalbus_c)
df_j3 = processamento_j3(caminho_j3)

# agrupando dataframes

df_totalbus = pd.concat([df_totalbus_v, df_totalbus_c], ignore_index=True)

# extratopago = vendas
# extratoalterados = cancelados
# extratocanceladoonline = cancelados
# extratoscanceladooffline = estorno