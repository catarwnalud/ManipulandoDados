import psycopg2
import pandas as pd


# Variáveis globais

conexao = psycopg2.connect(dbname = 'srag_sus', user = 'postgres', password = 'minhasenha', host = 'localhost', port = '5433') # Faz a conexão com o nosso banco de dados intitulado 'srag_sus'

print(conexao.status) # Mostra na tela o número 1 confirmando a conexão com o banco

cursor = conexao.cursor() # Executa os comandos

df = pd.read_csv('INFLUD20-01-05-2023.csv', sep =';') # Lê o arquivo 'INFLUD20-01-05-2023.csv' e o transforma em um dataframe para manipulação

# Função para selecionar colunas específicas do dataframe e transformá-las em um arquivo CSV, tornando possível a utilização do método 'copy_from' e enviando os dados para o banco de 
# maneira eficiente e recomendada.

def table_paciente(): 

    df_paciente = df[['CS_SEXO', 'DT_NASC', 'DT_1_DOSE', 'DT_2_DOSE',
                    'SURTO_SG', 'PAC_DSCBO', 'PAC_COCBO', 'DT_DOSEUNI', 'M_AMAMENTA',
                    'DT_VAC_MAE', 'MAE_VAC', 'CS_ESCOL_N', 'AVE_SUINO', 'OUT_ANIM', 'CS_RACA',
                    'CS_GESTANT','TP_IDADE', 'NU_IDADE_N']]
    
    columns = df_paciente.columns.str.lower() # Para especificar as colunas das tabelas que serão preenchidas

    df_paciente['PAC_COCBO'] = df_paciente['PAC_COCBO'].replace('XXX', None) # Tratamento de dados
                                                      
    df_paciente.to_csv('paciente.csv', sep='|', index=False)
    
    with open ('paciente.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'paciente', sep='|', null='', columns=(columns))

    conexao.commit()

    return 'Importação paciente concluída!'

print(table_paciente())

def table_ficha():

    df_ficha = df[['CO_REGIONA', 'ID_REGIONA', 'DT_NOTIFIC', 'DT_SIN_PRI', 'SG_UF_NOT',
                   'ID_MUNICIP', 'CO_MUN_NOT', 'CO_UNI_NOT', 'ID_UNIDADE', 'DT_DIGITA']]
    
    df_ficha.to_csv('ficha.csv', sep=';', index = False)

    columns = df_ficha.columns.str.lower()

    with open ('ficha.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'ficha', sep=';', null='', columns=(columns))

    conexao.commit()

    return 'Importação ficha concluída!'

print(table_ficha())

def table_conclusao():

    df_conclusao = df[['CLASSI_FIN', 'CLASSI_OUT', 'CRITERIO',
                        'EVOLUCAO', 'DT_EVOLUCA', 'DT_ENCERRA']]
    
    df_conclusao.to_csv('conclusao.csv', sep='|', index=False)

    columns = df_conclusao.columns.str.lower()

    with open ('conclusao.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'conclusao', sep='|', null='', columns=columns)

    conexao.commit()

    return 'Importação conclusao concluída!'

print(table_conclusao())

def table_sintomas():

    df_sintomas= df[['SATURACAO', 'DESC_RESP', 'DISPNEIA',
                        'GARGANTA', 'TOSSE', 'FEBRE', 'OUTRO_DES', 'DOR_ABD', 'PERD_OLFT',
                        'FADIGA', 'OUTRO_SIN', 'VOMITO', 'DIARREIA', 'PERD_PALA']]
    
    df_sintomas['OUTRO_DES'] = df_sintomas['OUTRO_DES'].str.replace('\\', '.')

    df_sintomas.to_csv('sintomas.csv', sep='#', index=False)

    columns = df_sintomas.columns.str.lower()

    with open ('sintomas.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'sintomas', sep='#', null='', columns=columns)

    conexao.commit()

    return 'Importação sintomas concluída!'

print(table_sintomas())

def table_exames():

    df_exames = df[['TOMO_RES', 'TOMO_OUT', 'DT_TOMO',
                        'RAIOX_RES', 'RAIOX_OUT', 'DT_RAIOX']]

    df_exames['RAIOX_OUT'] = df_exames['RAIOX_OUT'].str.replace('"', '')
    
    df_exames.to_csv('exames.csv', sep='@', index=False)

    columns = df_exames.columns.str.lower()

    with open ('exames.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'exames', sep='@', null='', columns=columns)

    conexao.commit()

    return 'Importação exames concluída!'

print(table_exames())

def table_interna_uti():

    df_interna_uti = df[['HOSPITAL', 'NOSOCOMIAL', 'DT_SAIDUTI',
                        'DT_ENTUTI', 'SUPORT_VEN', 'UTI', 'CO_MU_INTE', 'ID_MN_INTE',
                        'CO_RG_INTE', 'ID_RG_INTE', 'SG_UF_INTE', 'DT_INTERNA']]
    
    df_interna_uti.to_csv('interna_uti.csv', sep=';', index=False)

    columns = df_interna_uti.columns.str.lower()

    with open ('interna_uti.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'interna_uti', sep=';', null='', columns=columns)

    conexao.commit()

    return 'Importação interna_uti concluída!'

print(table_interna_uti())

def table_residencia():

    df_residencia = df[['ID_PAIS', 'CO_PAIS', 'SG_UF',
                        'ID_RG_RESI', 'CO_RG_RESI', 'ID_MN_RESI', 'CO_MUN_RES',
                        'CS_ZONA']]
    
    df_residencia.to_csv('residencia.csv', sep=';', index=False)

    columns = df_residencia.columns.str.lower()

    with open ('residencia.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'residencia', sep=';', null='', columns=columns)

    conexao.commit()

    return 'Importação residencia concluída!'

print(table_residencia())

def table_pcr():

    df_pcr = df[['PCR_RESUL', 'DT_PCR', 'POS_PCRFLU','TP_FLU_PCR', 'FLUASU_OUT', 'PCR_FLUASU',
                'PCR_FLUBLI','FLUBLI_OUT', 'POS_PCROUT', 'PCR_SARS2', 'PCR_VSR', 'PCR_PARA1',
                'PCR_PARA2', 'PCR_PARA3', 'PCR_PARA4', 'PCR_ADENO', 'PCR_METAP', 'PCR_BOCA',
                'PCR_RINO', 'PCR_OUTRO', 'DS_PCR_OUT']]
    
    df_pcr.to_csv('pcr.csv', sep=';', index=False)

    columns = df_pcr.columns.str.lower()

    with open ('pcr.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'pcr', sep=';', null='', columns=columns)

    conexao.commit()

    return 'Importação pcr concluída!'

print(table_pcr())

def table_hist_viagem():

    df_hist_viagem = df[['HISTO_VGM', 'PAIS_VGM', 'CO_PS_VGM', 'LO_PS_VGM', 'DT_VGM',
                         'DT_RT_VGM']]
    
    df_hist_viagem.to_csv('hist_viagem.csv', sep='|', index=False)

    columns = df_hist_viagem.columns.str.lower()

    with open ('hist_viagem.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'hist_viagem', sep='|', null='', columns=columns)

    conexao.commit()

    return 'Importação hist_viagem concluída!'

print(table_hist_viagem())

def table_comorbidades():

    df_comorbidades = df[['FATOR_RISC', 'PUERPERA', 'CARDIOPATI', 'HEMATOLOGI', 'SIND_DOWN',
                         'HEPATICA', 'ASMA', 'DIABETES', 'NEUROLOGIC', 'IMUNODEPRE', 'PNEUMOPATI',
                         'RENAL', 'OBESIDADE', 'OBES_IMC', 'OUT_MORBI', 'MORB_DESC']]
    
    df_comorbidades['OBES_IMC'] = df_comorbidades['OBES_IMC'].str.replace(',', '.')
    
    df_comorbidades.to_csv('comorbidades.csv', sep='@', index=False)

    columns = df_comorbidades.columns.str.lower()

    with open ('comorbidades.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'comorbidades', sep='@', null='', columns=columns)

    conexao.commit()

    return 'Importação comorbidades concluída!'

print(table_comorbidades())

def table_antigenico():

    df_antigenico = df[['DT_RES_AN', 'RES_AN', 'POS_AN_OUT', 'AN_SARS2', 'DS_AN_OUT',
                         'AN_VSR', 'AN_OUTRO', 'AN_PARA1', 'AN_PARA2', 'AN_PARA3', 
                         'AN_ADENO', 'TP_FLU_AN', 'POS_AN_FLU', 'TP_TES_AN']]
    
    df_antigenico.to_csv('antigenico.csv', sep=';', index=False)

    columns = df_antigenico.columns.str.lower()

    with open ('antigenico.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'antigenico', sep=';', null='', columns=columns)

    conexao.commit()

    return 'Importação antigenico concluída!'

print(table_antigenico())

def table_sorologia():

    df_sorologia = df[['TP_AM_SOR', 'SOR_OUT', 'DT_CO_SOR', 'TP_SOR', 'OUT_SOR',
                         'DT_RES', 'RES_IGG', 'RES_IGM', 'RES_IGA']]
    
    df_sorologia.to_csv('sorologia.csv', sep=';', index=False)

    columns = df_sorologia.columns.str.lower()

    with open ('sorologia.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'sorologia', sep=';', null='', columns=columns)

    conexao.commit()

    return 'Importação sorologia concluída!'

print(table_sorologia())

def table_amostra():

    df_amostra = df[['AMOSTRA', 'DT_COLETA', 'TP_AMOSTRA', 'OUT_AMOST']]
    
    df_amostra.to_csv('amostra.csv', sep='#', index=False)

    columns = df_amostra.columns.str.lower()

    with open ('amostra.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'amostra', sep='#', null='', columns=columns)

    conexao.commit()

    return 'Importação amostra concluída!'

print(table_amostra())

def table_gripe():

    df_gripe = df[['ANTIVIRAL', 'TP_ANTIVIR', 'OUT_ANTIV', 'DT_ANTIVIR', 'VACINA', 
                   'DT_UT_DOSE']]
    
    df_gripe.to_csv('gripe.csv', sep='|', index=False)

    columns = df_gripe.columns.str.lower()

    with open ('gripe.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'gripe', sep='|', null='', columns=columns)

    conexao.commit()

    return 'Importação gripe concluída!'

print(table_gripe())



