import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import base64
from io import BytesIO

# --- Constantes ---
CODIGO_OPME_PREFIX = '0702'
REINTERNACAO_DIAS_LIMITE = 30
MULTIPLOS_PROCEDIMENTOS_LIMITE = 3
MULTIPLOS_ATOS_PROF_PACIENTE_LIMITE = 2
MULTIPLOS_OPME_AIH_LIMITE = 2
CONCENTRACAO_FORNECEDOR_PERC_LIMITE = 50.0
ALTA_PROPORCAO_SP_SH_LIMITE = 5.0
ALTA_PROPORCAO_OPME_CUSTO_LIMITE = 0.7
CONCENTRACAO_MEDICO_HOSPITAL_PERC_LIMITE = 50.0
LIMITE_FREQ_ATOS_ALTO_CUSTO_PERCENTIL = 0.90
LIMITE_CONCENTRACAO_MEDICO_FORNECEDOR_PERC = 70.0
LIMITE_PERC_PROC_FDS = 30.0
LIMITE_QTD_PROC_FDS_ABS = 3

# --- Configuração Inicial do Aplicativo ---
st.set_page_config(page_title="Auditoria AIH Cardiovascular Avançada", layout="wide")

# --- Funções Auxiliares ---
def format_cnpj(cnpj):
    if pd.isna(cnpj) or cnpj == '':
        return "N/A"
    cnpj_str = str(cnpj).zfill(14)
    return f"{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:8]}/{cnpj_str[8:12]}-{cnpj_str[12:14]}"

def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    writer.save()
    processed_data = output.getvalue()
    return processed_data

def get_table_download_link(df, filename="data.csv", text="Download CSV"):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href

# --- Inicialização do Session State ---
def inicializar_session_state():
    defaults = {
        'dados_file_name': None,
        'fornecedores_file_name': None,
        'df_original': pd.DataFrame(),
        'df_fornecedores': pd.DataFrame(),
        'df_aih_custos': pd.DataFrame(),
        'df_aih_custos_unicos': pd.DataFrame(),
        'df_processado': pd.DataFrame(),
        'df_aih_custos_filtrado': pd.DataFrame(),
        'start_analysis': False,
        'medico_selecionado_detalhe': None,
        'log_qualidade': []
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

inicializar_session_state()


# --- Funções de Processamento de Dados ---
@st.cache_data
def load_and_process_data(dados_content, fornecedores_content, dados_file_name):
    log_qualidade_local = []
    try:
        # Carregar dados principais
        df = pd.read_csv(dados_content, sep=';', encoding='utf-8', decimal=',', thousands='.', dtype=str)
        log_qualidade_local.append(f"Arquivo CSV '{dados_file_name}' carregado. Linhas iniciais: {len(df)}")

        # Carregar dados de fornecedores
        df_fornecedores = pd.read_csv(fornecedores_content, sep=';', encoding='utf-8', dtype=str)
        log_qualidade_local.append(f"Arquivo de fornecedores carregado. Fornecedores únicos: {df_fornecedores['CNPJ'].nunique()}")

        # Processamento dos dados principais
        required_columns = ['SP_NAIH', 'NOME', 'PACCNS', 'DESC_ATO_PROF', 'MEDICO', 'VAL_SH', 'VAL_SP',
                          'SP_ATOPROF', 'SP_VALATO', 'PROC_REA', 'DESC_PROC_REAL', 'SP_DTINTER',
                          'SP_DTSAIDA', 'SP_PJ_DOC', 'SP_NF', 'SP_UF', 'SP_CNES', 'SP_GESTOR', 'SP_AA', 'SP_MM', 'SP_PF_DOC']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(missing_cols)}")

        # Processamento de datas
        for col in ['SP_DTINTER', 'SP_DTSAIDA']:
            df[col] = df[col].astype(str).str.strip().str.replace('-', '/', regex=False)
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

        # Processamento de valores monetários
        for col in ['VAL_SH', 'VAL_SP', 'SP_VALATO']:
            df[col] = df[col].fillna('0').astype(str).str.strip().str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Processamento de texto
        text_cols = ['MEDICO', 'DESC_ATO_PROF', 'DESC_PROC_REAL', 'NOME', 'PACCNS',
                     'PROC_REA', 'SP_ATOPROF', 'SP_PJ_DOC', 'SP_NF', 'SP_UF', 'SP_CNES', 'SP_GESTOR', 'SP_PF_DOC']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
                df[col] = df[col].replace('', np.nan)

        # Alertas sobre duplicidade de pacientes com PACCNS diferentes
        nomes_por_paccns = df.groupby('PACCNS')['NOME'].nunique()
        paccns_multi_nomes = nomes_por_paccns[nomes_por_paccns > 1]
        if not paccns_multi_nomes.empty:
            log_qualidade_local.append(f"Alerta: {len(paccns_multi_nomes)} PACCNS associados a múltiplos nomes distintos.")

        paccns_por_nome = df.groupby('NOME')['PACCNS'].nunique()
        nomes_multi_paccns = paccns_por_nome[paccns_por_nome > 1]
        if not nomes_multi_paccns.empty:
            log_qualidade_local.append(f"Alerta: {len(nomes_multi_paccns)} Nomes associados a múltiplos PACCNS distintos.")

        # Identificar OPMEs
        df['IS_OPME'] = df['SP_ATOPROF'].str.startswith(CODIGO_OPME_PREFIX, na=False)

        # Processar AIHs com valores únicos corretos (considerando SP_PF_DOC != "000000000000000")
        df_aih_custos = df[df['SP_PF_DOC'] != "000000000000000"].groupby('SP_NAIH').first().reset_index()
        df_aih_custos = df_aih_custos[['SP_NAIH', 'VAL_SH', 'VAL_SP', 'SP_DTINTER', 'SP_DTSAIDA', 'PACCNS', 'NOME', 'SP_CNES', 'SP_UF']]

        # Adicionar informações temporais
        df['ANO_INTERNACAO'] = df['SP_DTINTER'].dt.year
        df['MES_ANO_INTERNACAO'] = df['SP_DTINTER'].dt.to_period('M').astype(str)
        df_aih_custos['ANO_INTERNACAO'] = df_aih_custos['SP_DTINTER'].dt.year
        df_aih_custos['MES_ANO_INTERNACAO'] = df_aih_custos['SP_DTINTER'].dt.to_period('M').astype(str)

        st.session_state.log_qualidade = log_qualidade_local
        return df, df_aih_custos, df_fornecedores

    except Exception as e:
        st.error(f"Erro ao processar os dados: {str(e)}")
        st.session_state.log_qualidade.append(f"Erro: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- Funções de Análise de Irregularidades ---
def get_explicacao_alerta(tipo_alerta):
    explicacoes = {
        "reinternacoes_curto_periodo": f"Pacientes com nova internação em menos de {REINTERNACAO_DIAS_LIMITE} dias após a alta da internação anterior. Pode indicar tratamento inadequado, complicações não gerenciadas ou fracionamento indevido de tratamento.",
        "aih_multiplos_procedimentos_dia": f"AIHs com mais de {MULTIPLOS_PROCEDIMENTOS_LIMITE} procedimentos principais (DESC_PROC_REAL) distintos registrados para o mesmo paciente no mesmo dia de internação. Pode indicar cobrança excessiva ou erros de registro.",
        "pacientes_multiplos_atos_profissionais": f"Pacientes com mais de {MULTIPLOS_ATOS_PROF_PACIENTE_LIMITE} atos profissionais (SP_ATOPROF, não OPME) distintos registrados em suas AIHs no período analisado. Pode indicar fragmentação de cuidados ou cobranças múltiplas.",
        "medicos_alta_frequencia_atos_alto_custo": f"Médicos com alta frequência (acima do percentil {LIMITE_FREQ_ATOS_ALTO_CUSTO_PERCENTIL*100:.0f} ou top N) de realização de procedimentos específicos considerados de alto custo. Requer análise para verificar a pertinência e conformidade.",
        "aih_multiplos_opme": f"AIHs com mais de {MULTIPLOS_OPME_AIH_LIMITE} registros de OPME diferentes. Pode indicar uso excessivo ou desnecessário de materiais, ou faturamento fragmentado.",
        "fornecedores_opme_concentrados": f"Fornecedores de OPME que detêm mais de {CONCENTRACAO_FORNECEDOR_PERC_LIMITE}% do valor total de OPME fornecido ao hospital. Pode indicar direcionamento ou falta de cotação.",
        "outliers_custo_opme": "Registros de OPME cujo valor (SP_VALATO) é um outlier estatístico (usando IQR). Pode indicar superfaturamento.",
        "notas_fiscais_opme_duplicadas": "Notas fiscais de OPME (SP_NF) associadas a múltiplas AIHs. Indício de possível faturamento duplicado do mesmo material.",
        "opme_sem_nota_fiscal": "Registros de OPME (identificados pelo prefixo do código SP_ATOPROF) que não possuem um número de Nota Fiscal (SP_NF) associado. A NF é obrigatória para comprovar a aquisição e o custo da OPME.",
        "alta_proporcao_valsp_valsh": f"AIHs onde o valor dos serviços profissionais (VAL_SP) é mais de {ALTA_PROPORCAO_SP_SH_LIMITE} vezes o valor dos serviços hospitalares (VAL_SH). Pode indicar desproporção nos custos ou faturamento inadequado de serviços profissionais.",
        "alta_proporcao_custo_opme_total": f"AIHs onde o custo total das OPMEs (somatório de SP_VALATO para IS_OPME=True) representa mais de {ALTA_PROPORCAO_OPME_CUSTO_LIMITE*100:.0f}% do custo total da AIH (VAL_SH + VAL_SP + OPMEs). Pode indicar que a OPME é o principal direcionador de custo, necessitando de análise de pertinência.",
        "medicos_concentrados_por_hospital": f"Médicos que realizaram mais de {CONCENTRACAO_MEDICO_HOSPITAL_PERC_LIMITE}% do total de AIHs (não OPME) de um determinado hospital (CNES). Pode indicar concentração excessiva de mercado ou dependência do hospital em poucos profissionais.",
        "opme_sem_procedimento_principal_correspondente": "Registros de OPME que, na mesma AIH, não estão acompanhados de um procedimento principal (DESC_PROC_REAL) que tipicamente justificaria seu uso. Ex: Stent sem angioplastia. A lógica de correspondência aqui é simplificada e pode precisar de refinamento com base em tabelas de compatibilidade.",
        "concentracao_medico_fornecedor_opme": f"Médicos que concentram um volume significativo (acima de {LIMITE_CONCENTRACAO_MEDICO_FORNECEDOR_PERC}%) de valor de OPME em poucos fornecedores. Pode indicar direcionamento ou falta de diversidade na aquisição, especialmente se o fornecedor também for concentrado no hospital.",
        "procedimentos_dias_nao_uteis": f"Análise da frequência de procedimentos (DESC_PROC_REAL) realizados em finais de semana (Sábado/Domingo). Procedimentos eletivos com mais de {LIMITE_PERC_PROC_FDS}% de suas ocorrências no FDS e mais de {LIMITE_QTD_PROC_FDS_ABS} casos absolutos no FDS podem requerer justificativa.",
        "analisar_pacientes_duplicados": f"Análise dos pacientes que possuem diferentes nomes para um mesmo PACCNS. Isso pode significar alguma duplicidade no registro de AIHs para um mesmo paciente, mas também pode se referir a homônimos ou erros de digitação (precisa ser verificado com outro documento como, por exemplo, CPF ou RG).",
        "analisar_pacientes_multi_paccns": f"Análise dos pacientes que possuem mais de um PACCNS. Isso pode significar alguma duplicidade no registro de AIHs para um mesmo paciente, mas também pode se referir a homônimos ou erros de digitação (precisa ser verificado com outro documento como, por exemplo, CPF ou RG)."
    }
    return explicacoes.get(tipo_alerta, "Descrição não disponível.")

def analisar_reinternacoes(df_aih_custos_unicos):
    if df_aih_custos_unicos.empty or not {'PACCNS', 'SP_DTINTER', 'SP_DTSAIDA'}.issubset(df_aih_custos_unicos.columns):
        return pd.DataFrame()
    df_pac_datas = df_aih_custos_unicos.sort_values(['PACCNS', 'SP_DTINTER'])
    df_pac_datas['DATA_SAIDA_ANTERIOR'] = df_pac_datas.groupby('PACCNS')['SP_DTSAIDA'].shift(1)
    df_pac_datas['DIAS_ENTRE_INTERNACOES'] = (df_pac_datas['SP_DTINTER'] - df_pac_datas['DATA_SAIDA_ANTERIOR']).dt.days

    reinternacoes = df_pac_datas[
        (df_pac_datas['DIAS_ENTRE_INTERNACOES'].notna()) &
        (df_pac_datas['DIAS_ENTRE_INTERNACOES'] < REINTERNACAO_DIAS_LIMITE) &
        (df_pac_datas['DIAS_ENTRE_INTERNACOES'] >= 0) # Evitar negativos se houver erro de data
    ]
    if not reinternacoes.empty:
        return reinternacoes[['SP_NAIH', 'PACCNS', 'NOME', 'SP_DTINTER', 'DATA_SAIDA_ANTERIOR', 'DIAS_ENTRE_INTERNACOES']]
    return pd.DataFrame()

def analisar_aih_multiplos_procedimentos_dia(df_processado):
    if df_processado.empty or not {'SP_NAIH', 'PACCNS', 'NOME', 'SP_DTINTER', 'DESC_PROC_REAL'}.issubset(df_processado.columns):
        return pd.DataFrame()
    procs_por_aih_dia = df_processado[df_processado['DESC_PROC_REAL'].notna()].groupby(['SP_NAIH', 'PACCNS', 'NOME', 'SP_DTINTER'])['DESC_PROC_REAL'].nunique().reset_index(name='NUM_PROCEDIMENTOS_DISTINTOS')
    suspeitos = procs_por_aih_dia[procs_por_aih_dia['NUM_PROCEDIMENTOS_DISTINTOS'] > MULTIPLOS_PROCEDIMENTOS_LIMITE]
    if not suspeitos.empty:
        return suspeitos.sort_values('NUM_PROCEDIMENTOS_DISTINTOS', ascending=False)
    return pd.DataFrame()


def analisar_pacientes_multiplos_atos_prof(df_processado):
    if df_processado.empty or not {'IS_OPME', 'PACCNS', 'NOME', 'SP_ATOPROF', 'DESC_ATO_PROF'}.issubset(df_processado.columns):
        return pd.DataFrame()

    df_atos_prof = df_processado[~df_processado['IS_OPME'] & df_processado['SP_ATOPROF'].notna()]
    if df_atos_prof.empty:
        return pd.DataFrame()

    pac_agg = df_atos_prof.groupby(['PACCNS', 'NOME']).agg(
        NUM_ATOS_DISTINTOS=('SP_ATOPROF', 'nunique'),
        LISTA_ATOS_PROF_DESC=('DESC_ATO_PROF', lambda x: ', '.join(set(x.astype(str))))
    ).reset_index()

    multiplos_atos = pac_agg[pac_agg['NUM_ATOS_DISTINTOS'] > MULTIPLOS_ATOS_PROF_PACIENTE_LIMITE]
    if not multiplos_atos.empty:
        return multiplos_atos.sort_values('NUM_ATOS_DISTINTOS', ascending=False).head(15) # Top 15
    return pd.DataFrame()


def analisar_medicos_atos_alto_custo(df_processado):
    if df_processado.empty or not {'IS_OPME', 'DESC_ATO_PROF', 'MEDICO', 'SP_ATOPROF', 'SP_VALATO'}.issubset(df_processado.columns):
        return pd.DataFrame()
    atos_alto_custo_desc = [ # Exemplos, idealmente viriam de uma base de conhecimento ou SIGTAP
        'ANGIOPLASTIA CORONARIANA COM IMPLANTE DE STENT',
        'ANGIOPLASTIA CORONARIANA C/ IMPLANTE DE DOIS STENTS', # Exemplo do script original
        'CATETERISMO CARDIACO'
    ]
    df_altocusto = df_processado[
        ~df_processado['IS_OPME'] &
        df_processado['DESC_ATO_PROF'].isin(atos_alto_custo_desc) &
        (df_processado['MEDICO'].notna()) &
        (df_processado['MEDICO'] != 'NÃO SE APLICA') &
        (df_processado['MEDICO'] != 'DESCONHECIDO_OPME')
    ]
    if df_altocusto.empty:
        return pd.DataFrame()

    medicos_contagem = df_altocusto.groupby('MEDICO').agg(
        QTD_ATOS_ALTO_CUSTO=('SP_ATOPROF', 'count'),
        VALOR_TOTAL_ATOS_ALTO_CUSTO=('SP_VALATO', 'sum')
    ).reset_index().sort_values('QTD_ATOS_ALTO_CUSTO', ascending=False)

    if not medicos_contagem.empty:
        limite_freq = medicos_contagem['QTD_ATOS_ALTO_CUSTO'].quantile(LIMITE_FREQ_ATOS_ALTO_CUSTO_PERCENTIL)
        return medicos_contagem[medicos_contagem['QTD_ATOS_ALTO_CUSTO'] >= limite_freq].head(15)
    return pd.DataFrame()

def analisar_aih_multiplos_opme(df_processado):
    if df_processado.empty or not {'IS_OPME', 'SP_NAIH', 'PACCNS', 'NOME', 'SP_ATOPROF'}.issubset(df_processado.columns):
        return pd.DataFrame()
    opme_por_aih = df_processado[df_processado['IS_OPME']].groupby(['SP_NAIH', 'PACCNS', 'NOME'])['SP_ATOPROF'].nunique().reset_index(name='NUM_OPME_DISTINTAS')
    suspeitos = opme_por_aih[opme_por_aih['NUM_OPME_DISTINTAS'] > MULTIPLOS_OPME_AIH_LIMITE]
    if not suspeitos.empty:
        return suspeitos.sort_values('NUM_OPME_DISTINTAS', ascending=False)
    return pd.DataFrame()

def analisar_fornecedores_opme_concentrados(df_processado, df_fornecedores):
    """
    Analisa fornecedores de OPME com alta concentração, adicionando razão social

    Args:
        df_processado: DataFrame com os dados processados
        df_fornecedores: DataFrame com informações dos fornecedores (deve conter CNPJ e RAZAO_SOCIAL)

    Returns:
        DataFrame com fornecedores concentrados e informações adicionais
    """
    # Verifica se as colunas necessárias existem
    required_cols = {'IS_OPME', 'SP_VALATO', 'SP_PJ_DOC', 'SP_NAIH'}
    if df_processado.empty or not required_cols.issubset(df_processado.columns):
        return pd.DataFrame()

    # Filtra apenas OPMEs
    df_opme = df_processado[df_processado['IS_OPME']]

    if df_opme.empty:
        return pd.DataFrame()

    total_opme_valor = df_opme['SP_VALATO'].sum()

    if total_opme_valor > 0:
        # Agrupa por fornecedor
        fornecedor_share = df_opme.groupby('SP_PJ_DOC').agg(
            VALOR_TOTAL_OPME=('SP_VALATO', 'sum'),
            QTD_AIH_FORNECIDAS=('SP_NAIH', 'nunique')
        ).reset_index()

        # Calcula percentual
        fornecedor_share['PERCENTUAL_VALOR'] = (fornecedor_share['VALOR_TOTAL_OPME'] / total_opme_valor * 100).round(2)

        # Formata CNPJ
        fornecedor_share['SP_PJ_DOC_FORMATADO'] = fornecedor_share['SP_PJ_DOC'].apply(format_cnpj)

        # Adiciona razão social (se df_fornecedores estiver disponível)
        if df_fornecedores is not None and not df_fornecedores.empty:
            if 'CNPJ' in df_fornecedores.columns and 'RAZAO_SOCIAL' in df_fornecedores.columns:
                fornecedor_share = fornecedor_share.merge(
                    df_fornecedores[['CNPJ', 'RAZAO_SOCIAL']],
                    left_on='SP_PJ_DOC',
                    right_on='CNPJ',
                    how='left'
                ).drop(columns=['CNPJ'])

        # Filtra fornecedores concentrados
        concentrados = fornecedor_share[fornecedor_share['PERCENTUAL_VALOR'] > CONCENTRACAO_FORNECEDOR_PERC_LIMITE]

        if not concentrados.empty:
            # Reorganiza as colunas para colocar RAZAO_SOCIAL após SP_PJ_DOC
            col_order = ['SP_PJ_DOC', 'SP_PJ_DOC_FORMATADO']
            if 'RAZAO_SOCIAL' in concentrados.columns:
                col_order.append('RAZAO_SOCIAL')
            col_order.extend(['VALOR_TOTAL_OPME', 'QTD_AIH_FORNECIDAS', 'PERCENTUAL_VALOR'])

            return concentrados[col_order].sort_values('PERCENTUAL_VALOR', ascending=False)

    return pd.DataFrame()

def analisar_outliers_custo_opme(df_processado):
    if df_processado.empty or not {'IS_OPME', 'SP_VALATO'}.issubset(df_processado.columns):
        return pd.DataFrame()
    opme_data = df_processado[df_processado['IS_OPME'] & (df_processado['SP_VALATO'] > 0)]
    if not opme_data.empty:
        Q1 = opme_data['SP_VALATO'].quantile(0.25)
        Q3 = opme_data['SP_VALATO'].quantile(0.75)
        IQR = Q3 - Q1
        limite_superior = Q3 + 1.5 * IQR
        # limite_inferior = Q1 - 1.5 * IQR # Custo não deve ser negativo

        outliers = opme_data[opme_data['SP_VALATO'] > limite_superior]
        if not outliers.empty:
            outliers_cols = ['SP_NAIH', 'NOME', 'MEDICO', 'DESC_ATO_PROF', 'SP_VALATO', 'SP_PJ_DOC', 'SP_NF']
            result = outliers[outliers_cols].copy()
            result['SP_PJ_DOC_FORMATADO'] = result['SP_PJ_DOC'].apply(format_cnpj)
            return result.sort_values('SP_VALATO', ascending=False)
    return pd.DataFrame()

def analisar_nf_duplicadas_opme(df_processado, df_fornecedores=None):
    """
    Identifica notas fiscais de OPME duplicadas por fornecedor com valor total

    Args:
        df_processado: DataFrame com os dados processados
        df_fornecedores: DataFrame com informações dos fornecedores

    Returns:
        DataFrame com:
        - Razão Social
        - CNPJ formatado
        - Nota Fiscal
        - Número de AIHs associadas
        - Valor total das OPMEs
        - Lista de AIHs (resumida)
    """
    # Verifica colunas necessárias
    required_cols = {'IS_OPME', 'SP_NF', 'SP_NAIH', 'SP_PJ_DOC', 'SP_VALATO'}
    if df_processado.empty or not required_cols.issubset(df_processado.columns):
        return pd.DataFrame()

    # Filtra OPMEs com NF válida
    opme_com_nf = df_processado[
        df_processado['IS_OPME'] &
        df_processado['SP_NF'].notna() &
        (df_processado['SP_NF'] != "N/A") &
        df_processado['SP_PJ_DOC'].notna()
    ]

    if opme_com_nf.empty:
        return pd.DataFrame()

    # Agrupa por fornecedor e NF
    nf_por_fornecedor = opme_com_nf.groupby(['SP_PJ_DOC', 'SP_NF']).agg(
        NUM_AIH_ASSOCIADAS=('SP_NAIH', 'nunique'),
        VALOR_TOTAL_OPME=('SP_VALATO', 'sum'),
        AIH_LISTA=('SP_NAIH', lambda x: sorted(x.unique()))
    ).reset_index()

    # Filtra NFs duplicadas (com mais de uma AIH)
    duplicadas = nf_por_fornecedor[nf_por_fornecedor['NUM_AIH_ASSOCIADAS'] > 1]

    if duplicadas.empty:
        return pd.DataFrame()

    # Adiciona razão social
    if df_fornecedores is not None and not df_fornecedores.empty:
        if 'CNPJ' in df_fornecedores.columns and 'RAZAO_SOCIAL' in df_fornecedores.columns:
            duplicadas = duplicadas.merge(
                df_fornecedores[['CNPJ', 'RAZAO_SOCIAL']].drop_duplicates(),
                left_on='SP_PJ_DOC',
                right_on='CNPJ',
                how='left'
            ).drop(columns=['CNPJ'])

    # Formatação
    duplicadas['CNPJ_FORMATADO'] = duplicadas['SP_PJ_DOC'].apply(format_cnpj)
    duplicadas['VALOR_TOTAL_OPME'] = duplicadas['VALOR_TOTAL_OPME'].round(2)

    # Formata lista de AIHs para exibição
    duplicadas['AIH_ASSOCIADAS'] = duplicadas.apply(
        lambda x: f"{x['NUM_AIH_ASSOCIADAS']} AIHs ({', '.join(map(str, x['AIH_LISTA'][:3]))}" +
                 ("..." if len(x['AIH_LISTA']) > 3 else "") + ")",
        axis=1
    )

    # Ordem das colunas
    col_order = []
    if 'RAZAO_SOCIAL' in duplicadas.columns:
        col_order.append('RAZAO_SOCIAL')

    col_order.extend([
        'CNPJ_FORMATADO',
        'SP_NF',
        'NUM_AIH_ASSOCIADAS',
        'VALOR_TOTAL_OPME',
        'AIH_ASSOCIADAS'
    ])

    # Remove colunas auxiliares
    duplicadas = duplicadas.drop(columns=['AIH_LISTA', 'SP_PJ_DOC'])

    return duplicadas[col_order].sort_values('VALOR_TOTAL_OPME', ascending=False)


def analisar_opme_sem_nf(df_processado):
    if df_processado.empty or not {'IS_OPME', 'SP_NF'}.issubset(df_processado.columns):
        return pd.DataFrame()
    sem_nf = df_processado[df_processado['IS_OPME'] & (df_processado['SP_NF'].isna() | (df_processado['SP_NF'] == "N/A"))]
    if not sem_nf.empty:
        result_cols = ['SP_NAIH', 'NOME', 'MEDICO', 'DESC_ATO_PROF', 'SP_VALATO', 'SP_PJ_DOC']
        result = sem_nf[result_cols].copy()
        result['SP_PJ_DOC_FORMATADO'] = result['SP_PJ_DOC'].apply(format_cnpj)
        return result
    return pd.DataFrame()

def analisar_alta_proporcao_valsp_valsh(df_aih_custos_unicos):
    if df_aih_custos_unicos.empty or not {'VAL_SP', 'VAL_SH'}.issubset(df_aih_custos_unicos.columns):
        return pd.DataFrame()
    df_ratio = df_aih_custos_unicos.copy()
    df_ratio['RATIO_SP_SH'] = df_ratio['VAL_SP'] / df_ratio['VAL_SH'].replace(0, np.nan)
    altas_proporcoes = df_ratio[df_ratio['RATIO_SP_SH'] > ALTA_PROPORCAO_SP_SH_LIMITE]
    if not altas_proporcoes.empty:
        return altas_proporcoes[['SP_NAIH', 'NOME', 'VAL_SH', 'VAL_SP', 'RATIO_SP_SH']].sort_values('RATIO_SP_SH', ascending=False)
    return pd.DataFrame()

def analisar_alta_proporcao_custo_opme_total(df_processado, df_aih_custos_unicos):
    if df_processado.empty or df_aih_custos_unicos.empty or \
       not {'IS_OPME', 'SP_NAIH', 'SP_VALATO'}.issubset(df_processado.columns) or \
       not {'SP_NAIH', 'VAL_SH', 'VAL_SP', 'NOME'}.issubset(df_aih_custos_unicos.columns):
        return pd.DataFrame()

    custo_opme_por_aih = df_processado[df_processado['IS_OPME']].groupby('SP_NAIH')['SP_VALATO'].sum().reset_index(name='CUSTO_TOTAL_OPME_AIH')
    if custo_opme_por_aih.empty:
        return pd.DataFrame()

    df_merged = df_aih_custos_unicos.merge(custo_opme_por_aih, on='SP_NAIH', how='left').fillna({'CUSTO_TOTAL_OPME_AIH': 0})
    df_merged['CUSTO_TOTAL_AIH_CALC'] = df_merged['VAL_SH'] + df_merged['VAL_SP'] + df_merged['CUSTO_TOTAL_OPME_AIH']
    df_merged['RATIO_OPME_TOTAL'] = df_merged['CUSTO_TOTAL_OPME_AIH'] / df_merged['CUSTO_TOTAL_AIH_CALC'].replace(0, np.nan)

    altas_proporcoes = df_merged[df_merged['RATIO_OPME_TOTAL'] > ALTA_PROPORCAO_OPME_CUSTO_LIMITE]
    if not altas_proporcoes.empty:
        cols_res = ['SP_NAIH', 'NOME', 'CUSTO_TOTAL_OPME_AIH', 'CUSTO_TOTAL_AIH_CALC', 'RATIO_OPME_TOTAL']
        return altas_proporcoes[cols_res].sort_values('RATIO_OPME_TOTAL', ascending=False)
    return pd.DataFrame()

def analisar_medicos_concentrados_hospital(df_processado, df_aih_custos_unicos):
    if df_processado.empty or df_aih_custos_unicos.empty or \
       not {'IS_OPME', 'MEDICO', 'SP_CNES', 'SP_NAIH'}.issubset(df_processado.columns) or \
       not {'SP_CNES', 'SP_NAIH'}.issubset(df_aih_custos_unicos.columns):
        return pd.DataFrame()

    aih_nao_opme_medico_cnes = df_processado[
        ~df_processado['IS_OPME'] &
        (df_processado['MEDICO'].notna()) &
        (df_processado['MEDICO'] != 'NÃO SE APLICA') &
        (df_processado['MEDICO'] != 'DESCONHECIDO_OPME') &
        df_processado['SP_CNES'].notna()
    ].groupby(['SP_CNES', 'MEDICO'])['SP_NAIH'].nunique().reset_index(name='NUM_AIH_MEDICO_HOSP')

    total_aih_cnes = df_aih_custos_unicos[df_aih_custos_unicos['SP_CNES'].notna()].groupby('SP_CNES')['SP_NAIH'].nunique().reset_index(name='TOTAL_AIH_HOSPITAL')

    if aih_nao_opme_medico_cnes.empty or total_aih_cnes.empty:
        return pd.DataFrame()

    merged_data = aih_nao_opme_medico_cnes.merge(total_aih_cnes, left_on='SP_CNES', right_on='SP_CNES', how="left") # Left join para manter todos os médicos
    if not merged_data.empty and 'TOTAL_AIH_HOSPITAL' in merged_data.columns:
        merged_data['PERCENTUAL_AIH_MEDICO'] = (merged_data['NUM_AIH_MEDICO_HOSP'] / merged_data['TOTAL_AIH_HOSPITAL'].replace(0,np.nan) * 100).round(2)
        concentrados = merged_data[merged_data['PERCENTUAL_AIH_MEDICO'] > CONCENTRACAO_MEDICO_HOSPITAL_PERC_LIMITE]
        if not concentrados.empty:
            return concentrados[['SP_CNES', 'MEDICO', 'NUM_AIH_MEDICO_HOSP', 'TOTAL_AIH_HOSPITAL', 'PERCENTUAL_AIH_MEDICO']].sort_values(['SP_CNES', 'PERCENTUAL_AIH_MEDICO'], ascending=[True, False])
    return pd.DataFrame()

def analisar_opme_sem_proc_correspondente(df_processado):
    if df_processado.empty or not {'IS_OPME', 'SP_ATOPROF', 'SP_NAIH', 'DESC_PROC_REAL'}.issubset(df_processado.columns):
        return pd.DataFrame()

    opme_proc_map = {
        CODIGO_OPME_PREFIX: ['ANGIOPLASTIA', 'STENT', 'CATETERISMO', 'REVASCULARIZACAO', 'IMPLANTE', 'ENDOPROTES', 'ABLAÇÃO', 'MARCAPASSO'],
    }

    df_opme_analise = df_processado[df_processado['IS_OPME']].copy()
    if df_opme_analise.empty:
        return pd.DataFrame()

    # Para cada AIH, obter a lista de procedimentos principais realizados
    procedimentos_por_aih = df_processado[df_processado['DESC_PROC_REAL'].notna()].groupby('SP_NAIH')['DESC_PROC_REAL'].apply(lambda x: list(set(x.str.upper()))).to_dict()

    def checar_procedimento_compativel(row):
        cod_ato_opme_row = str(row['SP_ATOPROF'])
        aih_row = row['SP_NAIH']

        lista_procs_aih = procedimentos_por_aih.get(aih_row, [])
        if not lista_procs_aih: # Se não há procedimentos principais na AIH, OPME é suspeita
            return False

        for prefixo_opme_map, palavras_chave_proc_map in opme_proc_map.items():
            if cod_ato_opme_row.startswith(prefixo_opme_map):
                for desc_proc_aih_upper in lista_procs_aih:
                    if any(palavra_chave.upper() in desc_proc_aih_upper for palavra_chave in palavras_chave_proc_map):
                        return True # Encontrou procedimento compatível
                return False # Não encontrou para este tipo de OPME
        return True # OPME não mapeada, assume compatível por ora (ou poderia ser False se quisermos ser mais estritos)

    df_opme_analise['PROC_COMPATIVEL_ENCONTRADO'] = df_opme_analise.apply(checar_procedimento_compativel, axis=1)

    mismatched = df_opme_analise[~df_opme_analise['PROC_COMPATIVEL_ENCONTRADO']]
    if not mismatched.empty:
        cols_res = ['SP_NAIH', 'NOME', 'MEDICO', 'SP_ATOPROF', 'DESC_ATO_PROF', 'SP_VALATO']
        return mismatched[cols_res]
    return pd.DataFrame()

def analisar_concentracao_medico_fornecedor_opme(df_processado, df_fornecedores=None):
    """
    Analisa concentração de médicos com fornecedores de OPME, incluindo razão social do fornecedor

    Args:
        df_processado: DataFrame com os dados processados
        df_fornecedores: DataFrame com informações dos fornecedores (deve conter CNPJ e RAZAO_SOCIAL)

    Returns:
        DataFrame com médicos e fornecedores concentrados, incluindo razão social
    """
    # Verifica colunas necessárias
    required_cols = {'IS_OPME', 'MEDICO', 'SP_PJ_DOC', 'SP_VALATO', 'SP_ATOPROF'}
    if df_processado.empty or not required_cols.issubset(df_processado.columns):
        return pd.DataFrame()

    # Filtra dados relevantes
    df_med_forn = df_processado[
        df_processado['IS_OPME'] &
        (df_processado['MEDICO'].notna()) &
        (df_processado['MEDICO'] != 'DESCONHECIDO_OPME') &
        (df_processado['MEDICO'] != 'NÃO SE APLICA') &
        df_processado['SP_PJ_DOC'].notna()
    ]

    if df_med_forn.empty:
        return pd.DataFrame()

    # Agrupa por médico e fornecedor
    df_med_forn = df_med_forn.groupby(['MEDICO', 'SP_PJ_DOC']).agg(
        VALOR_TOTAL_OPME=('SP_VALATO', 'sum'),
        QTD_OPME_REGISTROS=('SP_ATOPROF', 'count')
    ).reset_index()

    # Calcula total por médico
    total_opme_por_medico = df_med_forn.groupby('MEDICO')['VALOR_TOTAL_OPME'].sum().reset_index(name='TOTAL_OPME_GERAL_MEDICO')
    df_med_forn = df_med_forn.merge(total_opme_por_medico, on='MEDICO', how="left")

    if 'TOTAL_OPME_GERAL_MEDICO' not in df_med_forn.columns:
        return pd.DataFrame()

    # Calcula percentual
    df_med_forn['PERC_FORNECEDOR_PARA_MEDICO'] = (
        df_med_forn['VALOR_TOTAL_OPME'] /
        df_med_forn['TOTAL_OPME_GERAL_MEDICO'].replace(0, np.nan) * 100
    ).round(2)

    # Filtra concentrações significativas
    concentracao = df_med_forn[
        (df_med_forn['PERC_FORNECEDOR_PARA_MEDICO'] > LIMITE_CONCENTRACAO_MEDICO_FORNECEDOR_PERC) &
        (df_med_forn['VALOR_TOTAL_OPME'] > df_med_forn['VALOR_TOTAL_OPME'].quantile(0.50))
    ].sort_values(['MEDICO', 'PERC_FORNECEDOR_PARA_MEDICO'], ascending=[True, False])

    if concentracao.empty:
        return pd.DataFrame()

    # Adiciona razão social se df_fornecedores estiver disponível
    if df_fornecedores is not None and not df_fornecedores.empty:
        if 'CNPJ' in df_fornecedores.columns and 'RAZAO_SOCIAL' in df_fornecedores.columns:
            concentracao = concentracao.merge(
                df_fornecedores[['CNPJ', 'RAZAO_SOCIAL']].drop_duplicates(),
                left_on='SP_PJ_DOC',
                right_on='CNPJ',
                how='left'
            ).drop(columns=['CNPJ'])

    # Formata CNPJ e organiza colunas
    concentracao['SP_PJ_DOC_FORMATADO'] = concentracao['SP_PJ_DOC'].apply(format_cnpj)

    # Define ordem das colunas
    col_order = ['MEDICO', 'SP_PJ_DOC', 'SP_PJ_DOC_FORMATADO']
    if 'RAZAO_SOCIAL' in concentracao.columns:
        col_order.append('RAZAO_SOCIAL')
    col_order.extend([
        'VALOR_TOTAL_OPME', 'QTD_OPME_REGISTROS',
        'TOTAL_OPME_GERAL_MEDICO', 'PERC_FORNECEDOR_PARA_MEDICO'
    ])

    return concentracao[col_order]


def analisar_procedimentos_dias_nao_uteis(df_processado):
    if df_processado.empty or not {'SP_DTINTER', 'DESC_PROC_REAL', 'SP_NAIH'}.issubset(df_processado.columns):
        return pd.DataFrame()

    df_com_dia_semana = df_processado[df_processado['SP_DTINTER'].notna()].copy()
    # Ensure SP_DTINTER is datetime
    if not pd.api.types.is_datetime64_any_dtype(df_com_dia_semana['SP_DTINTER']):
        df_com_dia_semana['SP_DTINTER'] = pd.to_datetime(df_com_dia_semana['SP_DTINTER'], errors='coerce')
        df_com_dia_semana.dropna(subset=['SP_DTINTER'], inplace=True)


    df_com_dia_semana['DIA_SEMANA'] = df_com_dia_semana['SP_DTINTER'].dt.day_name()

    aih_proc_dia_semana = df_com_dia_semana[df_com_dia_semana['DESC_PROC_REAL'].notna()].groupby(
        ['DESC_PROC_REAL', 'DIA_SEMANA']
    )['SP_NAIH'].nunique().reset_index(name='QTD_AIH_UNICAS')

    if aih_proc_dia_semana.empty:
        return pd.DataFrame()

    pivot_dias_semana = aih_proc_dia_semana.pivot_table( # Usar pivot_table para lidar com duplicatas se houver
        index='DESC_PROC_REAL', columns='DIA_SEMANA', values='QTD_AIH_UNICAS', aggfunc='sum'
    ).fillna(0)

    dias_ordem = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_dias_semana = pivot_dias_semana.reindex(columns=[dia for dia in dias_ordem if dia in pivot_dias_semana.columns], fill_value=0)

    if 'Saturday' in pivot_dias_semana.columns and 'Sunday' in pivot_dias_semana.columns:
        pivot_dias_semana['TOTAL_FDS'] = pivot_dias_semana.get('Saturday', 0) + pivot_dias_semana.get('Sunday', 0)
        # Calcular total da semana corretamente
        pivot_dias_semana['TOTAL_SEMANA_CALC'] = pivot_dias_semana.sum(axis=1) - pivot_dias_semana['TOTAL_FDS'] # Subtrai o próprio FDS se ele foi somado no total_semana
        pivot_dias_semana['TOTAL_SEMANA_CALC'] += pivot_dias_semana['TOTAL_FDS'] # Re-adiciona para ter o total real

        pivot_dias_semana['PERC_FDS'] = (pivot_dias_semana['TOTAL_FDS'] / pivot_dias_semana['TOTAL_SEMANA_CALC'].replace(0,np.nan) * 100).round(2)

        suspeitos_fds = pivot_dias_semana[
            (pivot_dias_semana['PERC_FDS'] > LIMITE_PERC_PROC_FDS) & (pivot_dias_semana['TOTAL_FDS'] > LIMITE_QTD_PROC_FDS_ABS)
        ].sort_values('PERC_FDS', ascending=False)
        if not suspeitos_fds.empty:
            return suspeitos_fds.reset_index()
    return pd.DataFrame()

def calcular_custo_total_aih(df_processado, df_aih_custos_unicos):
    """Calcula o custo total por AIH considerando SH, SP e OPME"""
    if df_processado.empty or df_aih_custos_unicos.empty:
        return pd.DataFrame()

    # Calcular custo total de OPME por AIH
    custo_opme_por_aih = df_processado[df_processado['IS_OPME']].groupby('SP_NAIH')['SP_VALATO'].sum().reset_index(name='CUSTO_TOTAL_OPME_AIH')

    # Juntar com os custos de SH e SP (já únicos por AIH)
    df_custo_total = df_aih_custos_unicos.merge(custo_opme_por_aih, on='SP_NAIH', how='left').fillna({'CUSTO_TOTAL_OPME_AIH': 0})

    # Calcular custo total
    df_custo_total['CUSTO_TOTAL_AIH'] = df_custo_total['VAL_SH'] + df_custo_total['VAL_SP'] + df_custo_total['CUSTO_TOTAL_OPME_AIH']

    return df_custo_total[['SP_NAIH', 'NOME', 'PACCNS', 'VAL_SH', 'VAL_SP', 'CUSTO_TOTAL_OPME_AIH', 'CUSTO_TOTAL_AIH']]

def analisar_pacientes_duplicados(df_processado):
    """
    Identifica pacientes com PACCNS associados a múltiplos nomes distintos

    Args:
        df_processado: DataFrame com os dados processados

    Returns:
        DataFrame com PACCNS, NOMES associados, AIHs e médicos envolvidos
    """
    if df_processado.empty or not {'PACCNS', 'NOME', 'SP_NAIH', 'MEDICO'}.issubset(df_processado.columns):
        return pd.DataFrame()

    # Encontra PACCNS com múltiplos nomes
    paccns_duplicados = df_processado.groupby('PACCNS')['NOME'].nunique().reset_index()
    paccns_duplicados = paccns_duplicados[paccns_duplicados['NOME'] > 1]['PACCNS']

    if paccns_duplicados.empty:
        return pd.DataFrame()

    # Filtra os dados originais para esses PACCNS
    df_duplicados = df_processado[df_processado['PACCNS'].isin(paccns_duplicados)]

    # Define as funções lambda separadamente para maior clareza
    def format_aihs(x):
        items = sorted(x.unique()[:3])
        return ', '.join(items) + ('...' if len(x.unique()) > 3 else '')

    def format_medicos(x):
        items = sorted(x.unique()[:3])
        return ', '.join(items) + ('...' if len(x.unique()) > 3 else '')

    # Agrupa para mostrar a relação PACCNS-NOME com exemplos de AIHs e médicos
    resultado = df_duplicados.groupby(['PACCNS', 'NOME']).agg(
        AIHS=('SP_NAIH', format_aihs),
        MEDICOS=('MEDICO', format_medicos),
        QTD_AIHS=('SP_NAIH', 'nunique')
    ).reset_index()

    return resultado.sort_values(['PACCNS', 'QTD_AIHS'], ascending=[True, False])

def analisar_pacientes_multi_paccns(df_processado):
    """
    Identifica pacientes com múltiplos PACCNS associados

    Args:
        df_processado: DataFrame com os dados processados

    Returns:
        DataFrame com NOME, PACCNS, AIHs e médicos envolvidos
    """
    if df_processado.empty or not {'NOME', 'PACCNS', 'SP_NAIH', 'MEDICO'}.issubset(df_processado.columns):
        return pd.DataFrame()

    # Encontra pacientes com múltiplos PACCNS
    pacientes_multi_paccns = df_processado.groupby('NOME')['PACCNS'].nunique().reset_index()
    pacientes_multi_paccns = pacientes_multi_paccns[pacientes_multi_paccns['PACCNS'] > 1]['NOME']

    if pacientes_multi_paccns.empty:
        return pd.DataFrame()

    # Filtra os dados originais para esses pacientes
    df_duplicados = df_processado[df_processado['NOME'].isin(pacientes_multi_paccns)]

    # Agrupa para mostrar a relação NOME-PACCNS com exemplos de AIHs e médicos
    resultado = df_duplicados.groupby(['NOME', 'PACCNS']).agg(
        AIHS=('SP_NAIH', lambda x: ', '.join(sorted(x.unique()[:3])) + ('...' if len(x.unique()) > 3 else '')),
        MEDICOS=('MEDICO', lambda x: ', '.join(sorted(x.unique()[:3])) + ('...' if len(x.unique()) > 3 else '')),
        QTD_AIHS=('SP_NAIH', 'nunique')
    ).reset_index()

    return resultado.sort_values(['NOME', 'QTD_AIHS'], ascending=[True, False])


# --- Interface Streamlit ---
st.title("🔍 Auditoria Avançada de AIHs Cardiovasculares (SUS)")
st.markdown("Esta ferramenta realiza uma análise detalhada de AIHs para procedimentos cardiovasculares, visando identificar padrões e potenciais irregularidades.")

# Upload dos Arquivos
st.sidebar.header("Carregar Dados")
uploaded_dados = st.sidebar.file_uploader("Upload do arquivo CSV (Dados.csv)", type=["csv"], key="dados_upload")
uploaded_fornecedores = st.sidebar.file_uploader("Upload do arquivo de Fornecedores (Fornecedores.csv)", type=["csv"], key="fornecedores_upload")

if uploaded_dados is not None and uploaded_fornecedores is not None:
    if (st.session_state.dados_file_name != uploaded_dados.name or
        st.session_state.fornecedores_file_name != uploaded_fornecedores.name or
        not st.session_state.start_analysis):

        st.session_state.dados_file_name = uploaded_dados.name
        st.session_state.fornecedores_file_name = uploaded_fornecedores.name
        st.session_state.start_analysis = False

        with st.spinner("Processando os dados... Por favor, aguarde."):
            df_loaded, df_aih_costs_loaded, df_fornecedores_loaded = load_and_process_data(
                uploaded_dados, uploaded_fornecedores, uploaded_dados.name
            )

        if not df_loaded.empty and not df_aih_costs_loaded.empty and not df_fornecedores_loaded.empty:
            st.session_state.df_original = df_loaded
            st.session_state.df_fornecedores = df_fornecedores_loaded
            st.session_state.df_aih_custos = df_aih_costs_loaded
            st.session_state.df_aih_custos_unicos = df_aih_costs_loaded
            st.session_state.start_analysis = True
            st.sidebar.success("Dados carregados e processados com sucesso!")
        else:
            st.sidebar.error("Falha ao carregar ou processar os dados. Verifique o log.")
            st.session_state.start_analysis = False
else:
    if st.session_state.start_analysis:
        st.info("⬅️ Por favor, carregue ambos os arquivos (Dados e Fornecedores) para iniciar a análise.")
    else:
        st.info("⬅️ Por favor, carregue os arquivos na barra lateral para iniciar a análise.")

if not st.session_state.start_analysis or st.session_state.df_original.empty:
    st.stop()

# --- Filtros Globais ---
st.sidebar.header("Filtros Globais")

# Obter anos disponíveis
anos_disponiveis = sorted(st.session_state.df_original['ANO_INTERNACAO'].dropna().unique().astype(int))
ano_selecionado_min, ano_selecionado_max = st.sidebar.select_slider(
    "Período (Ano da Internação):",
    options=anos_disponiveis,
    value=(min(anos_disponiveis), max(anos_disponiveis))
)
# Usa os dados originais carregados para popular os filtros
df_original_para_filtros = st.session_state.df_original
df_aih_custos_para_filtros = st.session_state.get('df_aih_custos_unicos', pd.DataFrame())

medicos_para_filtro_raw = df_original_para_filtros[
    (df_original_para_filtros['MEDICO'].notna()) &
    (df_original_para_filtros['MEDICO'] != 'NÃO SE APLICA') &
    (df_original_para_filtros['MEDICO'] != 'DESCONHECIDO_OPME')
]['MEDICO'].unique()
medicos_disponiveis = ['Todos'] + sorted(list(medicos_para_filtro_raw))
medico_filtrado = st.sidebar.selectbox("Médico Específico:", medicos_disponiveis, index=0)

procedimentos_disponiveis = ['Todos'] + sorted(list(df_original_para_filtros['DESC_PROC_REAL'].dropna().unique()))
proc_filtrado = st.sidebar.selectbox("Procedimento Principal (DESC_PROC_REAL):", procedimentos_disponiveis, index=0)

cnes_disponiveis = ['Todos'] + sorted(list(df_original_para_filtros['SP_CNES'].dropna().unique()))
cnes_filtrado = st.sidebar.selectbox("Hospital (CNES):", cnes_disponiveis, index=0)

# Aplicar filtros e armazenar no session_state para uso nas abas
df_filtrado_temp = st.session_state.df_original.copy()
df_aih_custos_filtrado_temp = st.session_state.df_aih_custos_unicos.copy()

# Filtragem por ano
if 'ANO_INTERNACAO' in df_filtrado_temp.columns:
    df_filtrado_temp = df_filtrado_temp[
        (df_filtrado_temp['ANO_INTERNACAO'] >= ano_selecionado_min) &
        (df_filtrado_temp['ANO_INTERNACAO'] <= ano_selecionado_max)
    ]
if 'ANO_INTERNACAO' in df_aih_custos_filtrado_temp.columns:
    df_aih_custos_filtrado_temp = df_aih_custos_filtrado_temp[
        (df_aih_custos_filtrado_temp['ANO_INTERNACAO'] >= ano_selecionado_min) &
        (df_aih_custos_filtrado_temp['ANO_INTERNACAO'] <= ano_selecionado_max)
    ]

if medico_filtrado != 'Todos':
    df_filtrado_temp = df_filtrado_temp[df_filtrado_temp['MEDICO'] == medico_filtrado]
    aihs_do_medico = st.session_state.df_original[st.session_state.df_original['MEDICO'] == medico_filtrado]['SP_NAIH'].unique()
    df_aih_custos_filtrado_temp = df_aih_custos_filtrado_temp[df_aih_custos_filtrado_temp['SP_NAIH'].isin(aihs_do_medico)]

if proc_filtrado != 'Todos':
    df_filtrado_temp = df_filtrado_temp[df_filtrado_temp['DESC_PROC_REAL'] == proc_filtrado]
    aihs_do_proc = st.session_state.df_original[st.session_state.df_original['DESC_PROC_REAL'] == proc_filtrado]['SP_NAIH'].unique()
    df_aih_custos_filtrado_temp = df_aih_custos_filtrado_temp[df_aih_custos_filtrado_temp['SP_NAIH'].isin(aihs_do_proc)]

if cnes_filtrado != 'Todos':
    df_filtrado_temp = df_filtrado_temp[df_filtrado_temp['SP_CNES'] == cnes_filtrado]
    if 'SP_CNES' in df_aih_custos_filtrado_temp.columns:
      df_aih_custos_filtrado_temp = df_aih_custos_filtrado_temp[df_aih_custos_filtrado_temp['SP_CNES'] == cnes_filtrado]

st.session_state.df_processado = df_filtrado_temp
st.session_state.df_aih_custos_filtrado = df_aih_custos_filtrado_temp

# --- Abas de Análise ---
tab_geral, tab_procedimentos, tab_medicos, tab_pacientes, tab_opme, tab_alertas, tab_detalhe_medico, tab_log = st.tabs([
    "📊 Visão Geral", "🩺 Procedimentos", "🧑‍⚕️ Médicos", "🧍 Pacientes", "🔩 OPME",
    "🚨 Alertas de Auditoria", "👨‍🔬 Análise por Médico", "📋 Log de Qualidade"
])

with tab_geral:
    st.header("📊 Visão Geral dos Dados")

    if st.session_state.df_processado.empty or st.session_state.df_aih_custos_filtrado.empty:
        st.warning("Nenhum dado disponível para análise com os filtros atuais.")
    else:
        # Cálculo dos custos totais
        total_sh = st.session_state.df_aih_custos_filtrado['VAL_SH'].sum()
        total_sp = st.session_state.df_aih_custos_filtrado['VAL_SP'].sum()
        total_opme = st.session_state.df_processado[st.session_state.df_processado['IS_OPME']]['SP_VALATO'].sum()
        custo_total = total_sh + total_sp + total_opme

        # Seção de métricas resumidas - Linha 1 (indicadores existentes)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total de AIHs", len(st.session_state.df_aih_custos_filtrado['SP_NAIH'].unique()))
        col2.metric("Total de Pacientes", len(st.session_state.df_aih_custos_filtrado['PACCNS'].unique()))
        col3.metric("Total de Hospitais", len(st.session_state.df_aih_custos_filtrado['SP_CNES'].unique()))
        col4.metric("Total de Médicos", len(st.session_state.df_processado['MEDICO'].unique()))

        # Seção de métricas resumidas - Linha 2 (novos indicadores de custo)
        col5, col6, col7, col8 = st.columns(4)
        col5.metric("Custo Hospitalar (SH)", f"R$ {total_sh:,.2f}",
                   help="Total de Serviços Hospitalares")
        col6.metric("Custo Profissional (SP)", f"R$ {total_sp:,.2f}",
                   help="Total de Serviços Profissionais")
        col7.metric("Custo com OPME", f"R$ {total_opme:,.2f}",
                   help="Total de Materiais e OPMEs")
        col8.metric("Custo Total", f"R$ {custo_total:,.2f}",
                   help="Soma de todos os custos (SH + SP + OPME)")

        # Seção de Evolução Temporal (mantida igual)
        st.subheader("🔍 Evolução Temporal")

        # Gráfico 1: AIHs Únicas por Mês
        st.markdown("### AIHs Únicas por Mês")
        df_aih_mes = st.session_state.df_aih_custos_filtrado.groupby('MES_ANO_INTERNACAO')['SP_NAIH'].nunique().reset_index(name='QTD_AIH')

        fig1 = px.line(df_aih_mes, x='MES_ANO_INTERNACAO', y='QTD_AIH',
                      title='Quantidade de AIHs por Mês',
                      labels={'MES_ANO_INTERNACAO': 'Mês/Ano', 'QTD_AIH': 'Quantidade de AIHs'},
                      markers=True)
        fig1.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig1, use_container_width=True)

        # Gráfico 2: Custos Totais (SH, SP, OPME) por Mês
        st.markdown("### Custos Totais por Mês")

        # Calcular custos totais por mês
        df_custos_mes = st.session_state.df_aih_custos_filtrado.groupby('MES_ANO_INTERNACAO').agg(
            VALOR_SH=('VAL_SH', 'sum'),
            VALOR_SP=('VAL_SP', 'sum')
        ).reset_index()

        # Calcular custo OPME por mês
        df_opme_mes = st.session_state.df_processado[st.session_state.df_processado['IS_OPME']].groupby('MES_ANO_INTERNACAO')['SP_VALATO'].sum().reset_index(name='VALOR_OPME')

        # Juntar os dados
        df_custos_mes = df_custos_mes.merge(df_opme_mes, on='MES_ANO_INTERNACAO', how='left').fillna(0)

        # Criar gráfico de linhas para os custos
        fig2 = px.line(df_custos_mes, x='MES_ANO_INTERNACAO',
                      y=['VALOR_SH', 'VALOR_SP', 'VALOR_OPME'],
                      title='Evolução dos Custos por Mês',
                      labels={'MES_ANO_INTERNACAO': 'Mês/Ano', 'value': 'Valor (R$)', 'variable': 'Tipo de Custo'},
                      markers=True)
        fig2.update_layout(yaxis_tickprefix='R$ ', yaxis_tickformat=',.2f',
                          xaxis_tickangle=-45)
        st.plotly_chart(fig2, use_container_width=True)

        # Botão para download dos dados
        st.markdown("### Baixar Dados")
        if st.button('Exportar Dados para CSV'):
            # Criar DataFrame consolidado para exportação
            df_export = df_aih_mes.merge(df_custos_mes, on='MES_ANO_INTERNACAO')
            csv = df_export.to_csv(index=False).encode('utf-8')

            st.download_button(
                label="Baixar dados completos (CSV)",
                data=csv,
                file_name="evolucao_temporal_aih.csv",
                mime="text/csv"
            )

        # Seção de distribuição por UF (mantida da versão anterior)
        st.subheader("Distribuição por UF")
        df_uf = st.session_state.df_aih_custos_filtrado['SP_UF'].value_counts().reset_index()
        df_uf.columns = ['UF', 'Quantidade']
        fig3 = px.bar(df_uf, x='UF', y='Quantidade', title='AIHs por UF')
        st.plotly_chart(fig3, use_container_width=True)

with tab_procedimentos:
    st.header("🩺 Análise de Procedimentos")

    if st.session_state.df_processado.empty:
        st.warning("Nenhum dado disponível para análise com os filtros atuais.")
    else:
        # Seção 1: Gráficos existentes (mantidos)
        #col1, col2 = st.columns(2)
        #with col1:
        st.subheader("🏆Procedimentos Mais Frequentes")
        df_procedimentos = st.session_state.df_processado['DESC_PROC_REAL'].value_counts().head(15).reset_index()
        df_procedimentos.columns = ['Procedimento', 'Quantidade']
        fig1 = px.bar(df_procedimentos, y='Procedimento', x='Quantidade',
            orientation='h', title='Top 15 Procedimentos')
        fig1.update_traces(texttemplate='%{x}', textposition='outside')
        fig1.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig1, use_container_width=True)

        # Botão para download dos dados
        st.download_button(
            label="Baixar Dados (CSV)",
            data=df_procedimentos.to_csv(index=False).encode('utf-8'),
            file_name="procedimentos_frequentes.csv",
            mime="text/csv"
        )

        #with col2:
        st.subheader("💰Custo Médio por Procedimento")
        df_custo_proc = st.session_state.df_processado.groupby('DESC_PROC_REAL')['SP_VALATO'].mean().nlargest(15).reset_index()
        df_custo_proc.columns = ['Procedimento', 'Custo Médio']
        fig2 = px.bar(df_custo_proc, y='Procedimento', x='Custo Médio',
                     orientation='h', title='Top 15 Procedimentos por Custo Médio')
        fig2.update_traces(texttemplate='R$ %{x:,.2f}', textposition='outside')
        fig2.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig2, use_container_width=True)

        # Botão para download dos dados
        st.download_button(
            label="Baixar Dados (CSV)",
            data=df_custo_proc.to_csv(index=False).encode('utf-8'),
            file_name="custo_medio_procedimentos.csv",
            mime="text/csv"
        )

        # Seção 2: Novo gráfico - Tempo Médio de Internação por Procedimento
        st.subheader("⏱ Tempo Médio de Internação por Procedimento")

        # Calcular tempo médio de internação em dias
        df_tempo_internacao = st.session_state.df_processado.copy()
        df_tempo_internacao['TEMPO_INTERNACAO'] = (df_tempo_internacao['SP_DTSAIDA'] - df_tempo_internacao['SP_DTINTER']).dt.days

        # Agrupar por procedimento e calcular tempo médio
        df_tempo_medio = df_tempo_internacao.groupby('DESC_PROC_REAL')['TEMPO_INTERNACAO'].mean().sort_values(ascending=False).head(10).reset_index()
        df_tempo_medio.columns = ['Procedimento', 'DIAS_MEDIOS']
        df_tempo_medio['DIAS_MEDIOS'] = df_tempo_medio['DIAS_MEDIOS'].round(1)

        fig3 = px.bar(df_tempo_medio,
                     x='DIAS_MEDIOS',
                     y='Procedimento',
                     orientation='h',
                     title='Top 10 Procedimentos por Tempo Médio de Internação (dias)',
                     labels={'DIAS_MEDIOS': 'Dias Médios', 'Procedimento': 'Procedimento'})

        # Adicionar valor exato em cada barra
        fig3.update_traces(texttemplate='%{x:.1f} dias', textposition='outside')
        fig3.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig3, use_container_width=True)

        # Botão para download dos dados
        st.download_button(
            label="Baixar Dados (CSV)",
            data=df_tempo_medio.to_csv(index=False).encode('utf-8'),
            file_name="tempo_medio_internacao.csv",
            mime="text/csv"
        )

        # Seção 3: Novo gráfico - Evolução da Frequência de Procedimentos
        st.subheader("📈 Evolução da Frequência de Procedimentos")

        # Identificar os top 5 procedimentos mais frequentes
        top_procedimentos = st.session_state.df_processado['DESC_PROC_REAL'].value_counts().head(5).index.tolist()

        # Filtrar apenas os top 5 e agrupar por mês/ano
        df_evolucao = st.session_state.df_processado[st.session_state.df_processado['DESC_PROC_REAL'].isin(top_procedimentos)]
        df_evolucao = df_evolucao.groupby(['MES_ANO_INTERNACAO', 'DESC_PROC_REAL'])['SP_NAIH'].nunique().reset_index(name='QTD_AIH')

        # Criar gráfico de linhas
        fig4 = px.line(df_evolucao,
                      x='MES_ANO_INTERNACAO',
                      y='QTD_AIH',
                      color='DESC_PROC_REAL',
                      title='Frequência Mensal dos Top 5 Procedimentos',
                      labels={'MES_ANO_INTERNACAO': 'Mês/Ano', 'QTD_AIH': 'Quantidade de AIHs', 'DESC_PROC_REAL': 'Procedimento'},
                      markers=True)

        fig4.update_layout(xaxis_tickangle=-45, legend_title_text='Procedimento')
        st.plotly_chart(fig4, use_container_width=True)

        # Botão para download dos dados
        st.download_button(
            label="Baixar Dados (CSV)",
            data=df_evolucao.to_csv(index=False).encode('utf-8'),
            file_name="evolucao_procedimentos.csv",
            mime="text/csv"
        )

with tab_medicos:
    st.header("🧑‍⚕️ Análise de Médicos")
    df_proc_tab = st.session_state.df_processado
    df_aih_custos_tab = st.session_state.df_aih_custos_filtrado

    if df_proc_tab.empty:
        st.warning("Nenhum dado de médico para exibir com os filtros globais atuais.")
    else:
        # Filtro para médicos válidos
        df_medicos_validos = df_proc_tab[
            (df_proc_tab['MEDICO'].notna()) &
            (df_proc_tab['MEDICO'] != 'NÃO SE APLICA') &
            (df_proc_tab['MEDICO'] != 'DESCONHECIDO_OPME')
        ]

        if df_medicos_validos.empty:
            st.info("Nenhum médico válido encontrado após filtros.")
        else:
            # Seção 1: Gráficos existentes (mantidos)
            st.subheader("Custo Médio por AIH por Médico")
            df_custo_medico = calcular_custo_total_aih(df_proc_tab, df_aih_custos_tab)
            df_custo_medico = df_custo_medico.merge(
                df_proc_tab[['SP_NAIH', 'MEDICO']].drop_duplicates(),
                on='SP_NAIH', how='left'
            )

            custo_medio_por_medico = df_custo_medico.groupby('MEDICO')['CUSTO_TOTAL_AIH'].mean().nlargest(10).reset_index()
            custo_medio_por_medico.columns = ['Médico', 'Custo Médio por AIH']

            fig1 = px.bar(custo_medio_por_medico, y='Médico', x='Custo Médio por AIH',
                         orientation='h', title='Top 10 Médicos com Maior Custo Médio por AIH')
            st.plotly_chart(fig1, use_container_width=True)

            # Seção 2: Novos gráficos solicitados
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("🏆 Top 10 Médicos por Quantidade de AIHs")
                df_medicos_aih = df_medicos_validos.groupby('MEDICO')['SP_NAIH'].nunique().nlargest(10).reset_index()
                df_medicos_aih.columns = ['Médico', 'QTD_AIH']

                fig2 = px.bar(df_medicos_aih,
                             x='QTD_AIH',
                             y='Médico',
                             orientation='h',
                             title='Quantidade de AIHs Únicas',
                             labels={'QTD_AIH': 'Quantidade de AIHs', 'Médico': ''})

                fig2.update_traces(texttemplate='%{x}', textposition='outside')
                fig2.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig2, use_container_width=True)

                # Botão para download dos dados
                st.download_button(
                    label="Baixar Dados de Quantidade (CSV)",
                    data=df_medicos_aih.to_csv(index=False).encode('utf-8'),
                    file_name="medicos_qtd_aih.csv",
                    mime="text/csv",
                    key="download_qtd"
                )

            with col2:
                st.subheader("💰 Top 10 Médicos por Custo Total")
                df_custo_total = df_custo_medico.groupby('MEDICO')['CUSTO_TOTAL_AIH'].sum().nlargest(10).reset_index()
                df_custo_total.columns = ['Médico', 'CUSTO_TOTAL_AIH']

                fig3 = px.bar(df_custo_total,
                             x='CUSTO_TOTAL_AIH',
                             y='Médico',
                             orientation='h',
                             title='Custo Total das AIHs (R$)',
                             labels={'CUSTO_TOTAL_AIH': 'Custo Total (R$)', 'Médico': ''})

                fig3.update_traces(texttemplate='R$ %{x:,.2f}', textposition='outside')
                fig3.update_layout(yaxis={'categoryorder':'total ascending'},
                                  xaxis_tickprefix='R$ ', xaxis_tickformat=',.2f')
                st.plotly_chart(fig3, use_container_width=True)

                # Botão para download dos dados
                st.download_button(
                    label="Baixar Dados de Custo (CSV)",
                    data=df_custo_total.to_csv(index=False).encode('utf-8'),
                    file_name="medicos_custo_total.csv",
                    mime="text/csv",
                    key="download_custo"
                )

            # Botão de download mantido para compatibilidade
            st.markdown(get_table_download_link(custo_medio_por_medico, "top_medicos_custo_medio.csv"), unsafe_allow_html=True)


with tab_pacientes:
    st.header("🧍 Análise de Pacientes")
    df_aih_custos_tab = st.session_state.df_aih_custos_filtrado
    df_processado_tab = st.session_state.df_processado

    if df_aih_custos_tab.empty:
        st.warning("Nenhum dado de paciente para exibir com os filtros globais atuais.")
    else:
        # Seção 1: Gráficos existentes (mantidos)
        st.subheader("Internações por Paciente")
        df_reinternacoes = df_aih_custos_tab.groupby(['PACCNS', 'NOME'])['SP_NAIH'].nunique().nlargest(10).reset_index()
        df_reinternacoes.columns = ['PACCNS', 'Nome do Paciente', 'Quantidade de Internações']

        fig1 = px.bar(df_reinternacoes, y='Nome do Paciente', x='Quantidade de Internações',
                     orientation='h', title='Pacientes com Mais Internações')
        st.plotly_chart(fig1, use_container_width=True)

        # Seção 2: Novos gráficos solicitados
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🏆 Top 10 Pacientes por Quantidade de AIHs")
            df_pacientes_aih = df_aih_custos_tab.groupby(['PACCNS', 'NOME'])['SP_NAIH'].nunique().nlargest(10).reset_index()
            df_pacientes_aih.columns = ['PACCNS', 'Paciente', 'QTD_AIH']

            fig2 = px.bar(df_pacientes_aih,
                         x='QTD_AIH',
                         y='Paciente',
                         orientation='h',
                         title='Quantidade de AIHs Únicas',
                         labels={'QTD_AIH': 'Quantidade de AIHs', 'Paciente': ''})

            fig2.update_traces(texttemplate='%{x}', textposition='outside')
            fig2.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig2, use_container_width=True)

            # Botão para download dos dados
            st.download_button(
                label="Baixar Dados de Quantidade (CSV)",
                data=df_pacientes_aih.to_csv(index=False).encode('utf-8'),
                file_name="pacientes_qtd_aih.csv",
                mime="text/csv",
                key="download_qtd_pac"
            )

        with col2:
            st.subheader("💰 Top 10 Pacientes por Custo Total")
            # Calcular custo total por paciente (SH + SP + OPME)
            custo_aih = df_aih_custos_tab.groupby(['PACCNS', 'NOME']).agg(
                VALOR_SH=('VAL_SH', 'sum'),
                VALOR_SP=('VAL_SP', 'sum')
            ).reset_index()

            custo_opme = df_processado_tab[df_processado_tab['IS_OPME']].groupby('PACCNS')['SP_VALATO'].sum().reset_index(name='VALOR_OPME')

            df_custo_pacientes = custo_aih.merge(custo_opme, on='PACCNS', how='left').fillna(0)
            df_custo_pacientes['CUSTO_TOTAL'] = df_custo_pacientes['VALOR_SH'] + df_custo_pacientes['VALOR_SP'] + df_custo_pacientes['VALOR_OPME']

            top_custo = df_custo_pacientes.nlargest(10, 'CUSTO_TOTAL')[['NOME', 'CUSTO_TOTAL']]
            top_custo.columns = ['Paciente', 'CUSTO_TOTAL']

            fig3 = px.bar(top_custo,
                         x='CUSTO_TOTAL',
                         y='Paciente',
                         orientation='h',
                         title='Custo Total das AIHs (R$)',
                         labels={'CUSTO_TOTAL': 'Custo Total (R$)', 'Paciente': ''})

            fig3.update_traces(texttemplate='R$ %{x:,.2f}', textposition='outside')
            fig3.update_layout(yaxis={'categoryorder':'total ascending'},
                              xaxis_tickprefix='R$ ', xaxis_tickformat=',.2f')
            st.plotly_chart(fig3, use_container_width=True)

            # Botão para download dos dados
            st.download_button(
                label="Baixar Dados de Custo (CSV)",
                data=top_custo.to_csv(index=False).encode('utf-8'),
                file_name="pacientes_custo_total.csv",
                mime="text/csv",
                key="download_custo_pac"
            )

        # Seção 3: Frequência de Reinternações
        st.subheader("🔄 Frequência de Reinternações")

        # Identificar reinternações (pacientes com mais de 1 AIH)
        df_reint = df_aih_custos_tab.groupby(['PACCNS', 'NOME'])['SP_NAIH'].nunique().reset_index(name='QTD_REINTERNACOES')
        df_reint = df_reint[df_reint['QTD_REINTERNACOES'] > 1].nlargest(10, 'QTD_REINTERNACOES')
        df_reint['QTD_REINTERNACOES'] = df_reint['QTD_REINTERNACOES'] - 1  # Ajuste para contar reinternações

        fig4 = px.bar(df_reint,
                     x='QTD_REINTERNACOES',
                     y='NOME',
                     orientation='h',
                     title='Top 10 Pacientes por Reinternações',
                     labels={'QTD_REINTERNACOES': 'Quantidade de Reinternações', 'NOME': 'Paciente'})

        fig4.update_traces(texttemplate='%{x}', textposition='outside')
        fig4.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig4, use_container_width=True)

        # Botão para download dos dados
        st.download_button(
            label="Baixar Dados de Reinternações (CSV)",
            data=df_reint.to_csv(index=False).encode('utf-8'),
            file_name="pacientes_reinternacoes.csv",
            mime="text/csv",
            key="download_reint"
        )

        # Botão de download mantido para compatibilidade
        st.markdown(get_table_download_link(df_reinternacoes, "top_pacientes_reinternacoes.csv"), unsafe_allow_html=True)


with tab_opme:
    st.header("🔩 Análise de OPME")
    df_proc_tab = st.session_state.df_processado
    df_fornecedores = st.session_state.df_fornecedores

    if df_proc_tab.empty or not df_proc_tab['IS_OPME'].any():
        st.warning("Nenhum dado de OPME para exibir com os filtros globais atuais.")
    else:
        df_opme_tab = df_proc_tab[df_proc_tab['IS_OPME']]

        # Container for Top 10 OPMEs por custo total with download button
        with st.container():
            st.subheader("Top 10 OPMEs por Custo Total")
            top_opme_custo = df_opme_tab.groupby('DESC_ATO_PROF')['SP_VALATO'].sum().nlargest(10).reset_index()
            top_opme_custo.columns = ['OPME', 'Custo Total']

            fig = px.bar(top_opme_custo, y='OPME', x='Custo Total',
                         orientation='h', title='Top 10 OPMEs por Custo Total')
            st.plotly_chart(fig, use_container_width=True)

            # Download button for this section
            st.download_button(
                label="Baixar Dados (CSV)",
                data=top_opme_custo.to_csv(index=False, sep=';', decimal=',').encode('utf-8'),
                file_name="top_opme_custo.csv",
                mime="text/csv"
            )

        # Container for Top 10 OPMEs por custo médio with download button
        with st.container():
            st.subheader("Custo Médio de OPME por Tipo")
            st.markdown("### Top 10 OPMEs por Custo Médio")

            custo_medio_opme = df_opme_tab.groupby('DESC_ATO_PROF')['SP_VALATO'].mean().nlargest(10).reset_index()
            custo_medio_opme.columns = ['OPME', 'Custo Médio']

            fig = px.bar(custo_medio_opme, y='OPME', x='Custo Médio',
                         orientation='h', title='Top 10 OPMEs por Custo Médio')
            st.plotly_chart(fig, use_container_width=True)

            # Download button for this section
            st.download_button(
                label="Baixar Dados (CSV)",
                data=custo_medio_opme.to_csv(index=False, sep=';', decimal=',').encode('utf-8'),
                file_name="custo_medio_opme.csv",
                mime="text/csv"
            )

        # Container for Top fornecedores de OPME with download button
        with st.container():
            st.subheader("Fornecedores de OPME")

            df_fornecedores_opme = df_opme_tab.merge(
                df_fornecedores,
                left_on='SP_PJ_DOC',
                right_on='CNPJ',
                how='left'
            )

            top_fornecedores = df_fornecedores_opme.groupby(['SP_PJ_DOC', 'RAZAO_SOCIAL'])['SP_VALATO'].sum().nlargest(20).reset_index()
            top_fornecedores.columns = ['CNPJ', 'Razão Social', 'Custo Total']

            # Gráfico top 10
            st.markdown("### Top 10 Fornecedores de OPME por Custo Total")
            fig = px.bar(top_fornecedores.head(10), y='Razão Social', x='Custo Total',
                         orientation='h', title='Top 10 Fornecedores de OPME por Custo Total')
            st.plotly_chart(fig, use_container_width=True)

            # Tabela top 20
            st.markdown("### Top 20 Fornecedores de OPME")
            st.dataframe(top_fornecedores)

            # Download button for this section
            st.download_button(
                label="Baixar Dados (CSV)",
                data=top_fornecedores.to_csv(index=False, sep=';', decimal=',').encode('utf-8'),
                file_name="top_fornecedores_opme.csv",
                mime="text/csv"
            )


with tab_alertas:
    st.header("🚨 Alertas de Auditoria")
    st.markdown("Esta seção destaca potenciais irregularidades. Cada alerta deve ser investigado individualmente.")

    df_alerta_proc = st.session_state.df_processado
    df_alerta_aih_custos = st.session_state.df_aih_custos_filtrado
    df_fornecedores = st.session_state.df_fornecedores

    if df_alerta_proc.empty and df_alerta_aih_custos.empty:  # If BOTH base dataframes are empty
        st.warning("Não há dados para gerar alertas com os filtros globais atuais.")
    else:
        # Dictionary mapping alert keys to their analysis functions
        # Call functions with filtered data
        mapa_alertas = {
            "reinternacoes_curto_periodo": analisar_reinternacoes(df_alerta_aih_custos),
            "aih_multiplos_procedimentos_dia": analisar_aih_multiplos_procedimentos_dia(df_alerta_proc),
            "pacientes_multiplos_atos_profissionais": analisar_pacientes_multiplos_atos_prof(df_alerta_proc),
            "medicos_alta_frequencia_atos_alto_custo": analisar_medicos_atos_alto_custo(df_alerta_proc),
            "aih_multiplos_opme": analisar_aih_multiplos_opme(df_alerta_proc),
            "fornecedores_opme_concentrados": analisar_fornecedores_opme_concentrados(df_alerta_proc, df_fornecedores),
            "outliers_custo_opme": analisar_outliers_custo_opme(df_alerta_proc),
            "notas_fiscais_opme_duplicadas": analisar_nf_duplicadas_opme(df_alerta_proc, df_fornecedores),
            "opme_sem_nota_fiscal": analisar_opme_sem_nf(df_alerta_proc),
            "alta_proporcao_valsp_valsh": analisar_alta_proporcao_valsp_valsh(df_alerta_aih_custos),
            "alta_proporcao_custo_opme_total": analisar_alta_proporcao_custo_opme_total(df_alerta_proc, df_alerta_aih_custos),
            "medicos_concentrados_por_hospital": analisar_medicos_concentrados_hospital(df_alerta_proc, df_alerta_aih_custos),
            "opme_sem_procedimento_principal_correspondente": analisar_opme_sem_proc_correspondente(df_alerta_proc),
            "concentracao_medico_fornecedor_opme": analisar_concentracao_medico_fornecedor_opme(df_alerta_proc, df_fornecedores),
            "procedimentos_dias_nao_uteis": analisar_procedimentos_dias_nao_uteis(df_alerta_proc),
            "analisar_pacientes_duplicados": analisar_pacientes_duplicados(df_alerta_proc),
            "analisar_pacientes_multi_paccns": analisar_pacientes_multi_paccns(df_alerta_proc),
        }

        alertas_found_overall = False
        for alert_key, alert_result_df in mapa_alertas.items():
            # Check if the result is a DataFrame and is not empty
            if isinstance(alert_result_df, pd.DataFrame) and not alert_result_df.empty:
                alertas_found_overall = True
                # Generate a more readable title from the key
                pretty_title = alert_key.replace("_", " ").title()
                st.subheader(f"⚠️ {pretty_title}")
                st.dataframe(alert_result_df)

                # Add a download button for the DataFrame as CSV
                csv_data = alert_result_df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 Baixar dados como CSV",
                    data=csv_data,
                    file_name=f"alerta_{alert_key}.csv",
                    mime="text/csv",
                    key=f"download_{alert_key}"  # Unique key for each button
                )

                with st.expander("O que este alerta significa?"):
                    st.markdown(get_explicacao_alerta(alert_key))
                st.markdown("---")

        if not alertas_found_overall:
            st.info("Nenhum alerta específico identificado com os filtros e regras atuais. Verifique se os dados filtrados não estão vazios ou ajuste os limiares de alerta se necessário.")


with tab_detalhe_medico:
    st.header("👨‍🔬 Análise Detalhada por Médico")

    df_proc_tab_detalhe = st.session_state.df_processado
    df_aih_custos_tab_detalhe = st.session_state.df_aih_custos_filtrado
    df_fornecedores = st.session_state.df_fornecedores  # Assuming this is available in session state

    if df_proc_tab_detalhe.empty:
        st.warning("Nenhum dado disponível para análise detalhada de médicos com os filtros globais atuais.")
    else:
        medicos_validos_detalhe_raw = df_proc_tab_detalhe[
            (df_proc_tab_detalhe['MEDICO'].notna()) &
            (df_proc_tab_detalhe['MEDICO'] != 'NÃO SE APLICA') &
            (df_proc_tab_detalhe['MEDICO'] != 'DESCONHECIDO_OPME')
        ]['MEDICO'].unique()
        medicos_disponiveis_detalhe = sorted(list(medicos_validos_detalhe_raw))

        if not medicos_disponiveis_detalhe:
            st.warning("Nenhum médico disponível para seleção com os filtros globais atuais.")
        else:
            default_medico_index = 0
            if medico_filtrado != 'Todos' and medico_filtrado in medicos_disponiveis_detalhe:
                default_medico_index = medicos_disponiveis_detalhe.index(medico_filtrado)
            elif st.session_state.medico_selecionado_detalhe and st.session_state.medico_selecionado_detalhe in medicos_disponiveis_detalhe:
                default_medico_index = medicos_disponiveis_detalhe.index(st.session_state.medico_selecionado_detalhe)

            medico_selecionado_atual = st.selectbox(
                "Selecione um médico para análise detalhada:",
                options=medicos_disponiveis_detalhe,
                index=default_medico_index,
                key="medico_select_detalhe_key"
            )
            st.session_state.medico_selecionado_detalhe = medico_selecionado_atual

            if st.session_state.medico_selecionado_detalhe:
                st.markdown(f"**Analisando Médico: {st.session_state.medico_selecionado_detalhe}** (Considerando filtros globais já aplicados)")

                # Filter data for selected doctor
                dados_medico = df_proc_tab_detalhe[df_proc_tab_detalhe['MEDICO'] == st.session_state.medico_selecionado_detalhe]
                aihs_medico = dados_medico['SP_NAIH'].unique()
                dados_aih_custos_medico = df_aih_custos_tab_detalhe[df_aih_custos_tab_detalhe['SP_NAIH'].isin(aihs_medico)]

                if dados_medico.empty:
                    st.info(f"Nenhum dado encontrado para o médico '{st.session_state.medico_selecionado_detalhe}' com os filtros globais atuais.")
                else:
                    # Metrics
                    total_aih_medico = len(aihs_medico)
                    val_sh_medico = dados_aih_custos_medico['VAL_SH'].sum() if not dados_aih_custos_medico.empty else 0
                    val_sp_medico = dados_aih_custos_medico['VAL_SP'].sum() if not dados_aih_custos_medico.empty else 0
                    val_opme_medico = dados_medico[dados_medico['IS_OPME']]['SP_VALATO'].sum()

                    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                    m_col1.metric("AIHs Únicas do Médico", f"{total_aih_medico}")
                    m_col2.metric("Total SH (AIHs do Médico)", f"R$ {val_sh_medico:,.2f}")
                    m_col3.metric("Total SP (AIHs do Médico)", f"R$ {val_sp_medico:,.2f}")
                    m_col4.metric("Total OPME (Atribuído ao Médico)", f"R$ {val_opme_medico:,.2f}")

                    # 1. Top Procedures Visualization
                    with st.container():
                        st.subheader("Procedimentos Realizados")
                        st.markdown("### Top Procedimentos do Médico")

                        top_procedimentos = dados_medico['DESC_PROC_REAL'].value_counts().nlargest(5).reset_index()
                        top_procedimentos.columns = ['Procedimento', 'Frequência']

                        fig = px.bar(top_procedimentos, x='Frequência', y='Procedimento',
                                    orientation='h', title=f'Top 5 Procedimentos - Dr. {st.session_state.medico_selecionado_detalhe}')
                        st.plotly_chart(fig, use_container_width=True)

                        st.download_button(
                            label="Baixar Dados de Procedimentos (CSV)",
                            data=top_procedimentos.to_csv(index=False, sep=';', decimal=',').encode('utf-8'),
                            file_name=f"top_procedimentos_{st.session_state.medico_selecionado_detalhe}.csv",
                            mime="text/csv"
                        )

                    # 2. OPME Suppliers Visualization
                    if 'IS_OPME' in dados_medico.columns and dados_medico['IS_OPME'].any():
                        with st.container():
                            st.subheader("Fornecedores de OPME Utilizados")
                            st.markdown("### Fornecedores de OPME por Valor Total")

                            # Merge with supplier data to get company name
                            df_fornecedores_opme = dados_medico[dados_medico['IS_OPME']].merge(
                                df_fornecedores,
                                left_on='SP_PJ_DOC',
                                right_on='CNPJ',
                                how='left'
                            )

                            fornecedores_valor = df_fornecedores_opme.groupby('RAZAO_SOCIAL')['SP_VALATO'].sum().nlargest(10).reset_index()
                            fornecedores_valor.columns = ['Fornecedor', 'Valor Total']

                            fig = px.bar(fornecedores_valor, x='Valor Total', y='Fornecedor',
                                        orientation='h', title=f'Top Fornecedores de OPME - Dr. {st.session_state.medico_selecionado_detalhe}')
                            st.plotly_chart(fig, use_container_width=True)

                            st.download_button(
                                label="Baixar Dados de Fornecedores (CSV)",
                                data=fornecedores_valor.to_csv(index=False, sep=';', decimal=',').encode('utf-8'),
                                file_name=f"fornecedores_opme_{st.session_state.medico_selecionado_detalhe}.csv",
                                mime="text/csv"
                            )
                    else:
                        st.info(f"O médico {st.session_state.medico_selecionado_detalhe} não possui procedimentos com OPME nos filtros atuais.")

                    # 3. Temporal Analysis
                    st.subheader("Evolução Temporal das Atividades do Médico")
                    if not dados_aih_custos_medico.empty and 'MES_ANO_INTERNACAO' in dados_aih_custos_medico.columns:
                        aih_medico_mes = dados_aih_custos_medico.groupby('MES_ANO_INTERNACAO')['SP_NAIH'].nunique().reset_index(name='QTD_AIH')
                        if not aih_medico_mes.empty:
                            fig = px.line(aih_medico_mes, x='MES_ANO_INTERNACAO', y='QTD_AIH',
                                         title='AIHs Únicas do Médico por Mês', markers=True, text='QTD_AIH')
                            fig.update_traces(textposition="bottom right")
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("Sem dados temporais de AIH para este médico.")
                    else:
                        st.info("Dados de AIH do médico ou coluna 'MES_ANO_INTERNACAO' ausentes para gráfico temporal.")

                    # 4. Complete AIH Records Table
                    with st.container():
                        st.subheader("Todos os Registros de AIH do Médico")

                        # Definir a ordem das colunas desejada
                        column_order = [
                            'SP_NAIH',       # Número da AIH
                            'NOME', # Nome do paciente (novo)
                            'DATA_INTERNACAO',
                            'DATA_ALTA',
                            'DESC_PROC_REAL',
                            'DESC_ATO_PROF',
                            'SP_VALATO',
                            'IS_OPME',
                            'SP_PJ_DOC',
                            'RAZAO_SOCIAL'   # Razão Social do fornecedor
                        ]

                        # Filtrar apenas colunas existentes no DataFrame
                        available_columns = [col for col in column_order if col in dados_medico.columns]

                        if not available_columns:
                            st.warning("Nenhuma coluna de dados disponível para exibição.")
                        else:
                            df_aih_completo = dados_medico[available_columns].copy()

                            # Adicionar Razão Social se existir relação com fornecedores
                            if 'SP_PJ_DOC' in df_aih_completo.columns and df_fornecedores is not None:
                                if 'CNPJ' in df_fornecedores.columns and 'RAZAO_SOCIAL' in df_fornecedores.columns:
                                    df_aih_completo = df_aih_completo.merge(
                                        df_fornecedores[['CNPJ', 'RAZAO_SOCIAL']].drop_duplicates(),
                                        left_on='SP_PJ_DOC',
                                        right_on='CNPJ',
                                        how='left'
                                    ).drop(columns=['CNPJ'])

                            # Formatação dos dados
                            if 'DATA_INTERNACAO' in df_aih_completo.columns:
                                df_aih_completo['DATA_INTERNACAO'] = df_aih_completo['DATA_INTERNACAO'].dt.strftime('%d/%m/%Y')

                            if 'DATA_ALTA' in df_aih_completo.columns:
                                df_aih_completo['DATA_ALTA'] = df_aih_completo['DATA_ALTA'].dt.strftime('%d/%m/%Y')

                            if 'SP_VALATO' in df_aih_completo.columns:
                                df_aih_completo['SP_VALATO'] = df_aih_completo['SP_VALATO'].apply(
                                    lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else ""
                                )

                            # Garantir a ordem correta das colunas
                            final_columns = []
                            for col in column_order:
                                if col in df_aih_completo.columns:
                                    final_columns.append(col)

                            # Exibir tabela
                            st.dataframe(df_aih_completo[final_columns])

                            # Botão de download
                            st.download_button(
                                label="Baixar Todos os Registros (CSV)",
                                data=df_aih_completo[final_columns].to_csv(index=False, sep=';', decimal=',').encode('utf-8'),
                                file_name=f"todos_registros_{st.session_state.medico_selecionado_detalhe}.csv",
                                mime="text/csv"
                            )
            else:
                st.info("Selecione um médico para ver a análise detalhada.")

with tab_log:
    st.header("📋 Log de Qualidade dos Dados")

    if not st.session_state.log_qualidade:
        st.info("Nenhum registro no log de qualidade.")
    else:
        st.subheader("Mensagens do Processamento")
        for mensagem in st.session_state.log_qualidade:
            if mensagem.startswith("Erro:"):
                st.error(mensagem)
            elif mensagem.startswith("Alerta:"):
                st.warning(mensagem)
            else:
                st.info(mensagem)

        st.subheader("Estatísticas dos Dados")
        if not st.session_state.df_processado.empty:
            st.write(f"Total de registros processados: {len(st.session_state.df_processado)}")
            st.write(f"Total de AIHs únicas: {len(st.session_state.df_processado['SP_NAIH'].unique())}")
            st.write(f"Total de OPMEs: {len(st.session_state.df_processado[st.session_state.df_processado['IS_OPME']])}")
            st.write(f"Total de valores ausentes em campos críticos:")

            campos_criticos = ['SP_NAIH', 'PACCNS', 'DESC_PROC_REAL', 'SP_DTINTER', 'SP_DTSAIDA']
            df_ausentes = pd.DataFrame({
                'Campo': campos_criticos,
                'Valores Ausentes': [st.session_state.df_processado[col].isna().sum() for col in campos_criticos]
            })
            st.dataframe(df_ausentes)

# --- Disclaimer ---
st.sidebar.markdown("---")
st.sidebar.caption(f"Auditoria AIH Cardiovascular Avançada. Versão 1.1 - {datetime.now().strftime('%Y-%m-%d')}")
st.sidebar.caption("Desenvolvido como ferramenta de auxílio à auditoria. Conclusões requerem análise manual.")
