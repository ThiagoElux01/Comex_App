# services/duas_utils.py
import re
import pandas as pd
from datetime import datetime

# ---------------------- Funções de transformação (migradas/adaptadas) ----------------------

def add_declaracion_column(df):
    df['Declaracion'] = ''
    for index, row in df.iterrows():
        if 'No ORDEN' in str(row.get('CONCEPTO', '')):
            for col in df.columns:
                if col not in ['source_file', 'CONCEPTO', 'Error', 'Declaracion', 'Fecha', 'Ad_Valorem',
                               'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas', 'Percepcion', 'PEC']:
                    if 'Declaraci' in str(row.get(col, '')):
                        df.at[index, 'Declaracion'] = row[col]
                        break
    return df

def ajustar_valores_declaracion(df):
    df['Declaracion'] = df['Declaracion'].apply(
        lambda x: re.sub(r'No Declaración', '', str(x)).strip()
    )
    return df

def ajustar_valores_declaracion_final(df):
    df['Declaracion'] = df['Declaracion'].apply(
        lambda x: re.sub(r'^.*?(1\d{2})-\d{4}-10-(\d{6}).*$', r'\1-\2', str(x))
        if re.search(r'1\d{2}-\d{4}-10-\d{6}', str(x)) else ''
    )
    return df

def add_fecha_column(df):
    df['Fecha'] = ''
    for index, row in df.iterrows():
        if '4.6 Imp.Gene' in str(row.get('CONCEPTO', '')):
            for col in df.columns:
                if col not in ['source_file', 'CONCEPTO', 'Error', 'Declaracion', 'Fecha', 'Ad_Valorem',
                               'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas', 'Percepcion', 'PEC']:
                    if 'Fecha' in str(row.get(col, '')):
                        df.at[index, 'Fecha'] = row[col]
                        break
    return df

def ajustar_valores_fecha(df):
    df['Fecha'] = df['Fecha'].apply(
        lambda x: re.sub(r'6.2 Fecha', '', str(x)).strip()
    )
    return df

def add_ad_valorem_column(df):
    df['Ad_Valorem'] = ''
    for index, row in df.iterrows():
        if '4.1 Ad/Valorem' in str(row.get('CONCEPTO', '')):
            if pd.notna(row.get('Col_7')) and str(row.get('Col_7')).strip() != '':
                df.at[index, 'Ad_Valorem'] = row['Col_7']
            elif pd.notna(row.get('Col_6')) and str(row.get('Col_6')).strip() != '':
                df.at[index, 'Ad_Valorem'] = row['Col_6']
    return df

def add_imp_prom_municipal_column(df):
    df['Imp_Prom_Municipal'] = ''
    for index, row in df.iterrows():
        if '4.5 Imp.Prom.Municipal' in str(row.get('CONCEPTO', '')):
            if pd.notna(row.get('Col_7')) and str(row.get('Col_7')).strip() != '':
                df.at[index, 'Imp_Prom_Municipal'] = row['Col_7']
            elif pd.notna(row.get('Col_6')) and str(row.get('Col_6')).strip() != '':
                df.at[index, 'Imp_Prom_Municipal'] = row['Col_6']
    return df

def add_imp_gene_a_las_ventas_column(df):
    df['Imp_Gene_a_las_Ventas'] = ''
    for index, row in df.iterrows():
        if '4.6 Imp.Gene.a las Ventas' in str(row.get('CONCEPTO', '')):
            if pd.notna(row.get('Col_7')) and str(row.get('Col_7')).strip() != '':
                df.at[index, 'Imp_Gene_a_las_Ventas'] = row['Col_7']
            elif pd.notna(row.get('Col_6')) and str(row.get('Col_6')).strip() != '':
                df.at[index, 'Imp_Gene_a_las_Ventas'] = row['Col_6']
    return df

def add_percepcion_column(df):
    df['Percepcion'] = ''
    for index, row in df.iterrows():
        if '4.7 Derechos Antidumping' in str(row.get('CONCEPTO', '')):
            for col in df.columns:
                if col not in ['source_file', 'CONCEPTO', 'Error', 'Declaracion', 'Fecha', 'Ad_Valorem',
                               'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas', 'Percepcion', 'PEC']:
                    if 'Percepción' in str(row.get(col, '')):
                        df.at[index, 'Percepcion'] = row[col]
                        break
    return df

def ajustar_valores_percepcion(df):
    df['Percepcion'] = df['Percepcion'].apply(
        lambda x: re.sub(r'Percepción IGV S/: ', '', str(x)).strip()
    )
    return df

def add_pec_column(df):
    import re
    df['PEC'] = ''
    for index, row in df.iterrows():
        conceito = str(row.get('CONCEPTO', '')).upper()

        # Caso 1: quando a linha de CONCEPTO contém "IMPORTE"
        if 'IMPORTE' in conceito:
            match = re.search(r'PEC\s*\d+', str(row.get('CONCEPTO', '')))
            if match:
                df.at[index, 'PEC'] = match.group(0)

        # Caso 2: quando aparece "MARITIMA" e ainda não preencheu PEC
        elif 'MARITIMA' in conceito and not df.at[index, 'PEC']:
            for col in df.columns:
                if col not in [
                    'source_file', 'CONCEPTO', 'Error', 'Declaracion', 'Fecha',
                    'Ad_Valorem', 'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas',
                    'Percepcion', 'PEC'
                ]:
                    if 'PEC' in str(row.get(col, '')):
                        df.at[index, 'PEC'] = row[col]
                        break

        # Caso 3: fallback — tenta extrair PEC do nome do arquivo, se ainda vazio
        elif str(row.get('CONCEPTO', '')).strip() != '' and not df.at[index, 'PEC']:
            match = re.search(r'PEC\s*\d+', str(row.get('source_file', '')))
            if match:
                df.at[index, 'PEC'] = match.group(0)

    return df

def ajustar_valores_pec(df):
    def ajustar_pec(valor):
        valor = str(valor)
        match = re.search(r'PEC\s*\S*', valor)
        if match:
            pec_valor = match.group(0)
            pec_valor = re.sub(r'^PEC(?!\s)', 'PEC ', pec_valor)
            return pec_valor.strip()
        return ''
    df['PEC'] = df['PEC'].apply(ajustar_pec)
    return df

def remover_virgulas_valores(df):
    colunas = ['Ad_Valorem', 'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas', 'Percepcion']
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)
    return df

def formatar_valores_para_float(df):
    colunas = ['Ad_Valorem', 'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas', 'Percepcion']
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    return df

def consolidar_dados(df):
    def primeiro_valor_nao_vazio(series):
        for valor in series:
            if pd.notna(valor) and str(valor).strip() != '':
                return valor
        return ''
    df_consolidado = df.groupby('source_file', as_index=False).agg({
        'Declaracion': primeiro_valor_nao_vazio,
        'Fecha': primeiro_valor_nao_vazio,
        'Ad_Valorem': primeiro_valor_nao_vazio,
        'Imp_Prom_Municipal': primeiro_valor_nao_vazio,
        'Imp_Gene_a_las_Ventas': primeiro_valor_nao_vazio,
        'Percepcion': primeiro_valor_nao_vazio,
        'PEC': primeiro_valor_nao_vazio,
        'Error': primeiro_valor_nao_vazio
    })
    return df_consolidado

def adicionar_coluna_tasa(df, cambio_df):
    """
    Faz merge por data exata com o DF de câmbio (Tasa) vindo do tab 2.
    Espera que cambio_df tenha colunas ['Data', 'Venta'].
    """
    if cambio_df is None or cambio_df.empty:
        # sem Tasa: retorna df como está
        return df

    # Detecta coluna de data
    data_coluna = None
    if 'Fecha' in df.columns:
        data_coluna = 'Fecha'
    elif 'Fecha de Emisión' in df.columns:
        data_coluna = 'Fecha de Emisión'

    if not data_coluna:
        return df

    df_temp = df.copy()
    df_temp['Fecha_temp'] = pd.to_datetime(df_temp[data_coluna], errors='coerce', dayfirst=True)

    cambio = cambio_df.copy()
    cambio['Data'] = pd.to_datetime(cambio['Data'], errors='coerce', dayfirst=True)

    df_temp = df_temp.merge(
        cambio[['Data', 'Venta']],
        how='left',
        left_on='Fecha_temp',
        right_on='Data'
    )
    df_temp.rename(columns={'Venta': 'Tasa'}, inplace=True)
    df_temp.drop(columns=['Fecha_temp', 'Data'], inplace=True)
    return df_temp

def adicionar_coluna_igv(df):
    # Item 3: tratar NaN como 0 na soma
    if 'Imp_Prom_Municipal' in df.columns and 'Imp_Gene_a_las_Ventas' in df.columns:
        df['IGV'] = df['Imp_Prom_Municipal'].fillna(0) + df['Imp_Gene_a_las_Ventas'].fillna(0)
    return df

def adicionar_cod_proveedor(df):
    if 'Declaracion' in df.columns:
        df['COD PROVEEDOR'] = df['Declaracion'].apply(
            lambda x: "13131295" if pd.notna(x) and str(x).strip() != '' else None
        )
    return df

def adicionar_cod_moneda(df):
    if 'Declaracion' in df.columns:
        df['COD Moneda'] = df['Declaracion'].apply(
            lambda x: "01" if pd.notna(x) and str(x).strip() != '' else None
        )
    return df

def adicionar_cod_autorizacion(df):
    if 'Declaracion' in df.columns:
        df['Cód. de Autorización'] = df['Declaracion'].apply(
            lambda x: "50" if pd.notna(x) and str(x).strip() != '' else None
        )
    return df

def adicionar_tip_fac_duas(df):
    if 'Declaracion' in df.columns:
        df['Tipo de Factura'] = df['Declaracion'].apply(
            lambda x: "12" if pd.notna(x) and str(x).strip() != '' else None
        )
    return df

def adicionar_cuenta(df):
    if 'Declaracion' in df.columns:
        df['Cuenta'] = df['Declaracion'].apply(
            lambda x: "421202" if pd.notna(x) and str(x).strip() != '' else None
        )
    return df

def remover_barras_fecha(df):
    # ajusta as datas para ddmmaa
    def formatar_e_remover_barras(data):
        try:
            return datetime.strptime(str(data), "%d/%m/%Y").strftime("%d%m%y")
        except ValueError:
            try:
                return datetime.strptime(str(data), "%d-%m-%Y").strftime("%d%m%y")
            except ValueError:
                return str(data).replace("/", "")
    if 'Fecha' in df.columns:
        df['Fecha'] = df['Fecha'].apply(formatar_e_remover_barras)
    return df

def organizar_colunas(df):
    colunas_desejadas = [
        'source_file','COD PROVEEDOR', 'Declaracion', 'Fecha', 'Ad_Valorem',
        'Imp_Prom_Municipal', 'Imp_Gene_a_las_Ventas', 'IGV',
        'Percepcion', 'PEC', 'Tasa','COD Moneda', 'Cód. de Autorización',
        'Tipo de Factura','Cuenta','Error', 
    ]
    colunas_presentes = [col for col in colunas_desejadas if col in df.columns]
    df = df[colunas_presentes + [col for col in df.columns if col not in colunas_presentes]]
    return df

def aplicar_etapas(df, cambio_df=None):
    """
    Orquestra a pipeline de transformação DUAS.
    """
    df = add_declaracion_column(df)
    df = ajustar_valores_declaracion(df)
    df = ajustar_valores_declaracion_final(df)
    df = add_fecha_column(df)
    df = ajustar_valores_fecha(df)
    df = add_ad_valorem_column(df)
    df = add_imp_prom_municipal_column(df)
    df = add_imp_gene_a_las_ventas_column(df)
    df = add_percepcion_column(df)
    df = ajustar_valores_percepcion(df)
    df = add_pec_column(df)
    df = ajustar_valores_pec(df)
    df = remover_virgulas_valores(df)
    df = formatar_valores_para_float(df)
    df = consolidar_dados(df)
    df = adicionar_coluna_tasa(df, cambio_df)
    df = adicionar_coluna_igv(df)
    df = adicionar_cod_proveedor(df)
    df = adicionar_cod_moneda(df)
    df = adicionar_cod_autorizacion(df)
    df = adicionar_tip_fac_duas(df)
    df = adicionar_cuenta(df)
    df = organizar_colunas(df)
    df = remover_barras_fecha(df)
    return df
