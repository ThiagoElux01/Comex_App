
# services/duas_service.py
from io import BytesIO
import pandas as pd
import pdfplumber
from typing import List, Optional
from .duas_utils import (
    aplicar_etapas
)

# ---------------------- Helpers internos ----------------------

def make_unique_columns(columns):
    """
    Garante nomes únicos e substitui vazios por 'Col'.
    """
    seen = set()
    out = []
    for i, col in enumerate(columns):
        col = (col or '').strip()
        col = col if col else 'Col'
        if col in seen:
            col = f"{col}_{i}"
        out.append(col)
        seen.add(col)
    return out

def standardize_column_names(columns):
    """
    Mantém sua regra: se tiver 'XML', 'CRAMIREZ', 'NTAPIA' -> renomeia para 'CONCEPTO'.
    Se tiver 'REF:' -> 'Col17'. Se não achar 'CONCEPTO', força a primeira coluna.
    """
    standardized = []
    found_concepto = False
    for col in columns:
        if col and ('XML' in col or 'CRAMIREZ' in col or 'NTAPIA' in col):
            standardized.append('CONCEPTO')
            found_concepto = True
        elif col and 'REF:' in col:
            standardized.append('Col17')
        else:
            standardized.append(col)
    if not found_concepto and standardized:
        standardized[0] = 'CONCEPTO'
    return standardized

# ---------------------- Extração + Pipeline ----------------------

def extract_table001_from_uploaded_files(
    uploaded_files: List, 
    progress_widget=None, 
    status_widget=None
) -> Optional[pd.DataFrame]:
    """
    Lê a PRIMEIRA página e a PRIMEIRA tabela de cada PDF (como você confirmou).
    """
    all_tables = []
    total = len(uploaded_files) if uploaded_files else 0
    if total == 0:
        return None

    for i, f in enumerate(uploaded_files, start=1):
        filename = getattr(f, "name", f"arquivo_{i}.pdf")
        try:
            with pdfplumber.open(BytesIO(f.getvalue())) as pdf:
                if len(pdf.pages) > 0:
                    page = pdf.pages[0]
                    tables = page.extract_tables() or []
                    if tables:
                        table = tables[0]
                        columns = make_unique_columns(table[0])
                        columns = standardize_column_names(columns)
                        df = pd.DataFrame(table[1:], columns=columns)
                        df['source_file'] = filename
                        if 'CONCEPTO' not in df.columns:
                            df['CONCEPTO'] = ''
                        df['Error'] = df['CONCEPTO'].apply(
                            lambda x: "File can't be read" if pd.isna(x) or str(x).strip() == '' else ''
                        )
                        all_tables.append(df)
        except Exception as e:
            # Se der erro no PDF, cria uma linha com erro.
            err_df = pd.DataFrame([{
                'source_file': filename,
                'CONCEPTO': '',
                'Error': f'Erro ao ler o PDF: {e}'
            }])
            all_tables.append(err_df)

        # Atualiza UI
        if progress_widget:
            pct = int(i / total * 100)
            progress_widget.progress(pct, text=f"Lendo {filename} ({i}/{total})")
        if status_widget:
            status_widget.write(f"📄 Extraído: **{filename}**")

    return pd.concat(all_tables, ignore_index=True) if all_tables else None

def process_duas_streamlit(
    uploaded_files: List, 
    progress_widget=None, 
    status_widget=None,
    cambio_df: Optional[pd.DataFrame] = None
) -> Optional[pd.DataFrame]:
    """
    Executa o pipeline DUAS para arquivos enviados pelo Streamlit.
    Retorna o DataFrame final (não salva em disco).
    """
    if not uploaded_files:
        return None

    if progress_widget:
        progress_widget.progress(0, text="Lendo PDFs DUAS...")

    combined_df = extract_table001_from_uploaded_files(uploaded_files, progress_widget, status_widget)
    if combined_df is None or combined_df.empty:
        if status_widget:
            status_widget.write("⚠️ Nenhuma tabela válida encontrada nos PDFs.")
        if progress_widget:
            progress_widget.progress(0, text="Aguardando...")
        return None

    if status_widget:
        status_widget.write("🔄 Aplicando regras DUAS...")
    if progress_widget:
        progress_widget.progress(50, text="Transformando dados (DUAS)...")

    df_final = aplicar_etapas(combined_df, cambio_df=cambio_df)

    if progress_widget:
        progress_widget.progress(100, text="Concluído.")
    return df_final
