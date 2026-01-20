
# services/percepcion_service.py
from io import BytesIO
from typing import List, Optional
import re
from datetime import datetime
import fitz  # PyMuPDF
import pandas as pd

def _extract_first_page_lines_to_df(pdf_bytes: bytes) -> pd.DataFrame:
    """
    Extrai as linhas (spans) da primeira página e retorna DataFrame com a primeira coluna 'Text'
    e colunas subsequentes 'Col_1', 'Col_2', ...
    """
    try:
        doc = fitz.open(stream=BytesIO(pdf_bytes), filetype="pdf")
        page = doc.load_page(0)
        tdict = page.get_text("dict")
        blocks = tdict.get("blocks", [])
        linhas = []
        for bloco in blocks:
            for linha in bloco.get("lines", []):
                spans = [span.get("text", "") for span in linha.get("spans", [])]
                if spans:
                    linhas.append(spans)
        if not linhas:
            return pd.DataFrame()
        df = pd.DataFrame(linhas)
        cols = ["Text"] + [f"Col_{i}" for i in range(1, df.shape[1])]
        df.columns = cols[:df.shape[1]]
        return df
    except Exception:
        return pd.DataFrame()

def _add_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replica a lógica do EXE: No_Liquidacion, CDA, Fecha, Monto (+ limpezas).
    """
    def extrair_valor(row):
        texto = str(row.get("Text", "")).upper()
        if "NUMERO DE LIQUIDACION" in texto and ":" in texto:
            return texto.split(":", 1)[1].strip()
        elif "NÚMERO DE LIQU" in texto or "NUMERO DE LIQU" in texto:
            return row.get("Col_1", "") or ""
        return ""

    def extrair_valor_cda(row):
        texto = str(row.get("Text", "")).upper()
        col1 = row.get("Col_1", "")
        if "C.D.A." in texto:
            if pd.notna(col1) and isinstance(col1, str) and col1.strip():
                return col1.replace(" ", "")
        if " :" in texto:
            return texto.split(":", 1)[1].strip().replace(" ", "")
        return ""

    def extrair_fecha(row):
        texto = str(row.get("Text", "")).upper()
        col1 = str(row.get("Col_1", "")).strip()

        m = re.search(r"DE FECHA\s*:\s*([\d]{2}[/-][\d]{2}[/-][\d]{4})", texto)
        if m:
            try:
                return datetime.strptime(m.group(1), "%d/%m/%Y").strftime("%d/%m/%y")
            except ValueError:
                return datetime.strptime(m.group(1), "%d-%m-%Y").strftime("%d/%m/%y")

        m2 = re.search(r"\b(\d{8})\b", col1)
        if m2:
            try:
                return datetime.strptime(m2.group(1), "%Y%m%d").strftime("%d/%m/%y")
            except ValueError:
                return ""

        return ""

    def extrair_monto(df_lines: pd.DataFrame):
        out = []
        for i in range(len(df_lines)):
            texto = str(df_lines.at[i, "Text"]).upper()
            if "SUNAT PERCEPCION IGV" in texto:
                out.append(df_lines.at[i + 1, "Text"] if i + 1 < len(df_lines) else "")
            else:
                out.append("")
        return out

    df["No_Liquidacion"] = df.apply(extrair_valor, axis=1)
    df["CDA"] = df.apply(extrair_valor_cda, axis=1)
    df["Fecha"] = df.apply(extrair_fecha, axis=1)
    df["Monto"] = extrair_monto(df)

    # Limpeza básica
    for col in ["No_Liquidacion", "CDA", "Monto", "Fecha"]:
        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else "")

    # Converte Monto → float (usa '.' como decimal após remover ',')
    def to_float(v):
        s = str(v).replace(",", "").strip()
        return round(float(s), 2) if s and s.replace(".", "", 1).isdigit() else None
    df["Monto"] = df["Monto"].apply(to_float)

    # Ajuste do CDA (ex.: "<xx> ... <dddddd>" → "xx-dddddd")
    def ajustar_cda(v):
        m = re.search(r"\b(\d{2,3})\D+.*?(\d{6,})\b", str(v))
        return f"{m.group(1)}-{m.group(2)}" if m else v
    df["CDA"] = df["CDA"].apply(ajustar_cda)

    # Remover sufixos indesejados do No_Liquidacion
    padroes_remover = ["-25", "-26", "-24", "-23", "-27"]
    regex = re.compile(r"(" + "|".join(map(re.escape, padroes_remover)) + r")\b")
    df["No_Liquidacion"] = df["No_Liquidacion"].apply(lambda x: regex.sub("", str(x)) if pd.notna(x) else x)

    return df

def _consolidar_por_arquivo(df_lines: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida por Source_File pegando o primeiro valor não vazio de cada campo.
    """
    dados = []
    for src in df_lines['Source_File'].unique():
        dfa = df_lines[df_lines['Source_File'] == src]
        pick = lambda s: s.dropna().replace('', pd.NA).dropna()
        no_liq = pick(dfa['No_Liquidacion'])
        cda    = pick(dfa['CDA'])
        fecha  = pick(dfa['Fecha'])
        monto  = pick(dfa['Monto'])
        dados.append([
            src,
            no_liq.iloc[0] if not no_liq.empty else '',
            cda.iloc[0] if not cda.empty else '',
            fecha.iloc[0] if not fecha.empty else '',
            monto.iloc[0] if not monto.empty else ''
        ])
    return pd.DataFrame(dados, columns=["Source_File", "No_Liquidacion", "CDA", "Fecha", "Monto"])

def process_percepcion_streamlit(
    uploaded_files: List,
    progress_widget=None,
    status_widget=None,
) -> Optional[pd.DataFrame]:
    """
    Pipeline Percepciones para Streamlit.
    Lê PDFs do uploader, aplica as regras e retorna o DataFrame final (sem salvar em disco).
    Mantém Tasa = 1.00 conforme lógica original do seu EXE.
    """
    if not uploaded_files:
        return None

    if progress_widget:
        progress_widget.progress(0, text="Lendo PDFs (Percepciones)...")

    dfs = []
    total = len(uploaded_files)
    for i, f in enumerate(uploaded_files, start=1):
        fname = getattr(f, "name", f"arquivo_{i}.pdf")
        lines_df = _extract_first_page_lines_to_df(f.getvalue())
        if not lines_df.empty:
            lines_df.insert(0, "Source_File", fname)
            dfs.append(lines_df)
        if progress_widget:
            progress_widget.progress(int(i/total*100), text=f"Lendo {fname} ({i}/{total})")
        if status_widget:
            status_widget.write(f"📄 Primeira página lida: **{fname}**")

    if not dfs:
        return None

    df_all = pd.concat(dfs, ignore_index=True)
    df_all = _add_columns(df_all)

    # Consolidação por arquivo
    df_rel = _consolidar_por_arquivo(df_all)

    # Pós-processo (mesma lógica do EXE)
    df_rel["Error"] = df_rel["No_Liquidacion"].apply(lambda x: "Can't read the file" if pd.isna(x) or str(x).strip() == "" else "")
    df_rel['Fecha'] = df_rel['Fecha'].astype(str).str.replace('/', '', regex=False)

    # Colunas fixas
    df_rel['Tasa'] = 1.00
    df_rel['COD PROVEEDOR'] = "13131295"
    df_rel['COD MONEDA'] = "00"
    df_rel['Cód. de Autorización'] = "54"
    df_rel['Cuenta'] = "421201"
    df_rel['Tipo de Factura'] = "12"

    # Ordem final
    df_rel = df_rel[['Source_File','COD PROVEEDOR','No_Liquidacion','Fecha','CDA','Monto',
                     'Tasa','COD MONEDA','Cód. de Autorización','Tipo de Factura','Cuenta','Error']]

    if progress_widget:
        progress_widget.progress(100, text="Concluído (Percepciones).")
    return df_rel
