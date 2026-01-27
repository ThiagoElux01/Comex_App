
import pandas as pd
import re
from datetime import datetime

def corrigir_data_sharepoint(valor):
    """
    Converte datas de qualquer formato irregular do SharePoint para dd/mm/yyyy.
    Caso não seja possível converter, retorna ''.
    """

    if valor is None:
        return ""

    s = str(valor).strip()

    # Remove lixo comum
    s = s.replace("\u200b", "")      # zero-width space
    s = s.replace("\u00a0", " ")     # NBSP
    s = s.strip()

    if s == "":
        return ""

    # ---------------------------
    # 1) Normalização com REGEX
    # ---------------------------

    # Extrai apenas o trecho que parece ser uma data
    padrao = re.compile(
        r'(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})'
        r'|(\d{1,2}\s+[A-Za-záéíóúñçÇâêôãõ]{3,15}\s+\d{2,4})'
    )
    m = padrao.search(s)
    if m:
        s = m.group(0)

    # ---------------------------
    # 2) Tenta vários formatos conhecidos
    # ---------------------------
    formatos = [
        "%d/%m/%Y", "%d-%m-%Y",
        "%Y/%m/%d", "%Y-%m-%d",
        "%m/%d/%Y", "%m-%d-%Y",
        "%d/%m/%y",  "%d-%m-%y",
        "%Y/%m/%d", "%Y-%m-%d",
        "%d %b %Y", "%d %B %Y",
        "%d %b %y", "%d %B %y",
    ]

    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d/%m/%Y")
        except:
           -----------
    # 3) Tenta meses PT/ES convertidos para inglês
    # ---------------------------

    meses = {
        # Português
        "jan": "Jan", "janeiro": "Jan",
        "fev": "Feb", "fevereiro": "Feb",
        "mar": "Mar", "março": "Mar",
        "abr": "Apr", "abril": "Apr",
        "mai": "May", "maio": "May",
        "jun": "Jun", "junho": "Jun",
        "jul": "Jul", "julho": "Jul",
        "ago": "Aug", "agosto": "Aug",
        "set": "Sep", "setembro": "Sep",
        "out": "Oct", "outubro": "Oct",
        "nov": "Nov", "novembro": "Nov",
        "dez": "Dec", "dezembro": "Dec",
    
        # Espanhol
        "ene": "Jan", "enero": "Jan",
        "feb": "Feb", "febrero": "Feb",
        "mar": "Mar", "marzo": "Mar",
        "abr": "Apr", "abril": "Apr",
        "may": "May", "mayo": "May",
        "jun": "Jun", "junio": "Jun",
        "jul": "Jul", "julio": "Jul",
        "ago": "Aug", "agosto": "Aug",
        "sep": "Sep", "sept": "Sep", "septiembre": "Sep",
        "oct": "Oct", "octubre": "Oct",
        "nov": "Nov", "noviembre": "Nov",
        "dic": "Dec", "diciembre": "Dec"
    }


    # substitui mês PT/ES por EN
    s_proc = s.lower()
    for mes_local, mes_en in meses.items():
        s_proc = re.sub(rf"\b{mes_local}\b", mes_en, s_proc)

    s_proc = s_proc.title()

    for fmt in ["%d %b %Y", "%d %B %Y", "%d-%b-%Y", "%d-%B-%Y"]:
        try:
            dt = datetime.strptime(s_proc, fmt)
            return dt.strftime("%d/%m/%Y")
        except:
            pass

    # ---------------------------
    # 4) Não converteu -> retorna vazio
    # ---------------------------
    return ""

# ============================================================
# FUNÇÃO ROBUSTA PARA LIMPAR NÚMEROS DO SHAREPOINT
# ============================================================
def clean_number(value):
    if value is None:
        return None

    s = str(value).strip()

    # Remove símbolos, letras e moedas (R$, $, USD etc.)
    s = re.sub(r"[^\d.,-]", "", s)

    # Caso: milhar com ponto e decimal com vírgula (BR)
    # Ex: 8.428,74 → 8428.74
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None

    # Caso: decimal com vírgula
    # Ex: 8428,74 → 8428.74
    if "," in s:
        s = s.replace(",", ".")
        try:
            return float(s)
        except:
            return None

    # Caso: decimal com ponto
    # Ex: 8428.74 → 8428.74
    if "." in s:
        try:
            return float(s)
        except:
            return None

    # Caso: inteiro puro (842874)
    if s.isdigit():
        return float(s)

    # fallback
    try:
        return float(s)
    except:
        return None


# ============================================================
# AJUSTE COMPLETO DO DATAFRAME DO SHAREPOINT
# ============================================================
def ajustar_sharepoint_df(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica ajustes específicos ao DataFrame Sharepoint."""

    df = df.copy()

    # ------------------------------------------------------------
    # 1) Normalizar nomes das colunas
    # ------------------------------------------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.lower()
    )

    # ============================================================
    # 2) IMPORTE DOCUMENTO → número (agora usando clean_number)
    # ============================================================
    possiveis_nomes_importe = [
        "importe_documento",
        "importe_del_documento",
        "importe",
    ]

    for col in possiveis_nomes_importe:
        if col in df.columns:
            df[col] = df[col].apply(clean_number)

    # ============================================================
    # 3) FECHA DE EMISION DEL DOCUMENTO → data
    # ============================================================
    possiveis_nomes_data = [
        "fecha_de_emision_del_documento",
        "fecha_emision_documento",
        "fecha",
    ]

    for col in possiveis_nomes_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            df[col] = df[col].dt.strftime("%d/%m/%Y")

    # ============================================================
    # 4) PROVEEDOR → texto antes do "-"
    # ============================================================
    if "proveedor" in df.columns:
        df["proveedor"] = (
            df["proveedor"]
            .astype(str)
            .str.split("-", n=1)
            .str[0]
            .str.strip()
        )

    return df
