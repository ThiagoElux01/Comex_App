
import pandas as pd
import re

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
