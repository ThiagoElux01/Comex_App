
import pandas as pd
import re

def clean_number(value):
    if value is None:
        return None

    s = str(value).strip()

    # Remove moeda e símbolos estranhos
    s = re.sub(r"[^\d.,-]", "", s)

    # Caso 1: formato BR (milhar com ponto + decimal com vírgula)
    # Ex: 8.428,74
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None

    # Caso 2: decimal usando vírgula
    # Ex: 8428,74
    if "," in s:
        s = s.replace(",", ".")
        try:
            return float(s)
        except:
            return None

    # Caso 3: decimal com ponto
    # Ex: 8428.74
    if "." in s:
        try:
            return float(s)
        except:
            return None

    # Caso 4: inteiro puro (Ex: 842874)
    if s.isdigit():
        return float(s)

    # fallback
    try:
        return float(s)
    except:
        return None

def ajustar_sharepoint_df(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica ajustes específicos ao DataFrame Sharepoint."""
    
    df = df.copy()

    # ------------------------------------------------------------
    # 1) Normalizar nomes de colunas
    # ------------------------------------------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.lower()
    )

    # ============================================================
    # 2) IMPORTE DOCUMENTO → número
    # ============================================================
    possiveis_nomes_importe = [
        "importe_documento",
        "importe_del_documento",
        "importe",
    ]

    for col in possiveis_nomes_importe:
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
