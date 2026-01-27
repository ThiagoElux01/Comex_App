
import pandas as pd
import re

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
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^\d,.-]", "", regex=True)  # remove letras, R$, $, etc
                .str.replace(".", "", regex=False)         # remove separador de milhares
                .str.replace(",", ".", regex=False)        # vírgula vira decimal
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

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
