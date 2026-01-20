
import io
import pdfplumber
import pandas as pd
import requests
import streamlit as st

def _get_sunat_conf():
    sunat = st.secrets.get("sunat", {})
    base_url = sunat.get("base_url")
    referer  = sunat.get("referer")
    token    = sunat.get("token")
    raw_cookie = sunat.get("cookie")  # Ex: "IAASISTENCIAGESTIONSESSION=xxx"
    cookies = {}
    if raw_cookie and "=" in raw_cookie:
        k, v = raw_cookie.split("=", 1)
        cookies[k] = v
    return base_url, referer, token, cookies

def _deduplicar_colunas(colunas):
    seen = {}
    novas = []
    for col in colunas:
        if col not in seen:
            seen[col] = 0
            novas.append(col)
        else:
            seen[col] += 1
            novas.append(f"{col}.{seen[col]}")
    return novas

def atualizar_dataframe_tasa(anos=None, progress_widget=None, status_widget=None):
    """
    Baixa PDFs de Tasa na SUNAT e consolida Data x Venta.
    Args:
        anos: lista de strings (ex: ["2024", "2025", "2026"])
        progress_widget: st.progress
        status_widget: st.empty() - para mensagens
    Returns:
        pandas.DataFrame ou None
    """
    base_url, referer, token, cookies = _get_sunat_conf()
    if not (base_url and referer and token and cookies):
        if status_widget:
            status_widget.warning("Configuração SUNAT ausente em secrets.toml.")
        return None

    headers = {
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": referer.rsplit("/", 1)[0],
        "Referer": referer,
        "User-Agent": "Mozilla/5.0"
    }

    if anos is None:
        anos = ["2024", "2025", "2026"]

    dataframes = []
    total_steps = len(anos) * 12
    step = 0

    if status_widget:
        status_widget.write("Atualizando DataFrame de Tasa...")

    for ano in anos:
        for mes_idx in range(12):  # 0..11
            step += 1
            prog = int(step / total_steps * 100)
            if progress_widget:
                progress_widget.progress(prog, text=f"Baixando {ano}-{mes_idx+1:02d}...")

            data = {
                "token": token,
                "anioDownload": ano,
                "mesDownload": str(mes_idx)  # zero-based
            }

            try:
                response = requests.post(base_url, headers=headers, cookies=cookies, data=data, timeout=30)
            except Exception as e:
                if status_widget:
                    status_widget.error(f"[ERRO] Falha de rede em {ano}-{mes_idx+1:02d}: {e}")
                continue

            if response.status_code != 200 or not response.content:
                if status_widget:
                    status_widget.warning(f"[AVISO] Sem conteúdo para {ano}-{mes_idx+1:02d}.")
                continue

            try:
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    if not pdf.pages:
                        if status_widget:
                            status_widget.info(f"[AVISO] PDF vazio para {ano}-{mes_idx+1:02d}")
                        continue
                    for page in pdf.pages:
                        table = page.extract_table()
                        if not table:
                            continue
                        df = pd.DataFrame(table[1:], columns=_deduplicar_colunas(table[0]))

                        for col in df.columns:
                            if col.startswith("Dia"):
                                df[col] = df[col].apply(lambda x: str(x).zfill(2) if pd.notnull(x) else x)

                        for col in ["Compra", "Compra1", "Compra2", "Compra3"]:
                            if col in df.columns:
                                df.drop(columns=[col], inplace=True)

                        mes_fmt = f"{mes_idx+1:02d}"
                        df["mes"] = mes_fmt
                        df["ano"] = ano

                        for col in df.columns:
                            if col.startswith("Dia"):
                                df["Data"] = df[col].astype(str) + "/" + df["mes"] + "/" + df["ano"]
                                break

                        if "Dia.1" in df.columns:
                            df["data1"] = df["Dia.1"].astype(str) + "/" + df["mes"] + "/" + df["ano"]
                        if "Dia.2" in df.columns:
                            df["data2"] = df["Dia.2"].astype(str) + "/" + df["mes"] + "/" + df["ano"]
                        if "Dia.3" in df.columns:
                            df["data3"] = df["Dia.3"].astype(str) + "/" + df["mes"] + "/" + df["ano"]

                        dataframes.append(df)
            except Exception as e:
                if status_widget:
                    status_widget.error(f"[ERRO] Falha ao processar PDF de {ano}-{mes_idx+1:02d}: {e}")
                continue

    if progress_widget:
        progress_widget.progress(100, text="Consolidando dados...")

    if not dataframes:
        if status_widget:
            status_widget.warning("Nenhum dado extraído dos PDFs.")
        return None

    final_df = pd.concat(dataframes, ignore_index=True)

    parts = []
    if set(["Data", "Venta"]).issubset(final_df.columns):
        parts.append(final_df[["Data", "Venta"]])

    mapping = [
        ("data1", "Venta.1"),
        ("data2", "Venta.2"),
        ("data3", "Venta.3"),
    ]
    for dcol, vcol in mapping:
        if dcol in final_df.columns and vcol in final_df.columns:
            parts.append(final_df[[dcol, vcol]].rename(columns={dcol: "Data", vcol: "Venta"}))

    if not parts:
        if status_widget:
            status_widget.warning("Estrutura inesperada no PDF. Colunas 'Venta' não encontradas.")
        return None

    df_merged = pd.concat(parts, ignore_index=True)

    df_merged["Venta"] = df_merged["Venta"].replace(r"^\s*$", pd.NA, regex=True)
    df_merged = df_merged[df_merged["Venta"].notna()]
    df_merged["Venta"] = pd.to_numeric(df_merged["Venta"], errors="coerce")
    df_merged = df_merged.dropna(subset=["Venta"])

    try:
        df_merged["Data"] = pd.to_datetime(df_merged["Data"], format="%d/%m/%Y", errors="coerce")
        df_merged = df_merged.dropna(subset=["Data"]).sort_values("Data").reset_index(drop=True)
    except Exception:
        pass

    if status_widget:
        status_widget.success("DataFrame de Tasa atualizado com sucesso.")

    return df_merged
