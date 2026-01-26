
import streamlit as st
import pandas as pd
import time
import requests
from pathlib import Path

# raiz do projeto
BASE_DIR = Path(__file__).resolve().parents[2]
ARQUIVO_MODELO = BASE_DIR / "assets" / "modelos" / "Externos.xlsx"

# URL do Power Automate
POWER_AUTOMATE_URL = st.secrets["POWER_AUTOMATE_URL"]

@st.cache_data
def carregar_modelo():
    return pd.read_excel(ARQUIVO_MODELO)

def render():
    st.subheader("Home")
    st.write("Atualização do arquivo de modelos externos")

    if st.button("🔄 Update"):
        try:
            with st.spinner("Atualizando dados no SharePoint..."):
                # 1) dispara o Flow
                response = requests.post(POWER_AUTOMATE_URL, timeout=10)

                if response.status_code != 200:
                    st.error("Erro ao disparar atualização no Power Automate")
                    return

                # 2) espera o refresh terminar (ajuste conforme realidade)
                time.sleep(30)  # ⏱️ ex: 30s

                # 3) limpa cache e recarrega Excel
                st.cache_data.clear()
                df = carregar_modelo()

            st.success("Atualização concluída ✅")
            st.info(f"📊 Total de linhas: {len(df)}")

        except Exception as e:
            st.error(f"Erro durante atualização: {e}")

    with st.expander("Ver prévia dos dados"):
        try:
            df = carregar_modelo()
            st.dataframe(df.head(10), use_container_width=True)
        except:
            st.warning("Arquivo ainda não carregado.")
