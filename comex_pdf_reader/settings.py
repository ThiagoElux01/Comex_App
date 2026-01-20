
import streamlit as st

APP_NAME = st.secrets.get("app", {}).get("name", "COMEX PDF READER")

# Páginas do app
PAGES = ["Home", "Processar PDFs", "Configurações"]
