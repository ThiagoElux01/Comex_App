
import streamlit as st
from settings import APP_NAME

def render():
    st.subheader("Configurações")
    st.write("Espaço para preferências do usuário, tema, etc.")
    st.text_input("Nome do app", value=APP_NAME, disabled=True)
    st.caption("Credenciais e tokens são lidos de `.streamlit/secrets.toml`.")
