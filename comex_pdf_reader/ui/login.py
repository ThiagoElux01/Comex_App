
import streamlit as st
from settings import APP_NAME
from auth import login, set_authenticated

def render_login():
    st.set_page_config(page_title=APP_NAME, page_icon="📄", layout="wide")
    st.markdown(f"## 📄 {APP_NAME}")
    st.write("Acesse com seu e-mail e senha.")

    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("Login (e-mail)", placeholder="seu.email@empresa.com")
        password = st.text_input("Password", type="password", placeholder="••••••")
        submitted = st.form_submit_button("Entrar")

    if submitted:
        if login(email, password):
            set_authenticated(email.strip())
            st.success("Login realizado com sucesso! Redirecionando...")
            st.rerun()
        else:
            st.error("Credenciais inválidas. Verifique e tente novamente.")

    with st.expander("Ajuda"):
        st.markdown(
            """
            **Acesso temporário (definido em `.streamlit/secrets.toml`)**  
            - E-mail: `thiago.farias@electrolux.com`  
            - Senha: `12345`
            """
        )
