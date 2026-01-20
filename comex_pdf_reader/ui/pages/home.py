
import streamlit as st

def render():
    st.subheader("Bem-vindo(a) 👋")
    st.info("Este é o template inicial do COMEX PDF READER com login básico.")
    st.markdown(
        """
        **Sugestões de próximos passos:**
        - Upload de PDFs
        - Extração de texto/tabelas
        - Barra de progresso à direita
        - Exportação para Excel
        - Integração com Tasa SUNAT
        """
    )
