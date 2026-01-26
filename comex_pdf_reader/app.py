
import streamlit as st
from auth import is_authenticated
from ui.login import render_login
from ui.layout import app_header, sidebar_navigation
from settings import PAGES
from ui.pages import home, process_pdfs, settings_page
from ui.pages import downloads_page   # (ok)

def main():
    st.set_page_config(page_title="COMEX PDF READER", page_icon="📄", layout="wide")

    if not is_authenticated():
        render_login()
        return

    app_header()

    # === [NOVO BLOCO] Botão de atalho para “Arquivos modelo” ===
    # Coloque este container logo após o app_header(), antes de ler a sidebar.
    top_actions = st.container()
    with top_actions:
        col_l, col_r = st.columns([6, 1])
        with col_r:
            if st.button("⬇️ Arquivos modelo", use_container_width=True):
                # Guarda o destino desejado e força novo run
                st.session_state["_goto_page"] = "Arquivos modelo"
                st.rerun()
    # ===========================================================

    # Se o botão acima foi clicado, priorize esse destino.
    if st.session_state.get("_goto_page"):
        page = st.session_state.pop("_goto_page")
    else:
        page = sidebar_navigation(PAGES)

    if page == "Home":
        home.render()
    elif page == "Processar PDFs":
        process_pdfs.render()
    elif page == "Arquivos modelo":  # <--- NOVO CASE já está certo no seu código
        downloads_page.render()
    elif page == "Configurações":
        settings_page.render()
    else:
        st.error("Página não encontrada.")

if __name__ == "__main__":
    main()
