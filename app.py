import streamlit as st
import pandas as pd
from streamlit_ace import st_ace, KEYBINDINGS, LANGUAGES, THEMES
from pipeline import pipeline, con_to_postgres
from sqlalchemy import text

st.title("Processador de Arquivos")

if st.button("Processar"):
    with st.spinner("Processando..."):
        logs = pipeline()
        for log in logs:
            st.write(log)


with st.sidebar:
    st.header("Query Box Editor")
content = st_ace(
                    placeholder="--Select Database and Write your SQL Query Here!",
                    language= "sql",
                    theme=st.sidebar.selectbox("Select Theme",options=THEMES),
                    keybinding=st.sidebar.selectbox("Select Keybinding",options=KEYBINDINGS),
                    wrap=True,

                    font_size=st.sidebar.slider("Font Size", 10, 24, 16),
                    min_lines=15,
                    key="run_query",
                )

if content:

    def run_query():
        query = content
        conn = con_to_postgres().connect()

        try:
            query = conn.execute(text(query))
            
            cols = [column for column in query.keys()]
            results_df= pd.DataFrame.from_records(
                data = query.fetchall(), 
                columns = cols
            )
            st.dataframe(results_df)
            export = results_df.to_csv()
            st.download_button(label="Download Results", data=export, file_name='query_results.csv' )
        except Exception as e:
            st.write(e)

    run_query()