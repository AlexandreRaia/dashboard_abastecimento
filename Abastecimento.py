import streamlit as st

pg = st.navigation(
    [
        st.Page('pages/Dashboard.py',  title='Dashboard',  icon='📊', default=True),
        st.Page('pages/Auditoria.py',  title='Auditoria',  icon='🔍'),
        st.Page('pages/Manutencao.py', title='Manutenção', icon='🔧'),
        st.Page('pages/lab.py',        title='Lab',        icon='🧪'),
    ],
    position='hidden',
)
pg.run()
