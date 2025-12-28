import streamlit as st

# 1. Configuración de la página
st.set_page_config(
    page_title="FPD Daily - Dashboard",
    layout="wide",
    page_icon="📊"
)

# 2. Título principal
st.title("📊 FPD Daily: Monitor de Crédito")
st.markdown("---")

# 3. Creación de las 4 pestañas
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Resumen General", 
    "🍇 Análisis de Cosechas", 
    "🏢 Por Sucursal", 
    "📋 Detalle de Datos"
])

# --- Contenido de la Pestaña 1 ---
with tab1:
    st.header("Resumen General")
    st.info("Espacio reservado para los KPIs principales y tendencias globales.")

# --- Contenido de la Pestaña 2 ---
with tab2:
    st.header("Análisis de Cosechas (Vintage)")
    st.info("Aquí colocaremos la gráfica de líneas con la evolución del FPD2 por mes.")

# --- Contenido de la Pestaña 3 ---
with tab3:
    st.header("Desempeño por Sucursal")
    st.info("Sección para comparar el riesgo entre las distintas oficinas.")

# --- Contenido de la Pestaña 4 ---
with tab4:
    st.header("Explorador de Datos")
    st.info("Vista detallada de los créditos y filtros específicos.")