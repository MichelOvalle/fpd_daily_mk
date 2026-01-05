import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import os
import json
import urllib.request
from datetime import datetime, timedelta

# --- 1. CONFIGURACIÓN DE LA APLICACIÓN ---
st.set_page_config(
    page_title="Monitor FPD - Inteligencia de Riesgos",
    layout="wide",
    page_icon="📊"
)

# --- 2. CONSTANTES DEL REPOSITORIO (8:54 AM) ---
# Michel, asegúrate de que estos nombres coincidan con tu repo red_v5
GITHUB_USER = "michel-ovalle" 
GITHUB_REPO = "red_v5"
DATA_FILE = "fpd_gemini.parquet"

# Diccionario para nombres de meses en español
NOMBRES_MESES = {
    '01': 'enero', 
    '02': 'febrero', 
    '03': 'marzo', 
    '04': 'abril', 
    '05': 'mayo', 
    '06': 'junio',
    '07': 'julio', 
    '08': 'agosto', 
    '09': 'septiembre', 
    '10': 'octubre', 
    '11': 'noviembre', 
    '12': 'diciembre'
}

# Configuración de leyenda para gráficos
LEGEND_CFG = dict(
    orientation="h", 
    yanchor="top", 
    y=-0.3, 
    xanchor="center", 
    x=0.5
)

# --- 3. FUNCIÓN DE FECHA REAL (API GITHUB) ---

def obtener_fecha_github():
    """
    Consulta directamente a GitHub para obtener la hora del commit del parquet.
    Esto soluciona el problema de ver la hora del archivo .py.
    """
    api_endpoint = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/commits?path={DATA_FILE}&page=1&per_page=1"
    
    try:
        # Petición HTTP manual para evitar dependencias extra
        headers = {"User-Agent": "Streamlit-Monitor-App"}
        peticion = urllib.request.Request(api_endpoint, headers=headers)
        
        with urllib.request.urlopen(peticion) as respuesta:
            datos_json = json.loads(respuesta.read().decode())
            # Extraemos la fecha del commit (UTC)
            fecha_utc_str = datos_json[0]['commit']['committer']['date']
            fecha_dt = datetime.strptime(fecha_utc_str, "%Y-%m-%dT%H:%M:%SZ")
            
            # Ajuste manual a la zona horaria de México (UTC-6)
            fecha_mexico = fecha_dt - timedelta(hours=6)
            return fecha_mexico.strftime("%d/%m/%Y %I:%M %p")
            
    except Exception:
        # Fallback local: si la API falla, intentamos leer el archivo físico
        if os.path.exists(DATA_FILE):
            timestamp_local = os.path.getmtime(DATA_FILE)
            dt_local = datetime.fromtimestamp(timestamp_local) - timedelta(hours=6)
            return dt_local.strftime("%d/%m/%Y %I:%M %p")
        return "Sincronizando con GitHub..."

# --- 4. CARGA DE DATOS CON DUCKDB (OPTIMIZADO) ---

@st.cache_data
def cargar_universos_filtros():
    """Obtiene los valores únicos para los multiselects del sidebar."""
    if not os.path.exists(DATA_FILE):
        return pd.DataFrame()
        
    query_filtros = f"""
        SELECT DISTINCT 
            COALESCE(unidad_regional, 'N/A') as unidad_regional, 
            COALESCE(sucursal, 'N/A') as sucursal, 
            COALESCE(producto_agrupado, 'N/A') as producto_agrupado, 
            COALESCE(tipo_cliente, 'N/A') as tipo_cliente 
        FROM '{DATA_FILE}'
    """
    return duckdb.query(query_filtros).df()

@st.cache_data
def cargar_datos_maestros(reg, suc, prod, tip):
    """Carga y procesa la tabla principal con filtros dinámicos."""
    if not os.path.exists(DATA_FILE): 
        return pd.DataFrame()
    
    # Formateador de listas para SQL
    def sql_format(lista):
        return "'" + "','".join(lista) + "'"
    
    query_master = f"""
    WITH base_procesada AS (
        SELECT 
            CAST(fecha_apertura AS DATE) as fecha_ap,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as flag_fpd,
            CASE WHEN NP = 'NP' THEN 1 ELSE 0 END as flag_np,
            id_credito, 
            monto_otorgado, 
            origen2,
            COALESCE(tipo_cliente, 'N/A') as dim_tipo, 
            COALESCE(sucursal, 'N/A') as dim_sucursal, 
            COALESCE(unidad_regional, 'N/A') as dim_regional, 
            COALESCE(producto_agrupado, 'N/A') as dim_producto
        FROM '{DATA_FILE}'
    )
    SELECT *, 
           strftime(fecha_ap, '%Y%m') as cosecha_id, 
           EXTRACT(YEAR FROM fecha_ap) as anio_fiscal, 
           strftime(fecha_ap, '%m') as mes_id
    FROM base_procesada
    WHERE 1=1
    {" AND dim_regional IN (" + sql_format(reg) + ")" if reg else ""}
    {" AND dim_sucursal IN (" + sql_format(suc) + ")" if suc else ""}
    {" AND dim_producto IN (" + sql_format(prod) + ")" if prod else ""}
    {" AND dim_tipo IN (" + sql_format(tip) + ")" if tip else ""}
    """
    
    try:
        return duckdb.query(query_master).to_df()
    except Exception as e:
        st.error(f"Error en consulta DuckDB: {e}")
        return pd.DataFrame()

# --- 5. INTERFAZ: SIDEBAR Y FILTRADO ---
st.sidebar.title("🎯 Panel de Control")
opciones = cargar_universos_filtros()

if not opciones.empty:
    sel_reg = st.sidebar.multiselect("📍 Unidad Regional", sorted(opciones['unidad_regional'].unique()))
    
    # Lógica de sucursales dependientes
    if sel_reg:
        lista_suc = sorted(opciones[opciones['unidad_regional'].isin(sel_reg)]['sucursal'].unique())
    else:
        lista_suc = sorted(opciones['sucursal'].unique())
        
    sel_suc = st.sidebar.multiselect("🏠 Sucursal", lista_suc)
    sel_prod = st.sidebar.multiselect("📦 Producto Agrupado", sorted(opciones['producto_agrupado'].unique()))
    sel_tip = st.sidebar.multiselect("👥 Tipo de Cliente", sorted(opciones['tipo_cliente'].unique()))
else:
    st.sidebar.error("Archivo Parquet no disponible.")

# Ejecución de carga principal
df_main = cargar_datos_maestros(sel_reg, sel_suc, sel_prod, sel_tip)

# --- 6. PROCESAMIENTO DE MÉTRICAS DE COSECHA ---
if not df_main.empty:
    # Ignorar la cosecha máxima (datos incompletos)
    id_cosecha_max = df_main['cosecha_id'].max()
    df_analisis = df_main[df_main['cosecha_id'] < id_cosecha_max].copy()
    
    eje_cosechas = sorted(df_analisis['cosecha_id'].unique())
    if eje_cosechas:
        c_actual = eje_cosechas[-1]
        c_anterior = eje_cosechas[-2] if len(eje_cosechas) > 1 else c_actual
        mes_nombre_str = NOMBRES_MESES.get(c_actual[-2:], "N/A").capitalize()

# --- 7. DISEÑO DEL DASHBOARD (TABS) ---
st.title("📊 Monitor de Inteligencia de Riesgo FPD")
st.markdown("---")

t1, t2, t3, t4 = st.tabs(["📈 Evolución Global", "💼 Vista Ejecutiva", "💡 Insights Pareto", "📥 Descarga Detalle"])

# TAB 1: EVOLUCIÓN GLOBAL
with t1:
    if not df_analisis.empty:
        # Agregación para métricas principales
        df_kpi = df_analisis.groupby('cosecha_id').agg({
            'id_credito': 'count', 
            'flag_fpd': 'sum', 
            'flag_np': 'sum'
        }).reset_index()
        
        df_kpi['rate_fpd'] = (df_kpi['flag_fpd'] * 100 / df_kpi['id_credito'])
        df_kpi['rate_np'] = (df_kpi['flag_np'] * 100 / df_kpi['id_credito'])
        
        k_act = df_kpi[df_kpi['cosecha_id'] == c_actual].iloc[0]
        k_ant = df_kpi[df_kpi['cosecha_id'] == c_anterior].iloc[0]

        # Fila de KPIs
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Cosecha Analizada", c_actual, f"vs {c_anterior}", delta_color="off")
        col2.metric("Créditos Otorgados", f"{int(k_act['id_credito']):,}", f"{int(k_act['id_credito'] - k_ant['id_credito']):+,}")
        col3.metric("Tasa FPD Global", f"{k_act['rate_fpd']:.2f}%", f"{k_act['rate_fpd'] - k_ant['rate_fpd']:.2f}%", delta_color="inverse")
        col4.metric("Tasa NP (No Pago)", f"{k_act['rate_np']:.2f}%", f"{k_act['rate_np'] - k_ant['rate_np']:.2f}%", delta_color="inverse")

        st.divider()

        # Gráfico 1: Tendencia Global
        st.subheader("1. Tendencia Histórica del Indicador FPD")
        fig_1 = px.line(df_kpi, x='cosecha_id', y='rate_fpd', markers=True, 
                        text=df_kpi['rate_fpd'].apply(lambda x: f'{x:.1f}%'))
        fig_1.update_layout(xaxis_type='category', plot_bgcolor='white', height=450)
        st.plotly_chart(fig_1, use_container_width=True)

        # Gráfico 2: Desglose por Origen
        st.subheader("2. Tasa FPD por Origen de Venta")
        df_ori = df_analisis.groupby(['cosecha_id', 'origen2']).agg({'id_credito':'count', 'flag_fpd':'sum'}).reset_index()
        df_ori['tasa'] = (df_ori['flag_fpd'] * 100 / df_ori['id_credito'])
        fig_2 = px.line(df_ori, x='cosecha_id', y='tasa', color='origen2', markers=True)
        fig_2.update_layout(xaxis_type='category', legend=LEGEND_CFG, height=450)
        st.plotly_chart(fig_2, use_container_width=True)

        # Gráfico 3: Desglose por Tipo de Cliente (Sin Formers)
        st.subheader("3. Comportamiento por Segmento de Cliente (Sin Formers)")
        df_seg = df_analisis[df_analisis['dim_tipo'] != 'Formers'].groupby(['cosecha_id', 'dim_tipo']).agg({'id_credito':'count', 'flag_fpd':'sum'}).reset_index()
        df_seg['tasa'] = (df_seg['flag_fpd'] * 100 / df_seg['id_credito'])
        fig_3 = px.line(df_seg, x='cosecha_id', y='tasa', color='dim_tipo', markers=True)
        fig_3.update_layout(xaxis_type='category', height=450)
        st.plotly_chart(fig_3, use_container_width=True)

# TAB 2: VISTA EJECUTIVA
with t2:
    st.header("💼 Análisis de Unidades de Negocio")
    
    def renderizar_bloque_ejecutivo(dimension, etiqueta):
        # Filtro de seguridad: Excluir Nómina para análisis de riesgo tradicional
        df_filt = df_analisis[(~df_analisis['dim_producto'].str.contains('NOMINA', case=False)) & (df_analisis['dim_sucursal'] != '999.EMPRESA NOMINA COLABORADORES')]
        
        df_exec = df_filt.groupby(['cosecha_id', dimension]).agg({'id_credito':'count', 'flag_fpd':'sum'}).reset_index()
        df_exec['tasa'] = (df_exec['flag_fpd'] * 100 / df_exec['id_credito'])
        
        datos_u = df_exec[df_exec['cosecha_id'] == c_actual].sort_values('tasa')
        datos_a = df_exec[df_exec['cosecha_id'] == c_anterior].sort_values('tasa')

        if not datos_u.empty:
            st.subheader(f"Reporte Gerencial: {etiqueta}")
            col_exec_1, col_exec_2 = st.columns(2)
            col_exec_1.success(f"✅ **Líder en Calidad:** {datos_u.iloc[0][dimension]} ({datos_u.iloc[0]['tasa']:.2f}%)")
            col_exec_2.error(f"⚠️ **Mayor Riesgo:** {datos_u.iloc[-1][dimension]} ({datos_u.iloc[-1]['tasa']:.2f}%)")
            
            # Tabla comparativa mes a mes
            df_merged = pd.merge(datos_u, datos_a[[dimension, 'tasa']], on=dimension, suffixes=('_actual', '_previo'))
            st.dataframe(df_merged[[dimension, 'id_credito', 'flag_fpd', 'tasa_actual', 'tasa_previo']].rename(columns={
                'id_credito': 'Créditos', 
                'flag_fpd': 'Casos FPD', 
                'tasa_actual': f'% FPD {c_actual}', 
                'tasa_previo': f'% FPD {c_anterior}'
            }), use_container_width=True, hide_index=True)

    renderizar_bloque_ejecutivo('dim_regional', 'Regionales')
    st.divider()
    renderizar_bloque_ejecutivo('dim_producto', 'Productos')
    st.divider()
    renderizar_bloque_ejecutivo('dim_sucursal', 'Sucursales')

# TAB 3: ANÁLISIS PARETO
with t3:
    st.header("💡 Insights de Concentración de Riesgo")
    
    # Pareto de Sucursales
    st.subheader(f"Sucursales con Mayor Impacto en FPD ({mes_nombre_str})")
    df_pareto = df_analisis[df_analisis['cosecha_id'] == c_actual].groupby('dim_sucursal').agg({'flag_fpd':'sum'}).reset_index().sort_values('flag_fpd', ascending=False)
    fig_pareto = px.bar(df_pareto.head(20), x='dim_sucursal', y='flag_fpd', text_auto=True, color_discrete_sequence=['#E74C3C'])
    st.plotly_chart(fig_pareto, use_container_width=True)

    # Combo Chart: Volumen vs Calidad
    st.subheader("Relación Volumen de Colocación vs Tasa de Riesgo")
    df_analisis['rango_monto'] = pd.cut(df_analisis['monto_otorgado'], bins=[0, 5000, 10000, 15000, 20000, 100000], labels=['$0-5k', '$5-10k', '$10-15k', '$15-20k', '+$20k'])
    res_combo = df_analisis[df_analisis['cosecha_id'] == c_actual].groupby('rango_monto', observed=False).agg({'id_credito':'count', 'flag_fpd':'sum'}).reset_index()
    res_combo['tasa'] = (res_combo['flag_fpd'] * 100 / res_combo['id_credito'])
    
    fig_combo = make_subplots(specs=[[{"secondary_y": True}]])
    fig_combo.add_trace(go.Bar(x=res_combo['rango_monto'], y=res_combo['id_credito'], name="Colocación (Q)", marker_color='#3498DB'), secondary_y=False)
    fig_combo.add_trace(go.Scatter(x=res_combo['rango_monto'], y=res_combo['tasa'], name="Tasa %", mode='lines+markers', line=dict(color='red', width=3)), secondary_y=True)
    fig_combo.update_layout(plot_bgcolor='white', height=500)
    st.plotly_chart(fig_combo, use_container_width=True)

# TAB 4: EXPORTAR DATOS
with t4:
    st.header("📥 Centro de Descarga de Detalle")
    cosecha_sel = st.selectbox("Seleccione la Cosecha a Exportar:", options=sorted(df_analisis['cosecha_id'].unique(), reverse=True))
    df_export = df_analisis[df_analisis['cosecha_id'] == cosecha_sel].drop(columns=['cosecha_id', 'anio', 'mes_id'])
    st.dataframe(df_export, use_container_width=True, hide_index=True)
    st.download_button("💾 Descargar CSV", data=df_export.to_csv(index=False).encode('utf-8'), file_name=f"Detalle_FPD_{cosecha_sel}.csv")

# --- 8. PIE DE PÁGINA DINÁMICO ---
st.markdown("---")
update_label = obtener_fecha_github()
st.markdown(f"""
    <div style='text-align: center; color: #7f8c8d; font-size: 0.9em;'>
        🛡️ <b>Monitor de Riesgo FPD - Versión Dashboard Pro</b> | 
        📅 Última actualización de datos (GitHub): <b>{update_label}</b> (Hora México) <br>
        <i>Este reporte se genera mediante procesamiento in-memory DuckDB sobre archivos Parquet.</i>
    </div>
""", unsafe_allow_html=True)