import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Intentar importar duckdb
try:
    import duckdb
except ImportError:
    st.error("Falta la librería 'duckdb'. Instálala ejecutando: pip install duckdb")
    st.stop()

# 1. Configuración de la página
st.set_page_config(
    page_title="FPD Daily - Dashboard",
    layout="wide",
    page_icon="📊"
)

# Estilo global para leyendas debajo del eje X
LEGEND_BOTTOM = dict(
    orientation="h",
    yanchor="top",
    y=-0.25,
    xanchor="center",
    x=0.5
)

# 2. Carga de universos para filtros (Manejo de Nulos)
@st.cache_data
def get_filter_universes():
    con = duckdb.connect()
    df = con.execute("""
        SELECT DISTINCT 
            COALESCE(unidad_regional, 'N/A') as unidad_regional, 
            COALESCE(sucursal, 'N/A') as sucursal, 
            COALESCE(producto_agrupado, 'N/A') as producto_agrupado, 
            COALESCE(tipo_cliente, 'N/A') as tipo_cliente 
        FROM 'fpd_gemini.parquet'
    """).df()
    return df

# 3. Función de procesamiento con filtros inteligentes (Vacio = Todos)
@st.cache_data
def get_filtered_data(regionales, sucursales, productos, tipos):
    def to_sql_list(lista):
        return "'" + "','".join(lista) + "'"

    query = f"""
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            CASE WHEN NP = 'NP' THEN 1 ELSE 0 END as np_num,
            id_credito, origen2, 
            COALESCE(tipo_cliente, 'N/A') as tipo_cliente, 
            COALESCE(sucursal, 'N/A') as sucursal, 
            COALESCE(unidad_regional, 'N/A') as unidad_regional, 
            COALESCE(producto_agrupado, 'N/A') as producto_agrupado
        FROM 'fpd_gemini.parquet'
    ),
    filtrado AS (
        SELECT * FROM base
        WHERE (tipo_cliente IS NOT NULL)
        {"AND unidad_regional IN (" + to_sql_list(regionales) + ")" if regionales else ""}
        {"AND sucursal IN (" + to_sql_list(sucursales) + ")" if sucursales else ""}
        {"AND producto_agrupado IN (" + to_sql_list(productos) + ")" if productos else ""}
        {"AND tipo_cliente IN (" + to_sql_list(tipos) + ")" if tipos else ""}
    ),
    final AS (
        SELECT * FROM filtrado 
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
    )
    SELECT 
        strftime(fecha_dt, '%Y%m') as cosecha_id,
        EXTRACT(YEAR FROM fecha_dt) as anio,
        strftime(fecha_dt, '%m') as mes,
        origen2, tipo_cliente, sucursal, unidad_regional, producto_agrupado,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si,
        SUM(np_num) as np_si
    FROM final
    GROUP BY ALL
    ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- BARRA LATERAL (FILTROS) ---
st.sidebar.header("🎯 Filtros de Cartera")
opt = get_filter_universes()

sel_reg = st.sidebar.multiselect("📍 Unidad Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo de Cliente", options=sorted(opt['tipo_cliente'].unique()))

# --- CUERPO PRINCIPAL ---
st.title("📊 Monitor de Desempeño Crediticio")

# Definición de pestañas
tab1, tab2, tab3, tab4 = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "🏢 Por Sucursal", "📋 Detalle de Datos"])

# Obtener datos base una sola vez para ambas pestañas
try:
    df_raw = get_filtered_data(sel_reg, sel_suc, sel_prod, sel_tip)
    
    # --- PESTAÑA 1: MONITOR FPD (Tu estructura actual) ---
    with tab1:
        if not df_raw.empty:
            df_total = df_raw.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum', 'np_si':'sum'}).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
            df_total['np_rate'] = (df_total['np_si'] * 100.0 / df_total['total_casos'])
            
            df_origen = df_raw.groupby(['cosecha_id', 'origen2']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_origen['fpd2_rate'] = (df_origen['fpd2_si'] * 100.0 / df_origen['total_casos'])

            # KPIs
            ultima_cosecha = df_total['cosecha_id'].max()
            k1, k2, k3, k4 = st.columns(4)
            ult = df_total.iloc[-1]
            k1.metric("Cosecha Actual", ultima_cosecha)
            k2.metric("Créditos", f"{int(ult['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ult['fpd2_rate']:.2f}%")
            k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%")

            st.divider()

            # Fila 1 y 2 de gráficas... (Mantenemos toda la lógica visual de Monitor FPD)
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Tendencia Global (FPD2)")
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['fpd2_rate'], mode='lines+markers+text',
                    text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'), line=dict(color='#1B4F72', width=4), 
                    fill='tozeroy', fillcolor='rgba(27, 79, 114, 0.1)', name='FPD2 Global'))
                fig1.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig1, use_container_width=True)
            with c2:
                st.subheader("FPD2 por Origen")
                fig2 = px.line(df_origen, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True, color_discrete_map={'fisico': '#2E86C1', 'digital': '#CB4335'})
                fig2.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig2, use_container_width=True)

    # --- PESTAÑA 2: RESUMEN EJECUTIVO (Nueva!) ---
    with tab2:
        if not df_raw.empty:
            st.header("💼 Resumen Ejecutivo de Riesgo")
            st.markdown("Vista matricial de la tasa FPD2 por principales dimensiones.")

            col_mat1, col_mat2 = st.columns(2)

            with col_mat1:
                st.subheader("📍 Riesgo por Unidad Regional")
                # Crear matriz Regional vs Cosecha
                df_reg_mat = df_raw.groupby(['unidad_regional', 'cosecha_id']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
                df_reg_mat['fpd2_rate'] = (df_reg_mat['fpd2_si'] * 100.0 / df_reg_mat['total_casos'])
                
                # Pivotar para formato tabla
                matriz_reg = df_reg_mat.pivot(index='unidad_regional', columns='cosecha_id', values='fpd2_rate')
                
                st.dataframe(matriz_reg.style.background_gradient(cmap='YlOrRd', axis=None).format("{:.2f}%"), use_container_width=True)

            with col_mat2:
                st.subheader("📦 Riesgo por Producto")
                # Crear matriz Producto vs Cosecha
                df_prod_mat = df_raw.groupby(['producto_agrupado', 'cosecha_id']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
                df_prod_mat['fpd2_rate'] = (df_prod_mat['fpd2_si'] * 100.0 / df_prod_mat['total_casos'])
                
                # Pivotar para formato tabla
                matriz_prod = df_prod_mat.pivot(index='producto_agrupado', columns='cosecha_id', values='fpd2_rate')
                
                st.dataframe(matriz_prod.style.background_gradient(cmap='YlOrRd', axis=None).format("{:.2f}%"), use_container_width=True)

            st.divider()

            # Resumen Ejecutivo en Barras (Volumen vs Riesgo)
            st.subheader("Análisis de Concentración de Riesgo (Últimos 6 meses)")
            df_last_6 = df_total.tail(6)
            
            fig_exec = go.Figure()
            fig_exec.add_trace(go.Bar(x=df_last_6['cosecha_id'], y=df_last_6['total_casos'], name="Volumen Créditos", marker_color='#BDC3C7'))
            fig_exec.add_trace(go.Scatter(x=df_last_6['cosecha_id'], y=df_last_6['fpd2_rate'], name="% FPD2", 
                                          mode='lines+markers+text', text=df_last_6['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                                          line=dict(color='#E74C3C', width=3), yaxis="y2"))
            
            fig_exec.update_layout(
                yaxis=dict(title="Volumen"),
                yaxis2=dict(title="Tasa FPD2 (%)", overlaying="y", side="right", ticksuffix="%"),
                plot_bgcolor='white', legend=LEGEND_BOTTOM, height=450
            )
            st.plotly_chart(fig_exec, use_container_width=True)

except Exception as e:
    st.error(f"Error en el procesamiento: {e}")

# Pestañas restantes vacías
with tab3: pass
with tab4: pass