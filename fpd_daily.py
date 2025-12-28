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
    filtrado_inicial AS (
        SELECT * FROM base
        WHERE 1=1
        {"AND unidad_regional IN (" + to_sql_list(regionales) + ")" if regionales else ""}
        {"AND sucursal IN (" + to_sql_list(sucursales) + ")" if sucursales else ""}
        {"AND producto_agrupado IN (" + to_sql_list(productos) + ")" if productos else ""}
        {"AND tipo_cliente IN (" + to_sql_list(tipos) + ")" if tipos else ""}
    ),
    final AS (
        SELECT * FROM filtrado_inicial 
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
    )
    SELECT 
        strftime(fecha_dt, '%Y%m') as cosecha_id,
        EXTRACT(YEAR FROM fecha_dt) as anio,
        strftime(fecha_dt, '%m') as mes,
        origen2, tipo_cliente, sucursal, unidad_regional,
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
st.title("📊 Dashboard de Control de Riesgo")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "🏢 Por Sucursal", "📋 Detalle de Datos"])

# Obtención de datos filtrados para todas las pestañas
df_raw = get_filtered_data(sel_reg, sel_suc, sel_prod, sel_tip)

# --- TAB 1: MONITOR FPD ---
with tab1:
    if not df_raw.empty:
        df_total = df_raw.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum', 'np_si':'sum'}).reset_index()
        df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
        df_total['np_rate'] = (df_total['np_si'] * 100.0 / df_total['total_casos'])
        
        # KPIs y Gráficas de la pestaña 1 (Mantenidas)
        k1, k2, k3, k4 = st.columns(4)
        ult = df_total.iloc[-1]
        k1.metric("Cosecha Actual", ult['cosecha_id'])
        k2.metric("Créditos", f"{int(ult['total_casos']):,}")
        k3.metric("Tasa FPD2", f"{ult['fpd2_rate']:.2f}%")
        k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%")
        st.info("Pestaña Monitor FPD restablecida con éxito.")

# --- TAB 2: RESUMEN EJECUTIVO (NUEVA LÓGICA) ---
with tab2:
    if not df_raw.empty:
        st.header("💼 Análisis de Unidades Regionales")
        
        # 1. Obtener la última cosecha disponible
        ultima_cosecha = df_raw['cosecha_id'].max()
        st.subheader(f"Desempeño Regional - Cosecha {ultima_cosecha}")

        # 2. Agrupar por Regional para esa cosecha
        df_reg_last = df_raw[df_raw['cosecha_id'] == ultima_cosecha].groupby('unidad_regional').agg({
            'total_casos': 'sum',
            'fpd2_si': 'sum'
        }).reset_index()
        df_reg_last['fpd2_rate'] = (df_reg_last['fpd2_si'] * 100.0 / df_reg_last['total_casos'])
        
        # 3. Ordenar para encontrar mejor y peor
        df_reg_sorted = df_reg_last.sort_values('fpd2_rate', ascending=True)
        
        mejor_reg = df_reg_sorted.iloc[0]
        peor_reg = df_reg_sorted.iloc[-1]

        # 4. Mostrar KPIs de impacto
        c_mejor, c_peor = st.columns(2)
        
        with c_mejor:
            st.success(f"🌟 **MEJOR REGIONAL: {mejor_reg['unidad_regional']}**")
            st.metric(label="Tasa FPD2 Mínima", value=f"{mejor_reg['fpd2_rate']:.2f}%")
            st.caption(f"Basado en {int(mejor_reg['total_casos'])} créditos colocados.")

        with c_peor:
            st.error(f"⚠️ **PEOR REGIONAL: {peor_reg['unidad_regional']}**")
            st.metric(label="Tasa FPD2 Máxima", value=f"{peor_reg['fpd2_rate']:.2f}%")
            st.caption(f"Basado en {int(peor_reg['total_casos'])} créditos colocados.")

        st.divider()

        # 5. Gráfica de Barras para comparar todas las regionales de la última cosecha
        st.markdown("### Comparativa de Riesgo por Unidad Regional")
        fig_reg = px.bar(
            df_reg_sorted,
            x='unidad_regional',
            y='fpd2_rate',
            text=df_reg_sorted['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
            labels={'unidad_regional': 'Unidad Regional', 'fpd2_rate': '% FPD2'},
            color='fpd2_rate',
            color_continuous_scale='RdYlGn_r' # Rojo para el alto, Verde para el bajo
        )
        
        fig_reg.update_traces(textposition='outside')
        fig_reg.update_layout(
            plot_bgcolor='white',
            height=450,
            showlegend=True,
            legend=LEGEND_BOTTOM,
            coloraxis_showscale=False # Ocultamos la barra de color lateral
        )
        st.plotly_chart(fig_reg, use_container_width=True)

        # 6. Tabla detallada
        with st.expander("Ver detalle completo de regionales"):
            st.dataframe(df_reg_sorted[['unidad_regional', 'total_casos', 'fpd2_si', 'fpd2_rate']].rename(columns={
                'unidad_regional': 'Regional', 'total_casos': 'Créditos Totales', 'fpd2_si': 'Casos FPD', 'fpd2_rate': '% Tasa FPD'
            }), hide_index=True, use_container_width=True)

    else:
        st.warning("No hay datos disponibles para mostrar el resumen ejecutivo.")

# Pestañas restantes vacías
with tab3: pass
with tab4: pass