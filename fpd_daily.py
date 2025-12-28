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
    y=-0.3,
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

# 3. Función para Tab 1 (Dependiente de Filtros)
@st.cache_data
def get_tab1_data(regionales, sucursales, productos, tipos):
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
        WHERE 1=1
        {"AND unidad_regional IN (" + to_sql_list(regionales) + ")" if regionales else ""}
        {"AND sucursal IN (" + to_sql_list(sucursales) + ")" if sucursales else ""}
        {"AND producto_agrupado IN (" + to_sql_list(productos) + ")" if productos else ""}
        {"AND tipo_cliente IN (" + to_sql_list(tipos) + ")" if tipos else ""}
    )
    SELECT 
        strftime(fecha_dt, '%Y%m') as cosecha_id,
        EXTRACT(YEAR FROM fecha_dt) as anio,
        strftime(fecha_dt, '%m') as mes,
        origen2, tipo_cliente, sucursal,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si,
        SUM(np_num) as np_si
    FROM filtrado 
    WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH) AND fecha_dt IS NOT NULL
    GROUP BY ALL ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# 4. Función para Tab 2 (FILTRO RADICAL SIN NOMINAS)
@st.cache_data
def get_tab2_data():
    # Usamos LIKE con comodines para asegurar que nada que diga NOMINA pase
    query = """
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito, 
            COALESCE(unidad_regional, 'N/A') as unidad_regional,
            producto_agrupado
        FROM 'fpd_gemini.parquet'
        WHERE UPPER(producto_agrupado) NOT LIKE '%NOMINA%'
    )
    SELECT 
        strftime(fecha_dt, '%Y%m') as cosecha_id,
        unidad_regional,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si,
        (SUM(fpd2_num) * 100.0 / COUNT(id_credito)) as fpd2_rate
    FROM base 
    WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH) AND fecha_dt IS NOT NULL
    GROUP BY ALL ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- SIDEBAR (Solo afecta a Tab 1) ---
st.sidebar.header("🎯 Filtros Monitor FPD")
opt = get_filter_universes()
sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))

st.title("📊 Monitor de Riesgo Crediticio")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "🏢 Por Sucursal", "📋 Detalle de Datos"])

# --- TAB 1: MONITOR FPD (RESTAURADA TOTALMENTE) ---
with tab1:
    df1 = get_tab1_data(sel_reg, sel_suc, sel_prod, sel_tip)
    if not df1.empty:
        df_t = df1.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum', 'np_si':'sum'}).reset_index()
        df_t['fpd2_rate'] = (df_t['fpd2_si'] * 100.0 / df_t['total_casos'])
        df_t['np_rate'] = (df_t['np_si'] * 100.0 / df_t['total_casos'])
        
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        ult = df_t.iloc[-1]
        k1.metric("Cosecha Actual", ult['cosecha_id'])
        k2.metric("Créditos", f"{int(ult['total_casos']):,}")
        k3.metric("Tasa FPD2", f"{ult['fpd2_rate']:.2f}%")
        k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%")
        st.divider()

        # Fila 1: Global y Origen
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Tendencia Global (FPD2)")
            fig1 = go.Figure(go.Scatter(x=df_t['cosecha_id'], y=df_t['fpd2_rate'], mode='lines+markers+text',
                text=df_t['fpd2_rate'].apply(lambda x: f'{x:.1f}%'), line=dict(color='#1B4F72', width=4), 
                fill='tozeroy', fillcolor='rgba(27, 79, 114, 0.1)', name='Global'))
            fig1.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, showlegend=True, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            st.subheader("FPD2 por Origen")
            df_o = df1.groupby(['cosecha_id', 'origen2']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_o['fpd2_rate'] = (df_o['fpd2_si'] * 100.0 / df_o['total_casos'])
            fig2 = px.line(df_o, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True, color_discrete_map={'fisico':'#2E86C1','digital':'#CB4335'})
            fig2.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig2, use_container_width=True)

        # Fila 2: YoY y FPD vs NP
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Comparativa Interanual")
            df_y = df1.groupby(['anio', 'mes']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_y['fpd2_rate'] = (df_y['fpd2_si'] * 100.0 / df_y['total_casos'])
            df_y = df_y[df_y['anio'].isin([2023, 2024, 2025])]
            fig3 = px.line(df_y, x='mes', y='fpd2_rate', color=df_y['anio'].astype(str), markers=True)
            fig3.update_layout(xaxis=dict(ticktext=['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'], tickvals=['01','02','03','04','05','06','07','08','09','10','11','12']),
                               yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig3, use_container_width=True)
        with c4:
            st.subheader("FPD2 vs NP")
            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=df_t['cosecha_id'], y=df_t['fpd2_rate'], name='% FPD2', line=dict(color='#1B4F72')))
            fig4.add_trace(go.Scatter(x=df_t['cosecha_id'], y=df_t['np_rate'], name='% NP', line=dict(color='#D35400', dash='dash')))
            fig4.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig4, use_container_width=True)

        # Fila 3: Tipo Cliente (Sin Formers)
        st.subheader("Tipo Cliente (Sin Formers)")
        df_tc = df1[df1['tipo_cliente'] != 'Formers'].groupby(['cosecha_id', 'tipo_cliente']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
        df_tc['fpd2_rate'] = (df_tc['fpd2_si'] * 100.0 / df_tc['total_casos'])
        fig5 = px.line(df_tc, x='cosecha_id', y='fpd2_rate', color='tipo_cliente', markers=True)
        fig5.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=400, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig5, use_container_width=True)

        st.divider()
        # Rankings
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {ult['cosecha_id']}")
        cosechas = sorted(df1['cosecha_id'].unique())
        ant = cosechas[-2] if len(cosechas) > 1 else cosechas[-1]
        df_r_c = df1[df1['cosecha_id'] == cosechas[-1]].groupby('sucursal').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd2_si'] * 100.0 / df_r_c['total_casos'])
        df_r_p = df1[df1['cosecha_id'] == ant].groupby('sucursal').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd2_si'] * 100.0 / df_r_p['total_casos'])
        df_final_r = pd.merge(df_r_c, df_r_p[['sucursal', 'rate_ant']], on='sucursal', how='left')
        ct, cb = st.columns(2)
        conf = {"sucursal":"Sucursal", "total_casos":"Créditos", "rate":st.column_config.NumberColumn("% FPD", format="%.2f%%"), "rate_ant":st.column_config.NumberColumn("% Ant", format="%.2f%%")}
        ct.dataframe(df_final_r.sort_values('rate', ascending=False).head(10), column_config=conf, use_container_width=True, hide_index=True)
        cb.dataframe(df_final_r.sort_values('rate', ascending=True).head(10), column_config=conf, use_container_width=True, hide_index=True)

# --- TAB 2: RESUMEN EJECUTIVO (INDEPENDIENTE Y SIN NOMINAS) ---
with tab2:
    df2 = get_tab2_data()
    if not df2.empty:
        st.header("💼 Resumen Ejecutivo Regional")
        st.caption("Nota: Este resumen excluye todos los productos de NOMINA y no depende de filtros laterales.")
        
        ult_c = df2['cosecha_id'].max()
        df_rank = df2[df2['cosecha_id'] == ult_c].sort_values('fpd2_rate')
        
        m_reg, p_reg = df_rank.iloc[0], df_rank.iloc[-1]
        c_m, c_p = st.columns(2)
        
        with c_m:
            st.success(f"🏆 **MEJOR UNIDAD: {m_reg['unidad_regional']}**")
            st.metric(f"FPD2 Cosecha {ult_c}", f"{m_reg['fpd2_rate']:.2f}%")
            st.caption(f"Créditos colocados: {int(m_reg['total_casos'])}")

        with c_p:
            st.error(f"🚨 **PEOR UNIDAD: {p_reg['unidad_regional']}**")
            st.metric(f"FPD2 Cosecha {ult_c}", f"{p_reg['fpd2_rate']:.2f}%")
            st.caption(f"Créditos colocados: {int(p_reg['total_casos'])}")

        st.divider()
        st.subheader(f"📋 Detalle de Calidad por Regional - Cosecha {ult_c}")
        st.dataframe(
            df_rank.style.background_gradient(subset=['fpd2_rate'], cmap='YlOrRd')
            .format({'fpd2_rate':'{:.2f}%', 'total_casos':'{:,}'}), 
            use_container_width=True, 
            hide_index=True
        )

with tab3: st.info("Pestaña Por Sucursal vacía.")
with tab4: st.info("Pestaña Detalle de Datos vacía.")