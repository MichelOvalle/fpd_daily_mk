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

# Diccionario para nombres de meses
MESES_NOMBRE = {
    '01': 'enero', '02': 'febrero', '03': 'marzo', '04': 'abril',
    '05': 'mayo', '06': 'junio', '07': 'julio', '08': 'agosto',
    '09': 'septiembre', '10': 'octubre', '11': 'noviembre', '12': 'diciembre'
}

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
        SELECT * FROM base WHERE 1=1
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

# 4. Función genérica para Tab 2 (Independiente y SIN NOMINAS)
@st.cache_data
def get_exec_data(field):
    query = f"""
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito, 
            COALESCE({field}, 'N/A') as dimension,
            producto_agrupado
        FROM 'fpd_gemini.parquet'
        WHERE UPPER(producto_agrupado) NOT LIKE '%NOMINA%'
    )
    SELECT 
        strftime(fecha_dt, '%Y%m') as cosecha_id,
        RIGHT(strftime(fecha_dt, '%Y%m'), 2) as mes_id,
        dimension,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si,
        (SUM(fpd2_num) * 100.0 / COUNT(id_credito)) as fpd2_rate
    FROM base 
    WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH) AND fecha_dt IS NOT NULL
    GROUP BY ALL ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- SIDEBAR ---
st.sidebar.header("🎯 Filtros Monitor FPD")
opt = get_filter_universes()
sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))

st.title("📊 Monitor de Riesgo Crediticio")
tab1, tab2, tab3, tab4 = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "🏢 Por Sucursal", "📋 Detalle de Datos"])

# --- TAB 1: MONITOR FPD (RESTAURADO) ---
with tab1:
    df1 = get_tab1_data(sel_reg, sel_suc, sel_prod, sel_tip)
    if not df1.empty:
        df_t = df1.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum', 'np_si':'sum'}).reset_index()
        df_t['fpd2_rate'] = (df_t['fpd2_si'] * 100.0 / df_t['total_casos'])
        df_t['np_rate'] = (df_t['np_si'] * 100.0 / df_t['total_casos'])
        
        k1, k2, k3, k4 = st.columns(4)
        ult_t1 = df_t.iloc[-1]
        k1.metric("Cosecha Actual", ult_t1['cosecha_id'])
        k2.metric("Créditos", f"{int(ult_t1['total_casos']):,}")
        k3.metric("Tasa FPD2", f"{ult_t1['fpd2_rate']:.2f}%")
        k4.metric("Tasa NP", f"{ult_t1['np_rate']:.2f}%")
        st.divider()

        # Fila 1
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Tendencia Global")
            fig1 = go.Figure(go.Scatter(x=df_t['cosecha_id'], y=df_t['fpd2_rate'], mode='lines+markers+text', text=df_t['fpd2_rate'].apply(lambda x: f'{x:.1f}%'), line=dict(color='#1B4F72', width=4), fill='tozeroy', fillcolor='rgba(27,79,114,0.1)', name='Global'))
            fig1.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            st.subheader("FPD2 por Origen")
            df_o = df1.groupby(['cosecha_id', 'origen2']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_o['fpd2_rate'] = (df_o['fpd2_si'] * 100.0 / df_o['total_casos'])
            fig2 = px.line(df_o, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True, color_discrete_map={'fisico':'#2E86C1','digital':'#CB4335'})
            fig2.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig2, use_container_width=True)

        # Fila 2
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Comparativa Interanual")
            df_y = df1.groupby(['anio', 'mes']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_y['fpd2_rate'] = (df_y['fpd2_si'] * 100.0 / df_y['total_casos'])
            fig3 = px.line(df_y[df_y['anio'].isin([2023, 2024, 2025])], x='mes', y='fpd2_rate', color='anio', markers=True)
            fig3.update_layout(xaxis=dict(ticktext=list(MESES_NOMBRE.values()), tickvals=list(MESES_NOMBRE.keys())), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig3, use_container_width=True)
        with c4:
            st.subheader("FPD2 vs NP")
            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=df_t['cosecha_id'], y=df_t['fpd2_rate'], name='% FPD2', line=dict(color='#1B4F72')))
            fig4.add_trace(go.Scatter(x=df_t['cosecha_id'], y=df_t['np_rate'], name='% NP', line=dict(color='#D35400', dash='dash')))
            fig4.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig4, use_container_width=True)

        st.subheader("Tipo Cliente (Sin Formers)")
        df_tc = df1[df1['tipo_cliente'] != 'Formers'].groupby(['cosecha_id', 'tipo_cliente']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
        df_tc['fpd2_rate'] = (df_tc['fpd2_si'] * 100.0 / df_tc['total_casos'])
        fig5 = px.line(df_tc, x='cosecha_id', y='fpd2_rate', color='tipo_cliente', markers=True)
        fig5.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=400, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig5, use_container_width=True)

        st.divider()
        # Rankings Sucursales
        cosechas = sorted(df1['cosecha_id'].unique())
        ant_c = cosechas[-2] if len(cosechas) > 1 else cosechas[-1]
        df_r_c = df1[df1['cosecha_id'] == cosechas[-1]].groupby('sucursal').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd2_si'] * 100.0 / df_r_c['total_casos'])
        df_r_p = df1[df1['cosecha_id'] == ant_c].groupby('sucursal').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd2_si'] * 100.0 / df_r_p['total_casos'])
        df_rf = pd.merge(df_r_c, df_r_p[['sucursal', 'rate_ant']], on='sucursal', how='left')
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {cosechas[-1]}")
        cr1, cr2 = st.columns(2)
        conf = {"sucursal":"Sucursal", "total_casos":"Créditos", "rate":st.column_config.NumberColumn("% FPD", format="%.2f%%"), "rate_ant":st.column_config.NumberColumn("% Ant", format="%.2f%%")}
        cr1.dataframe(df_rf.sort_values('rate', ascending=False).head(10), column_config=conf, hide_index=True, use_container_width=True)
        cr2.dataframe(df_rf.sort_values('rate', ascending=True).head(10), column_config=conf, hide_index=True, use_container_width=True)

# --- TAB 2: RESUMEN EJECUTIVO (Regionales + Productos) ---
with tab2:
    df_reg_all = get_exec_data('unidad_regional')
    df_prod_all = get_exec_data('producto_agrupado')
    
    if not df_reg_all.empty:
        st.header("💼 Análisis Ejecutivo")
        
        # 1. Variables de tiempo
        lista_c = sorted(df_reg_all['cosecha_id'].unique())
        ult_c = lista_c[-1]
        ant_c = lista_c[-2] if len(lista_c) > 1 else ult_c
        mes_u = MESES_NOMBRE.get(ult_c[-2:], 'N/A')
        mes_a = MESES_NOMBRE.get(ant_c[-2:], 'N/A')

        # --- SECCIÓN REGIONAL ---
        st.subheader(f"📍 Análisis por Unidad Regional")
        df_r_u = df_reg_all[df_reg_all['cosecha_id'] == ult_c].sort_values('fpd2_rate')
        df_r_a = df_reg_all[df_reg_all['cosecha_id'] == ant_c].sort_values('fpd2_rate')
        
        c_r1, c_r2 = st.columns(2)
        with c_r1:
            st.success(f"**Regional - Desempeño Destacado:** La mejor unidad es **{df_r_u.iloc[0]['dimension']}** con un **{df_r_u.iloc[0]['fpd2_rate']:.2f}%** en el mes de **{mes_u}**, mientras que en **{mes_a}** fue **{df_r_a.iloc[0]['dimension']}** con un **{df_r_a.iloc[0]['fpd2_rate']:.2f}%**.")
        with c_r2:
            st.error(f"**Regional - Foco de Atención:** La unidad con mayor riesgo es **{df_r_u.iloc[-1]['dimension']}** con un **{df_r_u.iloc[-1]['fpd2_rate']:.2f}%** en el mes de **{mes_u}**, mientras que en **{mes_a}** fue **{df_r_a.iloc[-1]['dimension']}** con un **{df_r_a.iloc[-1]['fpd2_rate']:.2f}%**.")

        # Tabla Regional
        df_r_tab = pd.merge(df_r_u, df_r_a[['dimension', 'fpd2_rate']].rename(columns={'fpd2_rate':'rate_ant'}), on='dimension', how='left')
        st.dataframe(df_r_tab[['dimension', 'total_casos', 'fpd2_si', 'fpd2_rate', 'rate_ant']].style.background_gradient(subset=['fpd2_rate'], cmap='YlOrRd').format({'fpd2_rate':'{:.2f}%', 'rate_ant':'{:.2f}%'}),
                     use_container_width=True, hide_index=True, column_config={"dimension":"Regional", "total_casos":"Créditos", "fpd2_si":"FPD Si", "fpd2_rate":f"% {mes_u.capitalize()}", "rate_ant":f"% {mes_a.capitalize()}"})

        st.divider()

        # --- SECCIÓN PRODUCTO ---
        st.subheader(f"📦 Análisis por Producto Agrupado")
        df_p_u = df_prod_all[df_prod_all['cosecha_id'] == ult_c].sort_values('fpd2_rate')
        df_p_a = df_prod_all[df_prod_all['cosecha_id'] == ant_c].sort_values('fpd2_rate')
        
        c_p1, c_p2 = st.columns(2)
        with c_p1:
            st.success(f"**Producto - Desempeño Destacado:** El mejor producto es **{df_p_u.iloc[0]['dimension']}** con un **{df_p_u.iloc[0]['fpd2_rate']:.2f}%** en el mes de **{mes_u}**, mientras que en **{mes_a}** fue **{df_p_a.iloc[0]['dimension']}** con un **{df_p_a.iloc[0]['fpd2_rate']:.2f}%**.")
        with c_p2:
            st.error(f"**Producto - Foco de Atención:** El producto con mayor riesgo es **{df_p_u.iloc[-1]['dimension']}** con un **{df_p_u.iloc[-1]['fpd2_rate']:.2f}%** en el mes de **{mes_u}**, mientras que en **{mes_a}** fue **{df_p_a.iloc[-1]['dimension']}** con un **{df_p_a.iloc[-1]['fpd2_rate']:.2f}%**.")

        # Tabla Producto (Lo que pediste)
        df_p_tab = pd.merge(df_p_u, df_p_a[['dimension', 'fpd2_rate']].rename(columns={'fpd2_rate':'rate_ant'}), on='dimension', how='left')
        st.dataframe(df_p_tab[['dimension', 'total_casos', 'fpd2_si', 'fpd2_rate', 'rate_ant']].style.background_gradient(subset=['fpd2_rate'], cmap='YlOrRd').format({'fpd2_rate':'{:.2f}%', 'rate_ant':'{:.2f}%'}),
                     use_container_width=True, hide_index=True, column_config={"dimension":"Producto", "total_casos":"Créditos", "fpd2_si":"FPD Si", "fpd2_rate":f"% {mes_u.capitalize()}", "rate_ant":f"% {mes_a.capitalize()}"})

with tab3: st.info("Pestaña Por Sucursal vacía.")
with tab4: st.info("Pestaña Detalle de Datos vacía.")