import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb  # Importación global para evitar errores

# --- 1. CONFIGURACIÓN Y ESTILOS ---
st.set_page_config(page_title="Monitor FPD - Dashboard Pro", layout="wide", page_icon="📊")

MESES_NOMBRE = {
    '01': 'enero', '02': 'febrero', '03': 'marzo', '04': 'abril', '05': 'mayo', '06': 'junio',
    '07': 'julio', '08': 'agosto', '09': 'septiembre', '10': 'octubre', '11': 'noviembre', '12': 'diciembre'
}

LEGEND_BOTTOM = dict(orientation="h", yanchor="top", y=-0.3, xanchor="center", x=0.5)

# --- 2. FUNCIONES DE DATOS ---

@st.cache_data
def get_filter_universes():
    con = duckdb.connect()
    return con.execute("""
        SELECT DISTINCT 
            COALESCE(unidad_regional, 'N/A') as unidad_regional, 
            COALESCE(sucursal, 'N/A') as sucursal, 
            COALESCE(producto_agrupado, 'N/A') as producto_agrupado, 
            COALESCE(tipo_cliente, 'N/A') as tipo_cliente 
        FROM 'fpd_gemini.parquet'
    """).df()

@st.cache_data
def get_main_data(regionales, sucursales, productos, tipos):
    def to_sql_list(lista):
        return "'" + "','".join(lista) + "'"
    # Se eliminó el INTERVAL 2 MONTH para que cuadre con tu Parquet al 100%
    query = f"""
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd_num,
            CASE WHEN NP = 'NP' THEN 1 ELSE 0 END as np_num,
            id_credito, id_segmento, id_producto, origen2, monto_otorgado, cuota, fpd2,
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
    SELECT *, strftime(fecha_dt, '%Y%m') as cosecha_id, EXTRACT(YEAR FROM fecha_dt) as anio, strftime(fecha_dt, '%m') as mes
    FROM filtrado WHERE fecha_dt IS NOT NULL
    """
    return duckdb.query(query).to_df()

@st.cache_data
def get_executive_data(field):
    query = f"""
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd_num,
            id_credito, COALESCE({field}, 'N/A') as dimension,
            producto_agrupado, sucursal
        FROM 'fpd_gemini.parquet'
        WHERE UPPER(producto_agrupado) NOT LIKE '%NOMINA%'
          AND sucursal != '999.EMPRESA NOMINA COLABORADORES'
    )
    SELECT strftime(fecha_dt, '%Y%m') as cosecha_id, 
           dimension, COUNT(id_credito) as total_vol, SUM(fpd_num) as fpd_si,
           (SUM(fpd_num) * 100.0 / COUNT(id_credito)) as fpd_rate
    FROM base WHERE fecha_dt IS NOT NULL
    GROUP BY ALL ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- 3. PROCESAMIENTO SIDEBAR ---
opt = get_filter_universes()
st.sidebar.header("🎯 Filtros Dashboard")
sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))

df_main = get_main_data(sel_reg, sel_suc, sel_prod, sel_tip)

st.title("📊 Monitor de Riesgo FPD")
tabs = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "💡 Insights Estratégicos", "📥 Exportar"])

# --- TAB 1: MONITOR FPD ---
with tabs[0]:
    if not df_main.empty:
        df_t = df_main.groupby('cosecha_id').agg({'id_credito':'count', 'fpd_num':'sum', 'np_num':'sum'}).reset_index()
        df_t['%FPD'] = (df_t['fpd_num'] * 100 / df_t['id_credito'])
        df_t['np_rate'] = (df_t['np_num'] * 100 / df_t['id_credito'])
        ult = df_t.iloc[-1]; ant = df_t.iloc[-2] if len(df_t) > 1 else ult
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Cosecha Actual", ult['cosecha_id'], f"Anterior: {ant['cosecha_id']}", delta_color="off")
        k2.metric("Créditos", f"{int(ult['id_credito']):,}", f"{int(ult['id_credito'] - ant['id_credito']):+,} vs mes ant")
        k3.metric("Tasa FPD", f"{ult['%FPD']:.2f}%", f"{ult['%FPD'] - ant['%FPD']:.2f}% vs mes ant", delta_color="inverse")
        k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%", f"{ult['np_rate'] - ant['np_rate']:.2f}% vs mes ant", delta_color="inverse")
        st.divider()

        # Tendencia Global
        st.subheader("1. Tendencia Global (%FPD)")
        fig1 = px.line(df_t, x='cosecha_id', y='%FPD', markers=True, text=df_t['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig1.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450)
        st.plotly_chart(fig1, use_container_width=True)

        # Por Origen
        st.subheader("2. FPD por Origen")
        df_o = df_main.groupby(['cosecha_id', 'origen2']).agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_o['%FPD'] = (df_o['fpd_num'] * 100 / df_o['id_credito'])
        fig2 = px.line(df_o, x='cosecha_id', y='%FPD', color='origen2', markers=True, text=df_o['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig2.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig2, use_container_width=True)

        # Histórico Indicadores
        st.subheader("4. Histórico Indicadores (Últimas 24 Cosechas)")
        df_t_24 = df_t.tail(24)
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(x=df_t_24['cosecha_id'], y=df_t_24['%FPD'], name='% FPD', mode='lines+markers+text', text=df_t_24['%FPD'].apply(lambda x: f'{x:.1f}%'), textposition="top center"))
        fig4.add_trace(go.Scatter(x=df_t_24['cosecha_id'], y=df_t_24['np_rate'], name='% NP', mode='lines+markers+text', text=df_t_24['np_rate'].apply(lambda x: f'{x:.1f}%'), textposition="bottom center", line=dict(dash='dash')))
        fig4.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig4, use_container_width=True)

        st.divider()
        cosechas = sorted(df_main['cosecha_id'].unique()); ult_c_id = cosechas[-1]; ant_c_id = cosechas[-2] if len(cosechas) > 1 else ult_c_id
        df_r_c = df_main[df_main['cosecha_id'] == ult_c_id].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd_num'] * 100 / df_r_c['id_credito'])
        df_r_p = df_main[df_main['cosecha_id'] == ant_c_id].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd_num'] * 100 / df_r_p['id_credito'])
        df_rf = pd.merge(df_r_c, df_r_p[['sucursal', 'id_credito', 'rate_ant']], on='sucursal', how='left', suffixes=('', '_ant'))
        
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {ult_c_id}")
        conf_rank = {"sucursal": "Sucursal", "id_credito": f"Créditos {ult_c_id}", "id_credito_ant": f"Créditos {ant_c_id}", "fpd_num": st.column_config.NumberColumn(f"Casos FPD {ult_c_id}", format="%d"), "rate": st.column_config.NumberColumn(f"%FPD {ult_c_id}", format="%.2f%%"), "rate_ant": st.column_config.NumberColumn(f"%FPD {ant_c_id}", format="%.2f%%")}
        st.dataframe(df_rf.sort_values('rate', ascending=False).head(10), column_config=conf_rank, hide_index=True, use_container_width=True)

# --- TAB 2: RESUMEN EJECUTIVO (INDEPENDIENTE) ---
with tabs[1]:
    st.header("💼 Resumen Ejecutivo Gerencial")
    def render_exec_block(field, dim_label):
        df_e = get_executive_data(field)
        if not df_e.empty:
            lista_c = sorted(df_e['cosecha_id'].unique()); ult_c_e = lista_c[-1]; ant_c_e = lista_c[-2] if len(lista_c) > 1 else ult_c_e
            mes_u = MESES_NOMBRE.get(ult_c_e[-2:]); mes_a = MESES_NOMBRE.get(ant_c_e[-2:])
            df_u = df_e[df_e['cosecha_id'] == ult_c_e].sort_values('fpd_rate'); df_a = df_e[df_e['cosecha_id'] == ant_c_e].sort_values('fpd_rate')
            
            c1, c2 = st.columns(2)
            c1.success(f"**{dim_label} Destacada:** {df_u.iloc[0]['dimension']} ({df_u.iloc[0]['fpd_rate']:.2f}% en {mes_u})")
            c2.error(f"**{dim_label} Riesgosa:** {df_u.iloc[-1]['dimension']} ({df_u.iloc[-1]['fpd_rate']:.2f}% en {mes_u})")
            
            df_tab = pd.merge(df_u[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']], df_a[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']].rename(columns={'total_vol':'vol_ant','fpd_si':'fpd_ant','fpd_rate':'rate_ant'}), on='dimension', how='left')
            st.dataframe(df_tab.style.background_gradient(subset=['fpd_rate','rate_ant'], cmap='YlOrRd').format({'fpd_rate':'{:.2f}%','rate_ant':'{:.2f}%','fpd_si':'{:,.0f}','fpd_ant':'{:,.0f}','total_vol':'{:,.0f}','vol_ant':'{:,.0f}'}),
                         use_container_width=True, hide_index=True, column_config={"dimension":dim_label, "total_vol":f"Créditos {mes_u.capitalize()}", "vol_ant":f"Créditos {mes_a.capitalize()}", "fpd_si":f"Casos FPD {mes_u.capitalize()}", "fpd_ant":f"Casos FPD {mes_a.capitalize()}", "fpd_rate":f"%FPD {mes_u.capitalize()}", "rate_ant":f"%FPD {mes_a.capitalize()}"})
            st.divider()
    render_exec_block('unidad_regional', 'Regional'); render_exec_block('producto_agrupado', 'Producto')

# --- TAB 4: EXPORTAR (DINÁMICA) ---
with tabs[3]:
    if not df_main.empty:
        ult_cosecha_act = df_main['cosecha_id'].max()
        st.header(f"📥 Exportar Detalle FPD (Cosecha {ult_cosecha_act})")
        df_exp = df_main[(df_main['cosecha_id'] == ult_cosecha_act) & (df_main['fpd2'] == 'FPD')].copy()
        cols = ['id_credito', 'id_segmento', 'id_producto', 'producto_agrupado', 'origen2', 'cosecha_id', 'monto_otorgado', 'cuota', 'sucursal']
        st.subheader(f"Casos FPD encontrados: {len(df_exp)}")
        st.dataframe(df_exp[cols].head(15), use_container_width=True, hide_index=True)
        st.download_button(label=f"💾 Descargar CSV", data=df_exp[cols].to_csv(index=False).encode('utf-8'), file_name=f'detalle_fpd_{ult_cosecha_act}.csv', mime='text/csv')