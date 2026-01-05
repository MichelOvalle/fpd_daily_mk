import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import os

# --- 1. CONFIGURACIÓN Y ESTILOS ---
st.set_page_config(page_title="Monitor FPD - Dashboard Pro", layout="wide", page_icon="📊")

# Nombre del archivo actualizado según tu instrucción
DATA_PATH = 'fpd_gemini.parquet'

MESES_NOMBRE = {
    '01': 'enero', '02': 'febrero', '03': 'marzo', '04': 'abril', '05': 'mayo', '06': 'junio',
    '07': 'julio', '08': 'agosto', '09': 'septiembre', '10': 'octubre', '11': 'noviembre', '12': 'diciembre'
}

LEGEND_BOTTOM = dict(orientation="h", yanchor="top", y=-0.3, xanchor="center", x=0.5)

# --- 2. FUNCIONES DE DATOS ---

@st.cache_data
def get_filter_universes():
    if not os.path.exists(DATA_PATH):
        st.error(f"⚠️ El archivo '{DATA_PATH}' no se encuentra en el repositorio.")
        return pd.DataFrame(columns=['unidad_regional', 'sucursal', 'producto_agrupado', 'tipo_cliente'])
    
    return duckdb.query(f"""
        SELECT DISTINCT 
            COALESCE(unidad_regional, 'N/A') as unidad_regional, 
            COALESCE(sucursal, 'N/A') as sucursal, 
            COALESCE(producto_agrupado, 'N/A') as producto_agrupado, 
            COALESCE(tipo_cliente, 'N/A') as tipo_cliente 
        FROM '{DATA_PATH}'
    """).df()

@st.cache_data
def get_main_data(regionales, sucursales, productos, tipos):
    if not os.path.exists(DATA_PATH): 
        return pd.DataFrame()
    
    def to_sql_list(lista):
        return "'" + "','".join(lista) + "'"
    
    # Hemos eliminado strptime porque fecha_apertura ya viene como DATE
    query = f"""
    WITH base AS (
        SELECT 
            CAST(fecha_apertura AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd_num,
            CASE WHEN NP = 'NP' THEN 1 ELSE 0 END as np_num,
            id_credito, id_segmento, id_producto, origen2, monto_otorgado, cuota, fpd2,
            COALESCE(tipo_cliente, 'N/A') as tipo_cliente, 
            COALESCE(sucursal, 'N/A') as sucursal, 
            COALESCE(unidad_regional, 'N/A') as unidad_regional, 
            COALESCE(producto_agrupado, 'N/A') as producto_agrupado
        FROM '{DATA_PATH}'
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
    
    try:
        return duckdb.query(query).to_df()
    except Exception as e:
        st.error(f"❌ Error de DuckDB: {e}")
        return pd.DataFrame()

@st.cache_data
def get_executive_data(field):
    if not os.path.exists(DATA_PATH): return pd.DataFrame()
    
    # Aquí también ajustamos el CAST para evitar el error de strptime
    query = f"""
    WITH base AS (
        SELECT 
            CAST(fecha_apertura AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd_num,
            id_credito, COALESCE({field}, 'N/A') as dimension,
            producto_agrupado, sucursal
        FROM '{DATA_PATH}'
        WHERE UPPER(producto_agrupado) NOT LIKE '%NOMINA%'
          AND sucursal != '999.EMPRESA NOMINA COLABORADORES'
    )
    SELECT strftime(fecha_dt, '%Y%m') as cosecha_id, 
           dimension, COUNT(id_credito) as total_vol, 
           SUM(fpd_num) as fpd_si, 
           (SUM(fpd_num) * 100.0 / COUNT(id_credito)) as fpd_rate
    FROM base WHERE fecha_dt IS NOT NULL
    GROUP BY ALL ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- 3. PROCESAMIENTO SIDEBAR ---
opt = get_filter_universes()

st.sidebar.header("🎯 Filtros Dashboard")
if not opt.empty:
    sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
    suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
    sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
    sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
    sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))
else:
    sel_reg = sel_suc = sel_prod = sel_tip = []

df_main = get_main_data(sel_reg, sel_suc, sel_prod, sel_tip)

# --- 4. LÓGICA DE FILTRADO GLOBAL ---
df_fpd = pd.DataFrame()
if not df_main.empty:
    max_c_real = df_main['cosecha_id'].max()
    df_fpd = df_main[df_main['cosecha_id'] < max_c_real].copy()
    
    lista_cosechas = sorted(df_fpd['cosecha_id'].unique())
    if lista_cosechas:
        ult_c_id = lista_cosechas[-1]
        ant_c_id = lista_cosechas[-2] if len(lista_cosechas) > 1 else ult_c_id
        mes_u_nombre = MESES_NOMBRE.get(ult_c_id[-2:], 'N/A').capitalize()
        mes_a_nombre = MESES_NOMBRE.get(ant_c_id[-2:], 'N/A').capitalize()

st.title("📊 Monitor de Riesgo FPD")
tabs = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "💡 Insights Estratégicos", "📥 Exportar"])

# --- TAB 1: MONITOR FPD ---
with tabs[0]:
    if not df_fpd.empty:
        df_t = df_fpd.groupby('cosecha_id').agg({'id_credito':'count', 'fpd_num':'sum', 'np_num':'sum'}).reset_index()
        df_t['%FPD'] = (df_t['fpd_num'] * 100 / df_t['id_credito'])
        df_t['np_rate'] = (df_t['np_num'] * 100 / df_t['id_credito'])
        ult = df_t.iloc[-1]; ant = df_t.iloc[-2] if len(df_t) > 1 else ult
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Cosecha Actual", ult['cosecha_id'], f"Anterior: {ant['cosecha_id']}", delta_color="off")
        k2.metric("Créditos", f"{int(ult['id_credito']):,}", f"{int(ult['id_credito'] - ant['id_credito']):+,} vs mes ant")
        k3.metric("Tasa FPD", f"{ult['%FPD']:.2f}%", f"{ult['%FPD'] - ant['%FPD']:.2f}% vs mes ant", delta_color="inverse")
        k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%", f"{ult['np_rate'] - ant['np_rate']:.2f}% vs mes ant", delta_color="inverse")
        st.divider()

        st.subheader("1. Tendencia Global (FPD)")
        fig1 = px.line(df_t, x='cosecha_id', y='%FPD', markers=True, text=df_t['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig1.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450)
        st.plotly_chart(fig1, use_container_width=True)

        st.subheader("2. FPD por Origen")
        df_o = df_fpd.groupby(['cosecha_id', 'origen2']).agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_o['%FPD'] = (df_o['fpd_num'] * 100 / df_o['id_credito'])
        fig2 = px.line(df_o, x='cosecha_id', y='%FPD', color='origen2', markers=True, text=df_o['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig2.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("3. Comparativo Anual (Mes a Mes)")
        df_y = df_fpd.groupby(['anio', 'mes']).agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_y['%FPD'] = (df_y['fpd_num'] * 100 / df_y['id_credito'])
        fig3 = px.line(df_y[df_y['anio'].isin([2023, 2024, 2025])], x='mes', y='%FPD', color=df_y['anio'].astype(str), markers=True, text=df_y['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig3.update_traces(textposition="top center").update_layout(xaxis=dict(ticktext=list(MESES_NOMBRE.values()), tickvals=list(MESES_NOMBRE.keys())), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("4. Histórico Indicadores (Últimas 24 Cosechas)")
        df_t_24 = df_t.tail(24)
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(x=df_t_24['cosecha_id'], y=df_t_24['%FPD'], name='% FPD', mode='lines+markers+text', text=df_t_24['%FPD'].apply(lambda x: f'{x:.1f}%'), textposition="top center"))
        fig4.add_trace(go.Scatter(x=df_t_24['cosecha_id'], y=df_t_24['np_rate'], name='% NP', mode='lines+markers+text', text=df_t_24['np_rate'].apply(lambda x: f'{x:.1f}%'), textposition="bottom center", line=dict(dash='dash')))
        fig4.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig4, use_container_width=True)

        # Rankings
        df_r_c = df_fpd[df_fpd['cosecha_id'] == ult_c_id].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd_num'] * 100 / df_r_c['id_credito'])
        df_r_p = df_fpd[df_fpd['cosecha_id'] == ant_c_id].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd_num'] * 100 / df_r_p['id_credito'])
        df_rf = pd.merge(df_r_c, df_r_p[['sucursal', 'id_credito', 'rate_ant']], on='sucursal', how='left', suffixes=('', '_ant'))
        
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {ult_c_id}")
        cr1, cr2 = st.columns(2)
        conf_rank = {"sucursal": "Sucursal", "id_credito": "Créditos Act", "id_credito_ant": "Créditos Ant", "fpd_num": "Casos FPD", "rate": st.column_config.NumberColumn("%FPD Act", format="%.2f%%"), "rate_ant": st.column_config.NumberColumn("%FPD Ant", format="%.2f%%")}
        cr1.markdown("**🔴 Top 10 Riesgo**"); cr1.dataframe(df_rf.sort_values('rate', ascending=False).head(10), column_config=conf_rank, hide_index=True, use_container_width=True)
        cr2.markdown("**🟢 Bottom 10 Riesgo**"); cr2.dataframe(df_rf.sort_values('rate', ascending=True).head(10), column_config=conf_rank, hide_index=True, use_container_width=True)
    else:
        st.info("No hay datos para mostrar con los filtros seleccionados.")

# --- TAB 2: RESUMEN EJECUTIVO ---
with tabs[1]:
    if not df_fpd.empty:
        st.header("💼 Resumen Ejecutivo Gerencial")
        def render_exec_block(field, dim_label):
            df_e_raw = get_executive_data(field)
            if df_e_raw.empty: return
            
            df_e = df_e_raw[df_e_raw['cosecha_id'] < max_c_real].copy()
            if not df_e.empty:
                lista_c_e = sorted(df_e['cosecha_id'].unique())
                u_e = lista_c_e[-1]; a_e = lista_c_e[-2] if len(lista_c_e) > 1 else u_e
                m_u = MESES_NOMBRE.get(u_e[-2:]); m_a = MESES_NOMBRE.get(a_e[-2:])
                df_u = df_e[df_e['cosecha_id'] == u_e].sort_values('fpd_rate')
                df_a = df_e[df_e['cosecha_id'] == a_e].sort_values('fpd_rate')
                
                c1, c2 = st.columns(2)
                c1.success(f"**{dim_label} Destacada:** {df_u.iloc[0]['dimension']} ({df_u.iloc[0]['fpd_rate']:.2f}% FPD)")
                c2.error(f"**{dim_label} Riesgosa:** {df_u.iloc[-1]['dimension']} ({df_u.iloc[-1]['fpd_rate']:.2f}% FPD)")
                
                df_tab = pd.merge(df_u[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']], df_a[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']].rename(columns={'total_vol':'vol_ant','fpd_si':'fpd_ant','fpd_rate':'rate_ant'}), on='dimension', how='left')
                st.dataframe(df_tab.style.background_gradient(subset=['fpd_rate','rate_ant'], cmap='YlOrRd').format({'fpd_rate':'{:.2f}%','rate_ant':'{:.2f}%'}), use_container_width=True, hide_index=True)
                st.divider()
        
        render_exec_block('unidad_regional', 'Regional')
        render_exec_block('producto_agrupado', 'Producto')

# --- TAB 3: INSIGHTS ESTRATÉGICOS ---
with tabs[2]:
    if not df_fpd.empty:
        st.header("💡 Insights Estratégicos")
        st.subheader("📍 Tendencia de Riesgo Regional (6 Meses)")
        u6 = lista_cosechas[-6:]
        df_h = df_fpd[df_fpd['cosecha_id'].isin(u6)].groupby(['unidad_regional','cosecha_id']).agg({'fpd_num':'sum','id_credito':'count'}).reset_index()
        df_h['%FPD'] = (df_h['fpd_num']*100/df_h['id_credito'])
        pivot_h = df_h.pivot(index='unidad_regional', columns='cosecha_id', values='%FPD').sort_values(by=u6[-1], ascending=True)
        st.dataframe(pivot_h.style.background_gradient(cmap='RdYlGn_r').format("{:.2f}%"), use_container_width=True)
        
        st.subheader(f"🏢 Pareto de Sucursales (Casos FPD {mes_u_nombre})")
        df_p = df_fpd[df_fpd['cosecha_id'] == ult_c_id].groupby('sucursal').agg({'fpd_num':'sum'}).reset_index().sort_values('fpd_num', ascending=False)
        fig_p = px.bar(df_p.head(15), x='sucursal', y='fpd_num', text='fpd_num', color_discrete_sequence=['#C0392B'])
        st.plotly_chart(fig_p, use_container_width=True)

# --- TAB 4: EXPORTAR ---
with tabs[3]:
    if not df_main.empty:
        st.header("📥 Exportar Detalle FPD")
        lista_export = sorted(df_main['cosecha_id'].unique(), reverse=True)[:2] 
        idx_defecto = 1 if len(lista_export) > 1 else 0
        
        cosecha_export = st.selectbox("Selecciona cosecha:", options=lista_export, index=idx_defecto)
        df_exp = df_main[(df_main['cosecha_id'] == cosecha_export) & (df_main['fpd2'] == 'FPD')].copy()
        
        st.subheader(f"Registros encontrados: {len(df_exp)}")
        st.dataframe(df_exp.head(100), use_container_width=True)
        st.download_button(label="💾 Descargar CSV", data=df_exp.to_csv(index=False).encode('utf-8'), file_name=f'fpd_{cosecha_export}.csv', mime='text/csv')