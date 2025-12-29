import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Intentar importar duckdb
try:
    import duckdb
except ImportError:
    st.error("Falta la librería 'duckdb'. Instálala ejecutando: pip install duckdb")
    st.stop()

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="FPD Daily - Dashboard Pro", layout="wide", page_icon="📊")

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
    query = f"""
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            CASE WHEN NP = 'NP' THEN 1 ELSE 0 END as np_num,
            id_credito, origen2, monto_otorgado,
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
    FROM filtrado WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH) AND fecha_dt IS NOT NULL
    """
    return duckdb.query(query).to_df()

@st.cache_data
def get_executive_data(field):
    query = f"""
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito, COALESCE({field}, 'N/A') as dimension,
            producto_agrupado, sucursal
        FROM 'fpd_gemini.parquet'
        WHERE UPPER(producto_agrupado) NOT LIKE '%NOMINA%'
          AND sucursal != '999.EMPRESA NOMINA COLABORADORES'
    )
    SELECT strftime(fecha_dt, '%Y%m') as cosecha_id, RIGHT(strftime(fecha_dt, '%Y%m'), 2) as mes_id,
           dimension, COUNT(id_credito) as total, SUM(fpd2_num) as fpd2_si,
           (SUM(fpd2_num) * 100.0 / COUNT(id_credito)) as fpd2_rate
    FROM base WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH) AND fecha_dt IS NOT NULL
    GROUP BY ALL
    """
    return duckdb.query(query).to_df()

# --- 3. SIDEBAR ---
st.sidebar.header("🎯 Filtros Dashboard")
opt = get_filter_universes()
sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))

st.title("📊 Monitor de Riesgo Crediticio")
tabs = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "💡 Insights Estratégicos", "📋 Datos"])

df_main = get_main_data(sel_reg, sel_suc, sel_prod, sel_tip)

# --- TAB 1: MONITOR FPD (COMPLETA) ---
with tabs[0]:
    if not df_main.empty:
        df_t = df_main.groupby('cosecha_id').agg({'id_credito':'count', 'fpd2_num':'sum', 'np_num':'sum'}).reset_index()
        df_t['rate'] = (df_t['fpd2_num'] * 100 / df_t['id_credito'])
        df_t['np_rate'] = (df_t['np_num'] * 100 / df_t['id_credito'])
        ult = df_t.iloc[-1]
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Cosecha Actual", ult['cosecha_id'])
        k2.metric("Créditos", f"{int(ult['id_credito']):,}")
        k3.metric("Tasa FPD2", f"{ult['rate']:.2f}%")
        k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%")
        st.divider()
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Tendencia Global (FPD2)")
            fig1 = px.line(df_t, x='cosecha_id', y='rate', markers=True, text=df_t['rate'].apply(lambda x: f'{x:.1f}%'))
            fig1.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=350)
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            st.subheader("FPD2 por Origen")
            df_o = df_main.groupby(['cosecha_id', 'origen2']).agg({'id_credito':'count', 'fpd2_num':'sum'}).reset_index()
            df_o['rate'] = (df_o['fpd2_num'] * 100 / df_o['id_credito'])
            fig2 = px.line(df_o, x='cosecha_id', y='rate', color='origen2', markers=True)
            fig2.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig2, use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Comparativa Interanual")
            df_y = df_main.groupby(['anio', 'mes']).agg({'id_credito':'count', 'fpd2_num':'sum'}).reset_index()
            df_y['rate'] = (df_y['fpd2_num'] * 100 / df_y['id_credito'])
            fig3 = px.line(df_y[df_y['anio'].isin([2023, 2024, 2025])], x='mes', y='rate', color=df_y['anio'].astype(str), markers=True)
            fig3.update_layout(xaxis=dict(ticktext=list(MESES_NOMBRE.values()), tickvals=list(MESES_NOMBRE.keys())), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig3, use_container_width=True)
        with c4:
            st.subheader("FPD2 vs NP")
            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=df_t['cosecha_id'], y=df_t['rate'], name='% FPD2'))
            fig4.add_trace(go.Scatter(x=df_t['cosecha_id'], y=df_t['np_rate'], name='% NP', line=dict(dash='dash')))
            fig4.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig4, use_container_width=True)

        st.subheader("Tipo Cliente (Sin Formers)")
        df_tc = df_main[df_main['tipo_cliente'] != 'Formers'].groupby(['cosecha_id', 'tipo_cliente']).agg({'id_credito':'count', 'fpd2_num':'sum'}).reset_index()
        df_tc['rate'] = (df_tc['fpd2_num'] * 100 / df_tc['id_credito'])
        fig5 = px.line(df_tc, x='cosecha_id', y='rate', color='tipo_cliente', markers=True)
        fig5.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=400, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig5, use_container_width=True)

        st.divider()
        cosechas = sorted(df_main['cosecha_id'].unique())
        ant_c = cosechas[-2] if len(cosechas) > 1 else cosechas[-1]
        df_r_c = df_main[df_main['cosecha_id'] == cosechas[-1]].groupby('sucursal').agg({'id_credito':'count', 'fpd2_num':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd2_num'] * 100 / df_r_c['id_credito'])
        df_r_p = df_main[df_main['cosecha_id'] == ant_c].groupby('sucursal').agg({'fpd2_num':'sum', 'id_credito':'count'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd2_num'] * 100 / df_r_p['id_credito'])
        df_rf = pd.merge(df_r_c, df_r_p[['sucursal', 'rate_ant']], on='sucursal', how='left')
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {cosechas[-1]}")
        cr1, cr2 = st.columns(2)
        conf = {"sucursal":"Sucursal", "id_credito":"Créditos", "rate":st.column_config.NumberColumn("% FPD", format="%.2f%%"), "rate_ant":st.column_config.NumberColumn("% Ant", format="%.2f%%")}
        cr1.dataframe(df_rf.sort_values('rate', ascending=False).head(10), column_config=conf, hide_index=True, use_container_width=True)
        cr2.dataframe(df_rf.sort_values('rate', ascending=True).head(10), column_config=conf, hide_index=True, use_container_width=True)

# --- TAB 2: RESUMEN EJECUTIVO ---
with tabs[1]:
    st.header("💼 Resumen Ejecutivo Gerencial")
    def render_exec_block(field, title, dim_label):
        df_e = get_executive_data(field)
        if not df_e.empty:
            lista_c = sorted(df_e['cosecha_id'].unique())
            ult_c = lista_c[-1]; ant_c = lista_c[-2] if len(lista_c) > 1 else ult_c
            mes_u = MESES_NOMBRE.get(ult_c[-2:]); mes_a = MESES_NOMBRE.get(ant_c[-2:])
            df_u = df_e[df_e['cosecha_id'] == ult_c].sort_values('fpd2_rate')
            df_a = df_e[df_e['cosecha_id'] == ant_c].sort_values('fpd2_rate')
            c1, c2 = st.columns(2)
            c1.success(f"**{dim_label} Destacada:** La mejor es **{df_u.iloc[0]['dimension']}** con un **{df_u.iloc[0]['fpd2_rate']:.2f}%** en **{mes_u}**, mientras que en **{mes_a}** fue **{df_a.iloc[0]['dimension']}** con un **{df_a.iloc[0]['fpd2_rate']:.2f}%**.")
            c2.error(f"**{dim_label} Riesgosa:** La de mayor riesgo es **{df_u.iloc[-1]['dimension']}** con un **{df_u.iloc[-1]['fpd2_rate']:.2f}%** en **{mes_u}**, mientras que en **{mes_a}** fue **{df_a.iloc[-1]['dimension']}** con un **{df_a.iloc[-1]['fpd2_rate']:.2f}%**.")
            df_tab = pd.merge(df_u[['dimension', 'fpd2_si', 'fpd2_rate']], df_a[['dimension', 'fpd2_si', 'fpd2_rate']].rename(columns={'fpd2_si':'fpd2_si_ant','fpd2_rate':'rate_ant'}), on='dimension', how='left')
            st.dataframe(df_tab.style.background_gradient(subset=['fpd2_rate','rate_ant'], cmap='YlOrRd').format({'fpd2_rate':'{:.2f}%','rate_ant':'{:.2f}%','fpd2_si':'{:,.0f}','fpd2_si_ant':'{:,.0f}'}),
                         use_container_width=True, hide_index=True, column_config={"dimension":dim_label, "fpd2_si":f"FPD Casos {mes_u.capitalize()}", "fpd2_si_ant":f"FPD Casos {mes_a.capitalize()}", "fpd2_rate":f"% {mes_u.capitalize()}", "rate_ant":f"% {mes_a.capitalize()}"})
            st.divider()
    render_exec_block('unidad_regional', "Regional", "Regional")
    render_exec_block('producto_agrupado', "Producto", "Producto")
    render_exec_block('sucursal', "Sucursal", "Sucursal")

# --- TAB 3: INSIGHTS ESTRATÉGICOS (AJUSTADO) ---
with tabs[2]:
    if not df_main.empty:
        st.header("💡 Insights Estratégicos")
        
        # 1. Heatmap (Verde = Bajo Riesgo, Rojo = Alto Riesgo)
        st.subheader("📍 Tendencia de Riesgo Regional (6 Meses)")
        u6 = sorted(df_main['cosecha_id'].unique())[-6:]
        df_h = df_main[df_main['cosecha_id'].isin(u6)].groupby(['unidad_regional','cosecha_id']).agg({'fpd2_num':'sum','id_credito':'count'}).reset_index()
        df_h['rate'] = (df_h['fpd2_num']*100/df_h['id_credito'])
        # Aplicamos la escala RdYlGn_r (Red-Yellow-Green Reversed) para que 0 sea Verde
        st.dataframe(df_h.pivot(index='unidad_regional', columns='cosecha_id', values='rate').style.background_gradient(cmap='RdYlGn_r').format("{:.2f}%"), use_container_width=True)
        
        st.divider()

        # 2. Pareto Sucursales (Solo Barras)
        st.subheader("🏢 Pareto de Sucursales (Volumen de Casos FPD)")
        df_p = df_main.groupby('sucursal').agg({'fpd2_num':'sum'}).reset_index().sort_values('fpd2_num', ascending=False)
        fig_p = px.bar(df_p.head(20), x='sucursal', y='fpd2_num', 
                       labels={'sucursal': 'Sucursal', 'fpd2_num': 'Casos FPD'},
                       title="Top 20 Sucursales con más casos FPD")
        fig_p.update_layout(plot_bgcolor='white', xaxis_tickangle=-45)
        st.plotly_chart(fig_p, use_container_width=True)

        st.divider()

        # 3. Combo Chart: Volumen y Tasa FPD por Rango
        st.subheader("💰 Volumen y Calidad por Rango de Monto")
        bins = [0, 3000, 5000, 8000, 12000, 20000, float('inf')]
        labels = ['$0-$3k', '$3k-$5k', '$5k-$8k', '$8k-$12k', '$12k-$20k', '>$20k']
        df_main['rango'] = pd.cut(df_main['monto_otorgado'], bins=bins, labels=labels, include_lowest=True)
        df_s = df_main.groupby('rango', observed=True).agg({'id_credito':'count', 'fpd2_num':'sum'}).reset_index()
        df_s['rate'] = (df_s['fpd2_num']*100/df_s['id_credito'])
        
        fig_combo = make_subplots(specs=[[{"secondary_y": True}]])
        fig_combo.add_trace(go.Bar(x=df_s['rango'], y=df_s['id_credito'], name="Créditos Colocados", marker_color='#AED6F1'), secondary_y=False)
        fig_combo.add_trace(go.Scatter(x=df_s['rango'], y=df_s['rate'], name="% Tasa FPD2", mode='lines+markers+text', text=df_s['rate'].apply(lambda x: f'{x:.1f}%'), line=dict(color='#C0392B', width=3)), secondary_y=True)
        fig_combo.update_layout(plot_bgcolor='white', legend=LEGEND_BOTTOM, height=500)
        fig_combo.update_yaxes(title_text="Cantidad de Créditos", secondary_y=False)
        fig_combo.update_yaxes(title_text="% Tasa FPD2", secondary_y=True, ticksuffix="%", range=[0, df_s['rate'].max()*1.5])
        st.plotly_chart(fig_combo, use_container_width=True)

with tabs[3]: st.info("Pestaña de Datos vacía.")