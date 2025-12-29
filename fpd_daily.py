import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb

# --- 1. CONFIGURACIÓN Y ESTILOS ---
st.set_page_config(page_title="Monitor FPD - Dashboard Pro", layout="wide", page_icon="📊")

MESES_NOMBRE = {
    '01': 'enero', '02': 'febrero', '03': 'marzo', '04': 'abril', '05': 'mayo', '06': 'junio',
    '07': 'julio', '08': 'agosto', '09': 'septiembre', '10': 'octubre', '11': 'noviembre', '12': 'diciembre'
}

LEGEND_BOTTOM = dict(orientation="h", yanchor="top", y=-0.3, xanchor="center", x=0.5)

# --- 2. FUNCIONES DE DATOS ---

@st.cache_data
def get_raw_data():
    """Carga inicial para identificar límites de cosecha"""
    con = duckdb.connect()
    # Obtenemos la cosecha máxima absoluta para poder ignorarla
    max_c = con.execute("SELECT MAX(strftime(TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE), '%Y%m')) FROM 'fpd_gemini.parquet'").fetchone()[0]
    return max_c

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
def get_main_data(regionales, sucursales, productos, tipos, cosecha_a_ignorar):
    def to_sql_list(lista):
        return "'" + "','".join(lista) + "'"
    
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
        SELECT *, strftime(fecha_dt, '%Y%m') as cosecha_id 
        FROM base 
        WHERE fecha_dt IS NOT NULL
        {"AND unidad_regional IN (" + to_sql_list(regionales) + ")" if regionales else ""}
        {"AND sucursal IN (" + to_sql_list(sucursales) + ")" if sucursales else ""}
        {"AND producto_agrupado IN (" + to_sql_list(productos) + ")" if productos else ""}
        {"AND tipo_cliente IN (" + to_sql_list(tipos) + ")" if tipos else ""}
    )
    SELECT *, EXTRACT(YEAR FROM fecha_dt) as anio, strftime(fecha_dt, '%m') as mes
    FROM filtrado 
    WHERE cosecha_id < '{cosecha_a_ignorar}'
    """
    return duckdb.query(query).to_df()

@st.cache_data
def get_executive_data(field, cosecha_a_ignorar):
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
    FROM base 
    WHERE fecha_dt IS NOT NULL 
      AND strftime(fecha_dt, '%Y%m') < '{cosecha_a_ignorar}'
    GROUP BY ALL ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- 3. LOGICA DE FILTROS ---
max_cosecha_parquet = get_raw_data()
opt = get_filter_universes()

st.sidebar.header("🎯 Filtros Dashboard")
sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))

# Obtenemos datos ignorando la máxima
df_main = get_main_data(sel_reg, sel_suc, sel_prod, sel_tip, max_cosecha_parquet)

st.title("📊 Monitor de Riesgo FPD")
st.info(f"💡 Nota: Se está ignorando la cosecha **{max_cosecha_parquet}** por ser la más reciente (en proceso de maduración).")

tabs = st.tabs(["📈 Monitor FPD", "💼 Resumen Ejecutivo", "💡 Insights Estratégicos", "📥 Exportar"])

# --- TAB 1: MONITOR FPD ---
with tabs[0]:
    if not df_main.empty:
        df_t = df_main.groupby('cosecha_id').agg({'id_credito':'count', 'fpd_num':'sum', 'np_num':'sum'}).reset_index()
        df_t['%FPD'] = (df_t['fpd_num'] * 100 / df_t['id_credito'])
        df_t['np_rate'] = (df_t['np_num'] * 100 / df_t['id_credito'])
        ult = df_t.iloc[-1]; ant = df_t.iloc[-2] if len(df_t) > 1 else ult
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Cosecha Actual", ult['cosecha_id'], f"Ant: {ant['cosecha_id']}")
        k2.metric("Créditos", f"{int(ult['id_credito']):,}", f"{int(ult['id_credito'] - ant['id_credito']):+,}")
        k3.metric("Tasa FPD", f"{ult['%FPD']:.2f}%", f"{ult['%FPD'] - ant['%FPD']:.2f}%", delta_color="inverse")
        k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%", f"{ult['np_rate'] - ant['np_rate']:.2f}%", delta_color="inverse")
        st.divider()

        # Gráficas Verticales con Etiquetas
        for titulo, col_y in [("1. Tendencia Global (FPD)", "%FPD")]:
            st.subheader(titulo)
            fig = px.line(df_t, x='cosecha_id', y=col_y, markers=True, text=df_t[col_y].apply(lambda x: f'{x:.1f}%'))
            fig.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450)
            st.plotly_chart(fig, use_container_width=True)
            st.divider()

        st.subheader("2. FPD por Origen")
        df_o = df_main.groupby(['cosecha_id', 'origen2']).agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_o['%FPD'] = (df_o['fpd_num'] * 100 / df_o['id_credito'])
        fig2 = px.line(df_o, x='cosecha_id', y='%FPD', color='origen2', markers=True, text=df_o['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig2.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig2, use_container_width=True)
        st.divider()

        st.subheader("3. Histórico Indicadores (Últimas 24 Cosechas)")
        df_t_24 = df_t.tail(24)
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(x=df_t_24['cosecha_id'], y=df_t_24['%FPD'], name='% FPD', mode='lines+markers+text', text=df_t_24['%FPD'].apply(lambda x: f'{x:.1f}%'), textposition="top center"))
        fig4.add_trace(go.Scatter(x=df_t_24['cosecha_id'], y=df_t_24['np_rate'], name='% NP', mode='lines+markers+text', text=df_t_24['np_rate'].apply(lambda x: f'{x:.1f}%'), textposition="bottom center", line=dict(dash='dash')))
        fig4.update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig4, use_container_width=True)

        st.divider()
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {ult['cosecha_id']}")
        df_r_c = df_main[df_main['cosecha_id'] == ult['cosecha_id']].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd_num'] * 100 / df_r_c['id_credito'])
        df_r_p = df_main[df_main['cosecha_id'] == ant['cosecha_id']].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd_num'] * 100 / df_r_p['id_credito'])
        df_rf = pd.merge(df_r_c, df_r_p[['sucursal', 'id_credito', 'rate_ant']], on='sucursal', how='left', suffixes=('', '_ant'))
        conf_rank = {"sucursal": "Sucursal", "id_credito": f"Créditos {ult['cosecha_id']}", "id_credito_ant": f"Créditos {ant['cosecha_id']}", "fpd_num": st.column_config.NumberColumn(f"Casos FPD {ult['cosecha_id']}", format="%d"), "rate": st.column_config.NumberColumn(f"%FPD {ult['cosecha_id']}", format="%.2f%%"), "rate_ant": st.column_config.NumberColumn(f"%FPD {ant['cosecha_id']}", format="%.2f%%")}
        st.dataframe(df_rf.sort_values('rate', ascending=False).head(10), column_config=conf_rank, hide_index=True, use_container_width=True)

# --- TAB 2: RESUMEN EJECUTIVO (INDEPENDIENTE E IGNORANDO MAX) ---
with tabs[1]:
    st.header("💼 Resumen Ejecutivo Gerencial")
    for f, d in [('unidad_regional', 'Regional'), ('producto_agrupado', 'Producto')]:
        df_e = get_executive_data(f, max_cosecha_parquet)
        if not df_e.empty:
            lc = sorted(df_e['cosecha_id'].unique()); u_c = lc[-1]; a_c = lc[-2] if len(lc)>1 else u_c
            m_u = MESES_NOMBRE.get(u_c[-2:]); m_a = MESES_NOMBRE.get(a_c[-2:])
            df_u = df_e[df_e['cosecha_id'] == u_c].sort_values('fpd_rate'); df_a = df_e[df_e['cosecha_id'] == a_c].sort_values('fpd_rate')
            c1, c2 = st.columns(2)
            c1.success(f"**{d} Destacada:** {df_u.iloc[0]['dimension']} ({df_u.iloc[0]['fpd_rate']:.2f}% en {m_u})")
            c2.error(f"**{d} Riesgosa:** {df_u.iloc[-1]['dimension']} ({df_u.iloc[-1]['fpd_rate']:.2f}% en {m_u})")
            df_tab = pd.merge(df_u[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']], df_a[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']].rename(columns={'total_vol':'vol_ant','fpd_si':'fpd_ant','fpd_rate':'rate_ant'}), on='dimension', how='left')
            st.dataframe(df_tab.style.background_gradient(subset=['fpd_rate','rate_ant'], cmap='YlOrRd').format({'fpd_rate':'{:.2f}%','rate_ant':'{:.2f}%','fpd_si':'{:,.0f}','fpd_ant':'{:,.0f}','total_vol':'{:,.0f}','vol_ant':'{:,.0f}'}), use_container_width=True, hide_index=True, column_config={"dimension":d, "total_vol":f"Créditos {m_u}", "vol_ant":f"Créditos {m_a}", "fpd_si":f"Casos FPD {m_u}", "fpd_ant":f"Casos FPD {m_a}", "fpd_rate":f"%FPD {m_u}", "rate_ant":f"%FPD {m_a}"})

# --- TAB 4: EXPORTAR (DINÁMICA - TÚ ELIGES) ---
with tabs[3]:
    st.header("📥 Exportar Detalle FPD")
    # En exportar dejamos que el usuario vea la última cosecha REAL si quiere
    cosecha_export = st.selectbox("Selecciona la cosecha a descargar:", options=[max_cosecha_parquet, df_main['cosecha_id'].max()], index=1)
    
    con_exp = duckdb.connect()
    df_exp = con_exp.execute(f"""
        SELECT id_credito, id_segmento, id_producto, producto_agrupado, origen2, 
               strftime(TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE), '%Y%m') as cosecha,
               monto_otorgado, cuota, sucursal
        FROM 'fpd_gemini.parquet'
        WHERE fpd2 = 'FPD' AND cosecha = '{cosecha_export}'
    """).df()
    
    st.subheader(f"Casos FPD encontrados en {cosecha_export}: {len(df_exp)}")
    st.dataframe(df_exp.head(10), use_container_width=True)
    st.download_button(label=f"💾 Descargar CSV {cosecha_export}", data=df_exp.to_csv(index=False).encode('utf-8'), file_name=f'detalle_fpd_{cosecha_export}.csv', mime='text/csv')