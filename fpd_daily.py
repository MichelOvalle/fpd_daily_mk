import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
    import duckdb
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
    import duckdb
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
    import duckdb
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
sel_reg = st.sidebar.multiselect("📍 Regional", options=sorted(opt['unidad_regional'].unique()))
suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique()) if sel_reg else sorted(opt['sucursal'].unique())
sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo Cliente", options=sorted(opt['tipo_cliente'].unique()))

df_main = get_main_data(sel_reg, sel_suc, sel_prod, sel_tip)

# --- 4. LÓGICA DE FILTRADO GLOBAL (IGNORAR MÁXIMO) ---
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

        st.subheader("5. Comportamiento %FPD por tipo de cliente")
        u24 = sorted(df_fpd['cosecha_id'].unique())[-24:]
        df_tc = df_fpd[(df_fpd['tipo_cliente'] != 'Formers') & (df_fpd['cosecha_id'].isin(u24))].groupby(['cosecha_id', 'tipo_cliente']).agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_tc['%FPD'] = (df_tc['fpd_num'] * 100 / df_tc['id_credito'])
        fig5 = px.line(df_tc, x='cosecha_id', y='%FPD', color='tipo_cliente', markers=True, text=df_tc['%FPD'].apply(lambda x: f'{x:.1f}%'))
        fig5.update_traces(textposition="top center").update_layout(xaxis=dict(type='category'), plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig5, use_container_width=True)

        st.divider()
        # Rankings Sucursales
        df_r_c = df_fpd[df_fpd['cosecha_id'] == ult_c_id].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_c['rate'] = (df_r_c['fpd_num'] * 100 / df_r_c['id_credito'])
        df_r_p = df_fpd[df_fpd['cosecha_id'] == ant_c_id].groupby('sucursal').agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_r_p['rate_ant'] = (df_r_p['fpd_num'] * 100 / df_r_p['id_credito'])
        df_rf = pd.merge(df_r_c, df_r_p[['sucursal', 'id_credito', 'rate_ant']], on='sucursal', how='left', suffixes=('', '_ant'))
        
        st.subheader(f"🏆 Rankings Sucursales - Cosecha {ult_c_id}")
        cr1, cr2 = st.columns(2)
        conf_rank = {"sucursal": "Sucursal", "id_credito": f"Créditos {ult_c_id}", "id_credito_ant": f"Créditos {ant_c_id}", "fpd_num": st.column_config.NumberColumn(f"Casos FPD {ult_c_id}", format="%d"), "rate": st.column_config.NumberColumn(f"%FPD {ult_c_id}", format="%.2f%%"), "rate_ant": st.column_config.NumberColumn(f"%FPD {ant_c_id}", format="%.2f%%")}
        cr1.markdown("**🔴 Top 10 Riesgo**"); cr1.dataframe(df_rf.sort_values('rate', ascending=False).head(10), column_config=conf_rank, hide_index=True, use_container_width=True)
        cr2.markdown("**🟢 Bottom 10 Riesgo**"); cr2.dataframe(df_rf.sort_values('rate', ascending=True).head(10), column_config=conf_rank, hide_index=True, use_container_width=True)

# --- TAB 2: RESUMEN EJECUTIVO ---
with tabs[1]:
    if not df_fpd.empty:
        st.header("💼 Resumen Ejecutivo Gerencial")
        def render_exec_block(field, dim_label):
            df_e_raw = get_executive_data(field)
            df_e = df_e_raw[df_e_raw['cosecha_id'] < max_c_real].copy()
            
            if not df_e.empty:
                lista_c_e = sorted(df_e['cosecha_id'].unique())
                u_e = lista_c_e[-1]; a_e = lista_c_e[-2] if len(lista_c_e) > 1 else u_e
                m_u = MESES_NOMBRE.get(u_e[-2:]); m_a = MESES_NOMBRE.get(a_e[-2:])
                df_u = df_e[df_e['cosecha_id'] == u_e].sort_values('fpd_rate')
                df_a = df_e[df_e['cosecha_id'] == a_e].sort_values('fpd_rate')
                
                c1, c2 = st.columns(2)
                c1.success(f"**{dim_label} Destacada:** La mejor es **{df_u.iloc[0]['dimension']}** con un **{df_u.iloc[0]['fpd_rate']:.2f}%** en **{m_u}**, mientras que en **{m_a}** fue **{df_a.iloc[0]['dimension']}** con un **{df_a.iloc[0]['fpd_rate']:.2f}%**.")
                c2.error(f"**{dim_label} Riesgosa:** La de mayor riesgo es **{df_u.iloc[-1]['dimension']}** con un **{df_u.iloc[-1]['fpd_rate']:.2f}%** en **{m_u}**, mientras que en **{m_a}** fue **{df_a.iloc[-1]['dimension']}** con un **{df_a.iloc[-1]['fpd_rate']:.2f}%**.")
                
                df_tab = pd.merge(df_u[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']], df_a[['dimension', 'total_vol', 'fpd_si', 'fpd_rate']].rename(columns={'total_vol':'vol_ant','fpd_si':'fpd_ant','fpd_rate':'rate_ant'}), on='dimension', how='left')
                st.dataframe(df_tab.style.background_gradient(subset=['fpd_rate','rate_ant'], cmap='YlOrRd').format({'fpd_rate':'{:.2f}%','rate_ant':'{:.2f}%','fpd_si':'{:,.0f}','fpd_ant':'{:,.0f}','total_vol':'{:,.0f}','vol_ant':'{:,.0f}'}),
                             use_container_width=True, hide_index=True, column_config={"dimension":dim_label, "total_vol":f"Créditos {m_u.capitalize()}", "vol_ant":f"Créditos {m_a.capitalize()}", "fpd_si":f"Casos FPD {m_u.capitalize()}", "fpd_ant":f"Casos FPD {m_a.capitalize()}", "fpd_rate":f"%FPD {m_u.capitalize()}", "rate_ant":f"%FPD {m_a.capitalize()}"})
                st.divider()
        render_exec_block('unidad_regional', 'Regional'); render_exec_block('producto_agrupado', 'Producto'); render_exec_block('sucursal', 'Sucursal')

# --- TAB 3: INSIGHTS ESTRATÉGICOS ---
with tabs[2]:
    if not df_fpd.empty:
        st.header("💡 Insights Estratégicos")
        st.subheader("📍 Tendencia de Riesgo Regional (6 Meses)")
        u6 = lista_cosechas[-6:]
        df_h = df_fpd[df_fpd['cosecha_id'].isin(u6) & ~df_fpd['producto_agrupado'].str.upper().str.contains('NOMINA')].groupby(['unidad_regional','cosecha_id']).agg({'fpd_num':'sum','id_credito':'count'}).reset_index()
        df_h['%FPD'] = (df_h['fpd_num']*100/df_h['id_credito'])
        pivot_h = df_h.pivot(index='unidad_regional', columns='cosecha_id', values='%FPD').sort_values(by=u6[-1], ascending=True)
        st.dataframe(pivot_h.style.background_gradient(cmap='RdYlGn_r').format("{:.2f}%"), use_container_width=True)
        
        st.subheader(f"🏢 Pareto de Sucursales (Casos FPD {mes_u_nombre})")
        df_p = df_fpd[df_fpd['cosecha_id'] == ult_c_id].groupby('sucursal').agg({'fpd_num':'sum'}).reset_index().sort_values('fpd_num', ascending=False)
        fig_p = px.bar(df_p.head(20), x='sucursal', y='fpd_num', text='fpd_num', color_discrete_sequence=['#C0392B'])
        fig_p.update_traces(textposition='outside').update_layout(plot_bgcolor='white', xaxis_tickangle=-45, yaxis_title="Casos FPD")
        st.plotly_chart(fig_p, use_container_width=True)
        
        st.subheader(f"💰 Volumen y Calidad: Comparativa {mes_u_nombre} vs {mes_a_nombre}")
        bins = [0, 3000, 5000, 8000, 12000, 20000, float('inf')]
        labels = ['$0-$3k', '$3k-$5k', '$5k-$8k', '$8k-$12k', '$12k-$20k', '>$20k']
        df_comp = df_fpd[df_fpd['cosecha_id'].isin([ult_c_id, ant_c_id])].copy()
        df_comp['rango'] = pd.cut(df_comp['monto_otorgado'], bins=bins, labels=labels, include_lowest=True)
        df_s = df_comp.groupby(['cosecha_id', 'rango'], observed=True).agg({'id_credito':'count', 'fpd_num':'sum'}).reset_index()
        df_s['%FPD'] = (df_s['fpd_num']*100/df_s['id_credito'])
        df_u_m = df_s[df_s['cosecha_id'] == ult_c_id]; df_a_m = df_s[df_s['cosecha_id'] == ant_c_id]
        
        fig_combo = make_subplots(specs=[[{"secondary_y": True}]])
        fig_combo.add_trace(go.Bar(x=df_u_m['rango'], y=df_u_m['id_credito'], name=f"Créditos {mes_u_nombre}", marker_color='#2E86C1', text=df_u_m['id_credito'], textposition='auto'), secondary_y=False)
        fig_combo.add_trace(go.Bar(x=df_a_m['rango'], y=df_a_m['id_credito'], name=f"Créditos {mes_a_nombre}", marker_color='#AED6F1', text=df_a_m['id_credito'], textposition='auto'), secondary_y=False)
        fig_combo.add_trace(go.Scatter(x=df_u_m['rango'], y=df_u_m['%FPD'], name=f"%FPD {mes_u_nombre}", mode='lines+markers+text', text=df_u_m['%FPD'].apply(lambda x: f'{x:.1f}%'), textposition='top center', line=dict(color='#C0392B', width=4)), secondary_y=True)
        fig_combo.add_trace(go.Scatter(x=df_a_m['rango'], y=df_a_m['%FPD'], name=f"%FPD {mes_a_nombre}", mode='lines+markers', line=dict(color='#E67E22', width=2, dash='dash')), secondary_y=True)
        fig_combo.update_layout(plot_bgcolor='white', barmode='group', height=550, legend=LEGEND_BOTTOM)
        st.plotly_chart(fig_combo, use_container_width=True)

# --- TAB 4: EXPORTAR (SOLO ÚLTIMAS 2 COSECHAS) ---
with tabs[3]:
    if not df_main.empty:
        st.header("📥 Exportar Detalle FPD")
        
        # Obtenemos solo las últimas dos del archivo completo
        lista_export = sorted(df_main['cosecha_id'].unique(), reverse=True)[:2] 
        
        # El índice 1 en lista_export (que es descendente) es la penúltima
        # Ponemos un pequeño chequeo por si solo hay una cosecha en el archivo
        idx_defecto = 1 if len(lista_export) > 1 else 0
        
        cosecha_export = st.selectbox(
            "Selecciona la cosecha a exportar:", 
            options=lista_export, 
            index=idx_defecto # Selecciona por default la penúltima
        )
        
        df_exp = df_main[(df_main['cosecha_id'] == cosecha_export) & (df_main['fpd2'] == 'FPD')].copy()
        cols = ['id_credito', 'id_segmento', 'id_producto', 'producto_agrupado', 'origen2', 'cosecha_id', 'monto_otorgado', 'cuota', 'sucursal']
        df_exp = df_exp[cols].rename(columns={'cosecha_id':'cosecha'})
        
        st.subheader(f"Casos FPD encontrados en {cosecha_export}: {len(df_exp)}")
        st.dataframe(df_exp, use_container_width=True, hide_index=True)
        st.download_button(
            label=f"💾 Descargar CSV {cosecha_export}", 
            data=df_exp.to_csv(index=False).encode('utf-8'), 
            file_name=f'detalle_fpd_{cosecha_export}.csv', 
            mime='text/csv'
        )