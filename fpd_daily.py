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
    # Función auxiliar para convertir listas a formato SQL
    def to_sql_list(lista):
        return "'" + "','".join(lista) + "'"

    # Lógica: Si la lista está vacía, no filtramos por ese campo (considera todos)
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
        WHERE (tipo_cliente IS NOT NULL) -- Base obligatoria
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
        origen2, tipo_cliente, sucursal,
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
st.sidebar.markdown("*(Si no seleccionas nada, se calculan todos los valores)*")

opt = get_filter_universes()

sel_reg = st.sidebar.multiselect("📍 Unidad Regional", options=sorted(opt['unidad_regional'].unique()))

# Sucursales dinámicas
if sel_reg:
    suc_disp = sorted(opt[opt['unidad_regional'].isin(sel_reg)]['sucursal'].unique())
else:
    suc_disp = sorted(opt['sucursal'].unique())

sel_suc = st.sidebar.multiselect("🏠 Sucursal", options=suc_disp)
sel_prod = st.sidebar.multiselect("📦 Producto", options=sorted(opt['producto_agrupado'].unique()))
sel_tip = st.sidebar.multiselect("👥 Tipo de Cliente", options=sorted(opt['tipo_cliente'].unique()))

# --- CUERPO PRINCIPAL ---
st.title("📊 FPD Daily: Dashboard de Riesgo")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Resumen General", "🍇 Análisis de Cosechas", "🏢 Por Sucursal", "📋 Detalle de Datos"])

with tab1:
    try:
        # Ejecutar consulta con filtros reactivos
        df_raw = get_filtered_data(sel_reg, sel_suc, sel_prod, sel_tip)
        
        if not df_raw.empty:
            # --- PROCESAMIENTO ---
            df_total = df_raw.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum', 'np_si':'sum'}).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
            df_total['np_rate'] = (df_total['np_si'] * 100.0 / df_total['total_casos'])
            
            df_origen = df_raw.groupby(['cosecha_id', 'origen2']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_origen['fpd2_rate'] = (df_origen['fpd2_si'] * 100.0 / df_origen['total_casos'])

            df_yoy = df_raw.groupby(['anio', 'mes']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_yoy['fpd2_rate'] = (df_yoy['fpd2_si'] * 100.0 / df_yoy['total_casos'])
            df_yoy = df_yoy[df_yoy['anio'].isin([2023, 2024, 2025])]

            df_tipo_graf = df_raw[df_raw['tipo_cliente'] != 'Formers'].groupby(['cosecha_id', 'tipo_cliente']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_tipo_graf['fpd2_rate'] = (df_tipo_graf['fpd2_si'] * 100.0 / df_tipo_graf['total_casos'])

            # KPIs
            ultima_cosecha = df_total['cosecha_id'].max()
            lista_cosechas = sorted(df_total['cosecha_id'].unique())
            cosecha_ant = lista_cosechas[-2] if len(lista_cosechas) > 1 else ultima_cosecha
            
            ult = df_total.iloc[-1]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Cosecha Actual", ultima_cosecha)
            k2.metric("Créditos", f"{int(ult['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ult['fpd2_rate']:.2f}%")
            k4.metric("Tasa NP", f"{ult['np_rate']:.2f}%")

            st.divider()

            # --- FILA 1 (50/50) ---
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Tendencia Global (FPD2)")
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['fpd2_rate'], mode='lines+markers+text',
                    text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'), textposition="top center",
                    line=dict(color='#1B4F72', width=4), fill='tozeroy', fillcolor='rgba(27, 79, 114, 0.1)', name='FPD2 Global'))
                fig1.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig1, use_container_width=True)

            with c2:
                st.subheader("FPD2 por Origen")
                fig2 = px.line(df_origen, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True, text=df_origen['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                               color_discrete_map={'fisico': '#2E86C1', 'digital': '#CB4335'})
                fig2.update_traces(textposition="top center")
                fig2.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig2, use_container_width=True)

            # --- FILA 2 (50/50) ---
            c3, c4 = st.columns(2)
            with c3:
                st.subheader("Comparativa Interanual (FPD2)")
                df_yoy['anio'] = df_yoy['anio'].astype(str)
                fig3 = px.line(df_yoy, x='mes', y='fpd2_rate', color='anio', markers=True, text=df_yoy['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                               color_discrete_map={'2023': '#BDC3C7', '2024': '#5499C7', '2025': '#1A5276'})
                fig3.update_traces(textposition="top center")
                fig3.update_layout(xaxis=dict(ticktext=['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'], tickvals=['01','02','03','04','05','06','07','08','09','10','11','12']),
                                   yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig3, use_container_width=True)

            with c4:
                st.subheader("Correlación: % FPD2 vs % NP")
                fig4 = go.Figure()
                fig4.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['fpd2_rate'], mode='lines+markers', name='% FPD2', line=dict(color='#1B4F72', width=3)))
                fig4.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['np_rate'], mode='lines+markers', name='% NP', line=dict(color='#D35400', width=3, dash='dash')))
                fig4.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=350, legend=LEGEND_BOTTOM, hovermode="x unified")
                st.plotly_chart(fig4, use_container_width=True)

            # --- FILA 3 (ANCHO COMPLETO) ---
            st.subheader("Tendencia FPD2 por Tipo de Cliente (Excluyendo Formers)")
            fig5 = px.line(df_tipo_graf, x='cosecha_id', y='fpd2_rate', color='tipo_cliente', markers=True,
                           text=df_tipo_graf['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                           color_discrete_map={'Nuevo': '#7D3C98', 'Renovacion': '#27AE60'})
            fig5.update_traces(textposition="top center", line=dict(width=4))
            fig5.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=400, legend=LEGEND_BOTTOM)
            st.plotly_chart(fig5, use_container_width=True)

            st.divider()

            # --- RANKINGS ---
            st.subheader(f"🏆 Rankings de Sucursales - Cosecha {ultima_cosecha}")
            # El ranking se recalcula con los datos ya filtrados por Sidebar
            df_suc_curr = df_raw[df_raw['cosecha_id'] == ultima_cosecha].groupby('sucursal').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_suc_curr['fpd2_rate'] = (df_suc_curr['fpd2_si'] * 100.0 / df_suc_curr['total_casos'])
            
            df_suc_prev = df_raw[df_raw['cosecha_id'] == cosecha_ant].groupby('sucursal').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_suc_prev['fpd2_rate_ant'] = (df_suc_prev['fpd2_si'] * 100.0 / df_suc_prev['total_casos'])
            
            df_rank = pd.merge(df_suc_curr, df_suc_prev[['sucursal', 'fpd2_rate_ant']], on='sucursal', how='left')

            if not df_rank.empty:
                col_t, col_b = st.columns(2)
                conf = {"sucursal": "Sucursal", "total_casos": "Créditos", 
                        "fpd2_rate": st.column_config.NumberColumn(f"% FPD {ultima_cosecha}", format="%.2f%%"),
                        "fpd2_rate_ant": st.column_config.NumberColumn(f"% FPD {cosecha_ant}", format="%.2f%%")}
                with col_t:
                    st.markdown("🔴 **Top 10: Mayor FPD**")
                    st.dataframe(df_rank.sort_values('fpd2_rate', ascending=False).head(10), column_config=conf, hide_index=True, use_container_width=True)
                with col_b:
                    st.markdown("🟢 **Bottom 10: Menor FPD**")
                    st.dataframe(df_rank.sort_values('fpd2_rate', ascending=True).head(10), column_config=conf, hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Error en el procesamiento: {e}")

# Pestañas restantes
with tab2: pass
with tab3: pass
with tab4: pass