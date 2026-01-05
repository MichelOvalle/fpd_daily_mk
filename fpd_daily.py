@st.cache_data
def get_main_data(regionales, sucursales, productos, tipos):
    if not os.path.exists(DATA_PATH): 
        st.error("Archivo no encontrado")
        return pd.DataFrame()
    
    # --- DIAGNÓSTICO DE COLUMNAS ---
    try:
        columnas_reales = duckdb.query(f"DESCRIBE SELECT * FROM '{DATA_PATH}'").df()['column_name'].tolist()
        st.write("🔍 Columnas detectadas en el archivo:", columnas_reales)
    except Exception as e:
        st.error(f"Error al leer metadatos: {e}")
    # -------------------------------

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
        st.error(f"❌ Error detallado de DuckDB: {e}")
        return pd.DataFrame()