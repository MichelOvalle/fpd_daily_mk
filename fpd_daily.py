import pandas as pd
import plotly.express as px

# Nombre del archivo fuente
FILE_NAME = "fpd_gemini.parquet"

def generar_fpd_daily():
    # 1. Cargar el archivo Parquet
    # El archivo contiene columnas clave como fpd2, fecha_apertura y monto_otorgado 
    try:
        df = pd.read_parquet(FILE_NAME)
        print(f"✅ Datos cargados: {len(df)} registros encontrados.")
    except Exception as e:
        print(f"❌ Error al cargar el archivo: {e}")
        return

    # 2. Preparación de fechas y creación de la Cosecha
    df['fecha_apertura'] = pd.to_datetime(df['fecha_apertura'])
    # Creamos la cosecha (Vintage) basada en el mes y año de apertura
    df['cosecha_mes'] = df['fecha_apertura'].dt.to_period('M').astype(str)

    # 3. Cálculo de métricas por Cosecha
    # Agrupamos para obtener el total de créditos y la suma de FPD2 [cite: 60, 61]
    vintage_df = df.groupby('cosecha_mes').agg(
        total_creditos=('id_credito', 'count'),
        casos_fpd2=('fpd2', 'sum'),
        monto_total=('monto_otorgado', 'sum')
    ).reset_index()

    # Cálculo del % de FPD2 (Tasa de Incumplimiento)
    vintage_df['fpd2_rate'] = (vintage_df['casos_fpd2'] / vintage_df['total_creditos']) * 100
    
    # Ordenamos cronológicamente para asegurar que se vea la "cosecha más reciente"
    vintage_df = vintage_df.sort_values('cosecha_mes')

    # 4. Visualización de Cosechas con Plotly
    # Esta gráfica mostrará la tendencia y permitirá ver cada nueva cosecha agregada
    fig = px.line(
        vintage_df, 
        x='cosecha_mes', 
        y='fpd2_rate',
        markers=True,
        text=vintage_df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
        title='<b>FPD Daily: Análisis de Cosechas (Vintage)</b><br><sup>Métrica: % FPD2 por Mes de Apertura</sup>',
        labels={'cosecha_mes': 'Cosecha (Mes de Apertura)', 'fpd2_rate': '% Tasa FPD2'},
        template='plotly_white'
    )

    # Estética de la gráfica
    fig.update_traces(
        line=dict(width=3, color='#2E86C1'),
        marker=dict(size=10),
        textposition="top center"
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        hovermode="x unified",
        yaxis_ticksuffix="%",
        margin=dict(l=40, r=40, t=80, b=40)
    )

    # 5. Salida de resultados
    print("\n--- Resumen de las últimas cosechas ---")
    print(vintage_df.tail(5).to_string(index=False))
    
    fig.show()

if __name__ == "__main__":
    generar_fpd_daily()