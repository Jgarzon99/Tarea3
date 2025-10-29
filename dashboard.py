import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import os

PARQUET_DIR = "data/processed/sensors_by_hour"

app = dash.Dash(__name__)
server = app.server

def load_latest_sample(n=5000):
    if not os.path.exists(PARQUET_DIR):
        return pd.DataFrame()
    try:
        df = pd.read_parquet(PARQUET_DIR)
        return df.sample(min(n, len(df)))
    except Exception as e:
        print("Error leyendo parquet:", e)
        return pd.DataFrame()

app.layout = html.Div([
    html.H3('Dashboard demo — Traffic Sensors'),
    dcc.Interval(id='interval', interval=5*1000, n_intervals=0),
    dcc.Graph(id='vehicles_dist')
])

@app.callback(
    dash.dependencies.Output('vehicles_dist', 'figure'),
    [dash.dependencies.Input('interval', 'n_intervals')]
)
def update_graph(n):
    df = load_latest_sample()
    if df.empty:
        return px.line()
    fig = px.histogram(df, x='vehicles_sum', nbins=50, title='Distribución de conteo por hora (muestra)')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
