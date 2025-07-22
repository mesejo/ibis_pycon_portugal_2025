from pathlib import Path

import dash
import ibis
import plotly.express as px
from dash import dcc, html
from dash.dependencies import Input, Output

data_dir = Path(__file__).resolve().parents[2] / "data"
con = ibis.duckdb.connect()
coffees = ibis.read_parquet(data_dir / "cleaned_coffees.parquet")

origins = coffees.distinct(on="origin_region").origin_region.to_list()

app = dash.Dash(__name__)
app.layout = html.Div([
    dcc.Dropdown(
        id='origins-filter',
        options=[{'label': i, 'value': i} for i in origins],
        value=origins,
        multi=True
    ),
    dcc.Graph(id='dot-plot')
])

@app.callback(
    Output('dot-plot', 'figure'),
    Input('origins-filter', 'value')
)
def update_plot(selected_species):
    filter_table = coffees.filter(coffees.origin_region.isin(selected_species))
    return px.scatter(
        filter_table.to_pandas(),
        x='sca_points',
        y='price_eur',
        color='origin_region',
        title='Dot Plot: SCA vs Price'
    )

if __name__ == '__main__':
    app.run(debug=True)
