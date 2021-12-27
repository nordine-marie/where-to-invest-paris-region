# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from math import nan, isnan
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

import json


app = dash.Dash(__name__)
server = app.server

df = pd.read_csv('../outputs/idf_clusterized_cities.csv')
df["B/GPE Class"] = df["B/GPE Class"].apply(str)
df = df.rename(columns={"B/GPE Class" : "City Class"})

clusters_fig = px.scatter(
    df, x="Building Score", y="GrandParisExpress Score", size="Population",
    color="City Class",hover_name="City Name"
    )

geo_fig = px.scatter(
    df, x="Building Score", y="GrandParisExpress Score", size_max=60,
    )

fig_topleft = clusters_fig


app.title = "Where to Invest in Paris Region"
app.layout = html.Div([
    html.H1("Where to Invest in Paris Region"),
    html.Div([
        html.Div([
            dcc.Graph(
                id='fig_topleft',
                style={'width': '50vw', 'height': '80vh'},
                figure=fig_topleft,
                hoverData={'points': [{'customdata': 'SAINT-OUEN'}]},
            ),  
            dcc.RadioItems(
                id='fig_topleft_radio',
                options=[
                    {'label': 'Cluster view', 'value': 'CV'},
                    {'label': 'Geo view', 'value': 'GV'},
                    ],
                value='CV'
                ),
        ],style={'display':'flex','flex-direction':'column','align-items': 'center'}),
        html.Div([
            dcc.Graph(
                id='land-value-time-series',
                style={'width': '45vw', 'height': '80vh'},
            ),
            ])
    ],style={'display':'flex'})

],style={'text-align': 'center'})

## CALLBACKS

@app.callback(
    Output('fig_topleft', 'figure'),
    Input('fig_topleft_radio', 'value'))
def update_figure(selected_view): # top left figure switcher
    if selected_view == 'CV' : # cluster view
        fig_topleft = clusters_fig
    else : # geo view
        fig_topleft = geo_fig
    fig_topleft.update_layout()

    return fig_topleft

@app.callback(
    dash.dependencies.Output('land-value-time-series', 'figure'),
    dash.dependencies.Input('fig_topleft', 'hoverData'))
def update_x_timeseries(hoverData):
    try :
        hovered_city_name = hoverData["points"][0]['hovertext']   
    except KeyError:
        hovered_city_name = "SAINT-OUEN"

    hovered_city=df[df["City Name"]==hovered_city_name]


    #df_city = pd.read_csv('https://files.data.gouv.fr/geo-dvf/latest/csv/2016/communes/75/75105.csv')
    #print('hey',df_city)
    return create_time_series(hovered_city)

def create_time_series(city):

    insee_code = str(city["Insee code"].values[0])
    
    with open('../outputs/idf_ppsm.json') as json_file:
        ppsm_dict = json.load(json_file)
        ppsm_df = pd.DataFrame(data={'Year':[year for year in range(2016,2022)], 'Price per square meter': ppsm_dict[insee_code]})


    fig = px.scatter(ppsm_df, x='Year', y='Price per square meter')

    fig.update_traces(mode='lines+markers')

    fig.update_xaxes(showgrid=False)

    fig.update_yaxes(type='linear')

    fig.add_annotation(x=0, y=1, xanchor='left', yanchor='bottom',
                       xref='paper', yref='paper', showarrow=False, align='left',
                       text=city["City Name"].values[0])

    #fig.update_layout(height=225, margin={'l': 20, 'b': 30, 'r': 10, 't': 10})

    return fig

if __name__ == '__main__':
    app.run_server(debug=True,port=8050)