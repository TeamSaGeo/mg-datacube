from dash import Dash, html, dcc, callback, Output, Input, State, DiskcacheManager, CeleryManager, ALL
import base64, io, os
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date, datetime
import dash_mantine_components as dmc
import geopandas as gpd
from zipfile import ZipFile
from deafrica_tools.spatial import xr_rasterize
from dash.exceptions import PreventUpdate
import xarray as xr
from pystac_client import Client
from odc.stac import configure_rio, stac_load
import dask.distributed
from deafrica_tools.load_era5 import load_era5
# from flask import Flask

if 'REDIS_URL' in os.environ:
    # Use Redis & Celery if REDIS_URL set as an env variable
    from celery import Celery
    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)
else:
    # Diskcache for non-production apps when developing locally
    import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)

app = Dash(__name__, suppress_callback_exceptions=True, background_callback_manager=background_callback_manager)
server = app.server

#------------Début recherche de la liste des collections -------------------
configure_rio(
    cloud_defaults=True,
    aws={"aws_unsigned": True},
    AWS_S3_ENDPOINT="s3.af-south-1.amazonaws.com",
)
catalog = Client.open("https://explorer.digitalearth.africa/stac")

# collections = [collection.id for collection in catalog.get_collections()]
collections = ["rainfall_chirps_monthly", "rainfall_chirps_daily", "air_temperature_at_2_metres_monthly", "air_temperature_at_2_metres_daily"]
#------------Fin recherche de la liste des collections -------------------

#------------Début recherche de la liste des niveaux de lieux -------------------
shp_ocha_mada_zip = "shp_ocha_mada_dash.zip"
location_level = {}
with ZipFile(shp_ocha_mada_zip,'r') as zip_obj:
    for objname in zip_obj.namelist():
        if objname.endswith(".shp"):
            shp_name = os.path.basename(objname).split("_")[1]
            location_level[objname]= shp_name
#------------Fin recherche de la liste des niveaux de lieux -------------------

app.layout = html.Div([
    html.H1(children='Madagascar Data Cube', style={'textAlign':'center'}),

    html.Div(children=[
        html.Div(children=[
            html.Label("Paramètres"),
            dcc.Dropdown(collections,"rainfall_chirps_monthly", id='collection-selection', clearable=False),
            html.Div(id='collection-description', style={'margin-bottom': 10}),

            dmc.DateRangePicker(id='my-date-range-picker',label="Plage de date",style={'margin-bottom': 20}),

            html.Label("Type du vecteur"),
            dcc.RadioItems(['Polygone', 'Point particulier'], 'Polygone',id="radio_items",style={'margin-bottom': 20}),
            html.Div(id = "output_type"),

            html.Button("Rechercher", id="btn_search", style={'margin-right': 60}),

            dcc.Store(id='memory'),

            html.Button("Exporter en CSV", id="btn_csv", disabled=True),
            dcc.Download(id="download-dataframe-csv"),

        ], style={'padding': 20, 'background-color': "cadetblue",  'flex': 1}),

        html.Div(children=[
            dcc.Graph(id='facet-graph', style={'width': '600px', 'height': '500px'}),
        ], style={'padding': 10, 'margin-left': 20, 'flex': 1})

    ], style={'display': 'flex', 'flex-direction': 'row'}),
])

#-----------Début Mise à jour de la date range -------------------
@app.callback(
    Output('my-date-range-picker', 'minDate'),
    Output('my-date-range-picker', 'maxDate'),
    Output('collection-description', 'children'),
    Input('collection-selection', 'value'))
def set_collection_options(selected_collection):
    if not selected_collection :
        raise PreventUpdate

    collection = catalog.get_collection(selected_collection)
    if collection:
        start_date, end_date = collection.extent.temporal.intervals[0]
        min_date = start_date.date()
        max_date = end_date.date()
        info = collection.description
        data_source = html.A("CHIRPS", href="https://www.chc.ucsb.edu/data/chirps" )
        spatial_resolution = html.A("5.55 km")
        unite = html.A("mm")
    else:
        min_date = datetime(1979, 1, 1).date()
        max_date = date.today()
        info = "Air temperature estimate"
        data_source = html.A("AWS Public Dataset Program", href="https://registry.opendata.aws/ecmwf-era5/")
        spatial_resolution = html.A("31 km")
        unite = html.A("°C")

    description = [html.B('Description: '), info , html.Br(),
                    html.B("Source: "), data_source, html.Br(),
                    html.B("Plage de dates: "), min_date, html.B(" à "), max_date, html.Br(),
                    html.B("Résolution spatiale: "), spatial_resolution, html.B(";\t\tUnité: "), unite
                    ]

    return min_date, max_date, description
#------------ Fin Mise à jour de la date range -------------------

#------------Début Choix du type de données à exporter -------------------
limit_admin = html.Div([
        html.Label("Limite administratif"),
        dcc.Dropdown(id='location-level-selection',
                     options=location_level,
                     clearable=False, style={'margin-bottom': 20}),
        html.Label("Lieu"),
        dcc.Dropdown(id='location-selection', multi=True, style={'margin-bottom': 20}),],id={
            'type': 'dynamic_output_type',
            'index': 0
        },),
point = html.Div([
        html.Div(["Choisir un fichier ayant au moins les champs", html.B("(Points;Longitude;Latitude)"), html.Br(),
                " séparés par des ", html.B("\";\""), html.Br(),
                " et dont le séparateur décimal est ", html.B("\",\"")], style={'margin-bottom': 20}),
        dcc.Upload(id='upload-data',
            children=html.Div(['Déposer ou ', html.A('Sélectionner un fichier csv')]),
            style={
                'width': '100%',
                'height': '60px',
                'lineHeight': '60px',
                'borderWidth': '1px',
                'borderStyle': 'dashed',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin-bottom': '20px'
            },
            accept = ".csv",
        ),
        html.Div(id='output-data-upload'), dcc.Store(id='csv_data'),],id={
            'type': 'dynamic_output_type',
            'index': 1
        }),
@app.callback(
    Output("output_type", "children"),
    Input("radio_items", 'value'),
    )
def display_radio_choice(value):
    if value == "Polygone":
        return limit_admin
    else:
        return point
#------------Fin Choix du type de données à exporter -------------------

#------------Début Mise à jour des points entrées par l'utilisateur -------------------
@callback(
    Output('output-data-upload', 'children'),
    Output('csv_data', 'data'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
    prevent_initial_call=True)
def update_output_points(contents, filename):
    if not contents :
        raise PreventUpdate

    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        # Assume that the user uploaded a CSV file
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep = ';', decimal=",")
        if all(item in df.columns for item in ['Latitude', "Longitude", "Points"]):
            return html.Div([html.B('Nom du fichier: '), filename], style={'margin-bottom': 20}), df.to_dict()
        else:
            return html.Div([html.B('Erreur: '), "Le fichier ne correspond pas aux standards demandés" ], style={'margin-bottom': 20, 'color':"rgb(154, 42, 42)"}), None
    except Exception as e:
        return html.Div([html.A(e)], style={'margin-bottom': 20, 'color':"rgb(154, 42, 42)"}), None
#------------Fin Mise à jour des points entrées par l'utilisateur -------------------

# #------------Début Mise à jour des limites administratifs -------------------
@app.callback(
    Output("location-selection", "options"),
    Input("location-level-selection", "value"),
)
def update_location_options(selected_value):
    if not selected_value:
        raise PreventUpdate
    zipfile = "zip://" + shp_ocha_mada_zip + "!" + selected_value
    mscar = gpd.read_file(zipfile)
    return mscar[location_level[selected_value]].tolist()

@app.callback(
    Output("location-selection", "value"),
    Input("location-selection", "options"),
)
def update_location_value(location_options):
    return []
#------------Fin Mise à jour des limites administratifs -------------------

#------------Début Mise à jour des données -------------------
@callback(
    Output('memory', 'data'),
    Output('facet-graph', 'figure'),
    Input("btn_search", "n_clicks"),
    State('collection-selection', 'value'),
    State('my-date-range-picker', 'value'),
    State({'type': "dynamic_output_type", 'index': ALL}, 'children'),
    prevent_initial_call=True,
    background=True,
    running=[
        (Output("btn_search", "disabled"), True, False),
        (Output("btn_csv", "disabled"), True, False),
    ],
)
def search_data(n_clicks,selected_collection,date_range, children):
    if not date_range:
        raise PreventUpdate

    location = None
    if children[0][0]["props"]["children"] == "Limite administratif":
        try:
            selected_level = children[0][1]["props"]["value"]
            location = children[0][3]["props"]["value"]
            if not location:
                raise PreventUpdate
        except:
            raise PreventUpdate
    else:
        try:
            df_csv = children[0][3]["props"]["data"]
            if not df_csv:
                raise PreventUpdate
        except :
            raise PreventUpdate

    # Get items from selected collection
    query = catalog.search(
        collections=[selected_collection],
        datetime=f"{date_range[0]}/{date_range[1]}",
        )
    items = list(query.items())

    # If type = polygon
    if location:
        lat, lon = -18.7557, 46.8644  # Madagascar
        buffer_lat, buffer_lon = 7, 4

        # If paramètre = rainfall
        if items:
            # Set the bounding box
            # [xmin, ymin, xmax, ymax] in latitude and longitude (EPSG:4326).
            bbox = [lon - buffer_lon, lat - buffer_lat, lon + buffer_lon, lat + buffer_lat]
            ds = stac_load(
                        items,
                        bbox = bbox,
                        chunks={},
                        crs = "EPSG:4326",
                        resolution = 0.045,
                        )

        # If paramètre = temperature
        else:
            time = tuple(date_range)
            frequence = "1D" if "daily" in selected_collection else "1M"
            selected_collection = selected_collection[:27]
            lat_list = [lat-buffer_lat, lat+buffer_lat]
            lon_list = [lon-buffer_lon, lon+buffer_lon]
            temp_max = load_era5(selected_collection, lat_list,lon_list , time, reduce_func=np.max, resample=frequence )
            temp_min = load_era5(selected_collection, lat_list, lon_list, time, reduce_func=np.min, resample=frequence )
            temp_mean = load_era5(selected_collection, lat_list, lon_list, time, reduce_func=np.mean, resample=frequence )

            ds = xr.concat([temp_max,temp_min, temp_mean], dim=["t_max", "t_min", "t_mean"])
            ds = ds.rename({'lon': 'longitude','lat': 'latitude', 'concat_dim': 'temperature'})
            # convert to Celsius, keeping other attributes
            attrs = ds.attrs
            attrs['units']='C'
            ds = ds - 273.15
            ds[selected_collection].attrs = attrs

        # # Mask dataarray with selected location
        zipfile = "zip://" + shp_ocha_mada_zip + "!" + selected_level
        df = gpd.read_file(zipfile)
        df_location = df.loc[df[location_level[selected_level]].isin(np.array(location))]
        mask = xr_rasterize(df_location,ds)
        ds = ds.where(mask)

        var_name = list(ds.data_vars)[0]
        ds = ds[var_name].compute()
        ds_mean = get_mean_data_per_location(ds,selected_level,location,df)
        legend = location_level[selected_level]

    # If type = points
    else:
        df = pd.DataFrame.from_dict(df_csv)
        ds_per_location = []
        if items:
            for index, row in df.iterrows():
                lat, lon = row["Latitude"], row["Longitude"]
                ds = stac_load(
                    items,
                    bbox = [lon, lat, lon, lat],
                    crs = "EPSG:4326",
                    resolution = 0.045,
                )
                ds_per_location.append(ds.mean(['latitude', 'longitude']).drop('spatial_ref'))
            ds_mean = xr.concat(ds_per_location,dim=df["Points"].to_list()).rename({"concat_dim":"points"})
        else:
            time = tuple(date_range)
            frequence = "1D" if "daily" in selected_collection else "1M"
            selected_collection = selected_collection[:27]
            for index, row in df.iterrows():
                lat, lon = [row["Latitude"]], [row["Longitude"]]
                temp_max = load_era5(selected_collection, lat,lon , time, reduce_func=np.max, resample=frequence )
                temp_min = load_era5(selected_collection, lat, lon, time, reduce_func=np.min, resample=frequence )
                temp_mean = load_era5(selected_collection, lat, lon, time, reduce_func=np.mean, resample=frequence )
                ds = xr.concat([temp_max,temp_min, temp_mean], dim=["t_max", "t_min", "t_mean"])
                ds = ds.rename({'concat_dim': 'temperature'})
                ds_per_location.append(ds.mean(['lat', 'lon']).drop('spatial_ref'))
            ds_mean = xr.concat(ds_per_location,dim=df["Points"].to_list()).rename({"concat_dim":"points"})
            # convert to Celsius, keeping other attributes
            attrs = ds_mean.attrs
            attrs['units']='C'
            ds_mean = ds_mean - 273.15
            ds_mean[selected_collection].attrs = attrs

        var_name = list(ds_mean.data_vars)[0]
        ds = ds_mean = ds_mean[var_name].compute()
        print(ds_mean)
        legend = "Points"

    fig = update_graph(ds,legend, ds_mean.to_dataframe())
    return ds_mean.to_dict(), fig

def get_mean_data_per_location(mask_data,selected_level,location,df):
    ds_per_location = []
    for l in location:
        df_per_location = df.loc[df[location_level[selected_level]] == l ]
        df_per_location_mask = xr_rasterize(df_per_location,mask_data)
        ds_location = mask_data.where(df_per_location_mask)
        ds_per_location.append(ds_location.mean(['latitude', 'longitude']).drop('spatial_ref'))

    concated_ds = xr.concat(ds_per_location,dim=location).rename({"concat_dim":location_level[selected_level]})
    return concated_ds

def update_graph(ds,legend,ds_mean):
    times = [str(i.date()) for i in pd.to_datetime(ds.time.values)]
    nb = len(times)

    # show figure if dataarray less than 18 items and collection is rainfall
    if nb <= 18 and len(ds.dims)<4 and legend != "Points" :
        ds["time"] = [i for i in range(nb)] # replace time values to int/float
        fig = px.imshow(ds, facet_col='time', facet_col_wrap=6, color_continuous_scale='YlGnBu', origin="lower")

        # set text times annotation
        col = len(np.unique([a['x'] for a in fig.layout.annotations]))
        remainder = nb % col
        nb_rows = nb // col
        x_axis_start_positions = []
        if remainder == 0:
            for i in range(nb_rows):
                x_axis_start_positions = list(range(col*i,col*(i+1))) + x_axis_start_positions
        else:
            x_axis_start_positions = list(range(remainder))
            for i in range(nb_rows):
                position_temp = len(x_axis_start_positions)
                x_axis_start_positions = list(range(position_temp,position_temp+col)) + x_axis_start_positions
        for i, t in zip(x_axis_start_positions, times):
            fig.layout.annotations[i]['text'] = t

    else:
        ds_mean_temp = ds_mean[ds_mean.index.get_level_values(1) == "t_mean"]
        if not ds_mean_temp.empty:
            ds_mean = ds_mean_temp

        fig = px.line(ds_mean, x=ds_mean.index.get_level_values(-1), y=ds.name, color=ds_mean.index.get_level_values(0),
                     labels={"x":"time", "color": legend })
    return fig
# ------------Fin Mise à jour des données -------------------

# ------------Début Export en CSV  -------------------
@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("btn_csv", "n_clicks"),
    State('memory', 'data'),
    State('collection-selection', 'value'),
    State({'type': "dynamic_output_type", 'index': ALL}, 'children'),
    # State("location-level-selection", "value"),
    prevent_initial_call=True,
)
def download_data(n_clicks, dict_data, selected_collection, children):
    if not dict_data :
        raise PreventUpdate

    selected_level = None
    if children[0][0]["props"]["children"] == "Limite administratif":
        selected_level = children[0][1]["props"]["value"]

    concated_ds = xr.DataArray.from_dict(dict_data)

    #     assign new coords
    concated_ds["time"] = pd.to_datetime(concated_ds.time)
    year = concated_ds.time.dt.year
    month = concated_ds.time.dt.month
    day = concated_ds.time.dt.day
    if "month" in selected_collection:
        split_date_ds = concated_ds.assign_coords(year=("time", year.data), month=("time", month.data))
        concated_ds = split_date_ds.set_index(time=("year", "month")).unstack("time")
    else:
        split_date_ds = concated_ds.assign_coords(year=("time", year.data), month=("time", month.data), day=("time", day.data))
        concated_ds = split_date_ds.set_index(time=("year", "month", "day")).unstack("time")

    output_df = concated_ds.to_dataframe()

    if "temperature" in selected_collection:
        columns = list(filter(lambda x: x!="temperature", output_df.index.names))
        output_df = output_df.pivot_table("air_temperature_at_2_metres",columns, "temperature")

    # Add code ocha
    if selected_level:
        zipfile = "zip://" + shp_ocha_mada_zip + "!" + selected_level
        df = gpd.read_file(zipfile)
        df_ocha_columns = df[[location_level[selected_level]] + list(df.columns[df.columns.str.endswith("_co")])]
        output_df = output_df.reset_index().merge(df_ocha_columns, on=location_level[selected_level])
    else:
        output_df = output_df.reset_index()

    return dcc.send_data_frame(output_df.to_csv, f"{selected_collection}.csv", sep = ';', decimal=",", index=False)
# ------------Fin Export en CSV  -------------------

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_ui=True, host="0.0.0.0", port="80")
