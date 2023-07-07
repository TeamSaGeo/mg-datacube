from dash import Dash, html, dcc, callback, Output, Input, State
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date, datetime
import dash_mantine_components as dmc
import os
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

# server = Flask(__name__)
# app = Dash(server=server)
app = Dash(__name__)
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
#------------Début recherche de la liste des niveaux de lieux -------------------

app.layout = html.Div([
    html.H1(children='Madagascar Data Cube', style={'textAlign':'center'}),

    html.Div(children=[

        html.Div(children=[
            html.Label("Collection"),
            dcc.Dropdown(collections,"rainfall_chirps_monthly", id='collection-selection', clearable=False),
            html.Div(id='collection-description'),
            html.Div(id='collection-date_range', style={'margin-bottom': 20}),

            dmc.DateRangePicker(id='my-date-range-picker',label="Date Range",style={'margin-bottom': 30}),

            html.Label("Location level"),
            dcc.Dropdown(id='location-level-selection',
                         options=location_level,
                         clearable=False, style={'margin-bottom': 30}),

            html.Label("Location"),
            dcc.Dropdown(id='location-selection', multi=True, style={'margin-bottom': 30}),

            html.Button("Download CSV", id="btn_csv"),
            dcc.Download(id="download-dataframe-csv"),

            dcc.Store(id='memory'),
            dcc.Store(id='memory_per_location'),

        ], style={'padding': 20, 'background-color': "cadetblue",  'flex': 1}),

        html.Div(children=[
            dcc.Graph(id='facet-graph', style={'width': '650px', 'height': '500px'}),
        ], style={'padding': 10, 'margin-left': 20, 'flex': 1})

    ], style={'display': 'flex', 'flex-direction': 'row'}),
])

#------------Début Mise à jour de la date range -------------------
@app.callback(
    Output('my-date-range-picker', 'minDate'),
    Output('my-date-range-picker', 'maxDate'),
    Output('collection-description', 'children'),
    Output('collection-date_range', 'children'),
    Input('collection-selection', 'value'))
def set_collection_options(selected_collection):
    if not selected_collection :
        raise PreventUpdate

    collection = catalog.get_collection(selected_collection)
    if collection:
        start_date, end_date = collection.extent.temporal.intervals[0]
        min_date = start_date.date()
        max_date = end_date.date()
        description = f"You have selected {collection.description}"
        date_range = f"(from {min_date} to {max_date})"
    else:
        min_date = datetime(1979, 1, 1).date()
        max_date = date.today()
        description = f"You have selected Air temperature estimate"
        date_range = f"(from {min_date} to {max_date})"

    return min_date, max_date, description, date_range
#------------ Fin Mise à jour de la date range -------------------

#------------Début Mise à jour des niveaux de lieux -------------------
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
#------------Fin Mise à jour des niveaux de lieux -------------------

#------------Début Mise à jour des lieux -------------------
@app.callback(
    Output("location-selection", "value"),
    Input("location-selection", "options"),
)
def update_location_value(location_options):
    return []
#------------Fin Mise à jour des lieux -------------------

#------------Début Mise à jour des données -------------------
@callback(
    Output('memory', 'data'),
    Input('collection-selection', 'value'),
    Input('my-date-range-picker', 'value'),
    State("location-level-selection", "value"),
    Input("location-selection", "value"),
)
def update_data(selected_collection,date_range, selected_level,location):
    if not date_range :
        raise PreventUpdate

    # Get items from selected collection
    query = catalog.search(
        collections=[selected_collection],
        datetime=f"{date_range[0]}/{date_range[1]}",
        )
    items = list(query.items())
    lat, lon = -18.7557, 46.8644  # Madagascar
    buffer_lat, buffer_lon = 7, 4

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

    if location :
        # filter dataarray by selected location
        zipfile = "zip://" + shp_ocha_mada_zip + "!" + selected_level
        df = gpd.read_file(zipfile)
        df_location = df.loc[df[location_level[selected_level]].isin(np.array(location))]
        mask = xr_rasterize(df_location,ds)
        ds = ds.where(mask)

    var_name = list(ds.data_vars)[0]
    ds = ds[var_name].compute().to_dict()
    return ds

def get_mean_data_per_location(mask_data,selected_level,location):
    ds_per_location = []
    zipfile = "zip://" + shp_ocha_mada_zip + "!" + selected_level
    df = gpd.read_file(zipfile)
    for l in location:
        df_per_location = df.loc[df[location_level[selected_level]] == l ]
        df_per_location_mask = xr_rasterize(df_per_location,mask_data)
        ds_location = mask_data.where(df_per_location_mask)
        ds_per_location.append(ds_location.mean(['latitude', 'longitude']).drop('spatial_ref'))

    concated_ds = xr.concat(ds_per_location,dim=location).rename({"concat_dim":location_level[selected_level]})
    return concated_ds

#------------Fin Mise à jour des données -------------------

#------------Début Mise à jour de la graphe  -------------------
@callback(
    Output('facet-graph', 'figure'),
    Input('memory', 'data'),
    State("location-level-selection", "value"),
    State("location-selection", "value"),
)
def update_graph(dict_data,selected_level,location):
    if not dict_data :
        raise PreventUpdate

    ds = xr.DataArray.from_dict(dict_data)
    times = [str(i.date()) for i in pd.to_datetime(ds.time.values)]
    nb = len(times)

    # show figure if dataarray less than 18 items
    if nb <= 18 and len(ds.dims)<4:
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
        if not location :
            ds_mean = ds.mean(['latitude', 'longitude']).drop('spatial_ref').to_dataframe()
            try:
                fig = px.line(ds_mean, y=ds.name)
            except:
                fig = px.line(ds_mean, x=ds_mean.index.get_level_values(1), y=ds.name, color=ds_mean.index.get_level_values(0),
                labels={"x":"time", "color":"paramètres"})
        else:
            ds_mean = get_mean_data_per_location(ds,selected_level,location).to_dataframe()
            ds_mean_temp = ds_mean[ds_mean.index.get_level_values(1) == "t_mean"]
            if not ds_mean_temp.empty:
                ds_mean = ds_mean_temp

            fig = px.line(ds_mean, x=ds_mean.index.get_level_values(-1), y=ds.name, color=ds_mean.index.get_level_values(0),
                         labels={"x":"time", "color": location_level[selected_level] })
    return fig
#------------Fin Mise à jour de la graphe  -------------------

#------------Début Mise à jour du CSV  -------------------
@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("btn_csv", "n_clicks"),
    State('memory', 'data'),
    State('collection-selection', 'value'),
    State("location-level-selection", "value"),
    State("location-selection", "value"),
    prevent_initial_call=True,
)
def download_data(n_clicks, dict_data, selected_collection, selected_level,location):
    if not dict_data or not location :
        raise PreventUpdate

    ds = xr.DataArray.from_dict(dict_data)
    concated_ds = get_mean_data_per_location(ds,selected_level,location)

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

    return dcc.send_data_frame(output_df.to_csv, f"{selected_collection}.csv", sep = ';', decimal=",")

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_ui=True, host="0.0.0.0", port="80")
