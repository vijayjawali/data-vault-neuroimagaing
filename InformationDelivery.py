# %%
# ! pip install dash
# ! pip install dash-core-components
# ! pip install dash-html-components
# ! pip install dash-renderer


import dash
from dash import dcc as dcc
from dash import html as html
import psycopg2, pickle
from psycopg2 import Error
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import pickle


def readObservationDataFromEnterpriseLayer(conn):
    try:
        connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
        column_names = ["ObservationMetaDataKey","ObservationKey","name","value","timestamps"]
        
        sql = """SELECT 
                fo."ObservationMetaDataKey",
                fo."ObservationKey",
                dob."name",
                dob."value"[1:array_upper(dob."value", 1)][1:1],
                dob."timestamps"
                FROM "FactObservation" fo 
                INNER JOIN "DimObservation" dob ON (
                    fo."ObservationKey" = dob."ObservationKey"
                ) WHERE dob."name" = 'VM0001_Moto_HBA_Probe1_Deoxy'"""
        cursor = connection.cursor()
        cursor.execute(sql)
        tuples_list = cursor.fetchall()

        df = pd.DataFrame(tuples_list, columns=column_names)
        return df

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def readKeyvalueFromEnterpriseLayer(conn):
    try:
        connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
        column_names = ["Key","value"]
        
        sql = """select 
                    dmd."key",
                    dmd."value"
                from "FactObservation" fo
                INNER JOIN "DimMetaData" dmd ON (
                    dmd."MetaDataKey" = fo."MetaDataKey"
                )
                INNER JOIn "DimObservation" dob ON (
                    dob."ObservationKey" = fo."ObservationKey"
                ) WHERE dob."name" = 'VM0001_Moto_HBA_Probe1_Deoxy'"""
        cursor = connection.cursor()
        cursor.execute(sql)
        
        tuples_list = []
        metaDataDict = {}
        
        for key, value in cursor.fetchall(  ):
            metaDataDict[key] = pickle.loads(value)
            tuples_list.append((key, pickle.loads(value)))

        
        df = pd.DataFrame(tuples_list, columns=column_names)
        return df, metaDataDict

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def readExperimentListWithFactors(conn):
    try:
        connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
        column_names = ["experiment","factor","treatment"]
        
        sql = """select 
                    de."title",
                    df."name",
                    df."levelValue"
                from "FactTreatmentFactors" ftf
                INNER JOIN "DimExperiment" de ON (
                    de."ExperimentKey" = ftf."Experimentkey"
                )
                INNER JOIN "DimFactor" df ON (
                    df."FactorKey" = ftf."Factorkey"
                )"""
        cursor = connection.cursor()
        cursor.execute(sql)
        tuples_list = cursor.fetchall()

        df = pd.DataFrame(tuples_list, columns=column_names)
        return df

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def readVMGroups(conn):
    try:
        connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
        column_names = ["group","ExperimentalUnit"]
        
        sql = """select 
                    dg."name",
                    ds."name"
                from "FactAssignedTo" fat 
                INNER JOIN "DimGroup" dg ON (
                    dg."GroupKey" = fat."GroupKey"
                )
                INNER JOIN "DimSubject" ds ON (
                    fat."ExperimentalUnitKey" = ds."SubjectKey"
                ) WHERE ds."name" LIKE 'Subj%'"""
        cursor = connection.cursor()
        cursor.execute(sql)
        tuples_list = cursor.fetchall()

        df = pd.DataFrame(tuples_list, columns=column_names)
        return df

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
    


def readPreAutismGroups(conn):
    try:
        connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
        column_names = ["group","ExperimentalUnit"]
        
        sql = """select 
                    dg."name",
                    ds."name"
                from "FactAssignedTo" fat 
                INNER JOIN "DimGroup" dg ON (
                    dg."GroupKey" = fat."GroupKey"
                )
                INNER JOIN "DimSubject" ds ON (
                    fat."ExperimentalUnitKey" = ds."SubjectKey"
                ) WHERE ds."name" LIKE 'Autism%'"""
        cursor = connection.cursor()
        cursor.execute(sql)
        tuples_list = cursor.fetchall()

        df = pd.DataFrame(tuples_list, columns=column_names)
        return df

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def nameToFigureGroup(fig_name, connectionParameters):
    figure = go.Figure()
    
    if fig_name == 'Visuomotor functional connectivity':
            experimentListWithGroupsAndSubjectsDF = readVMGroups(connectionParameters)
            experimentListWithGroupsDF = experimentListWithGroupsAndSubjectsDF[['group']].drop_duplicates()
            figure = go.Figure(data=[go.Table(
            header=dict(values=list(experimentListWithGroupsDF.columns),
                            fill_color='paleturquoise',
                            align='left'),
            cells=dict(values=experimentListWithGroupsDF.transpose().values.tolist(),
                            fill_color='lavender',
                            align='left'))
            ])
    elif fig_name == 'Pre-autism': 
        experimentListWithGroupsAndSubjectsDF = readPreAutismGroups(connectionParameters)
        experimentListWithGroupsDF = experimentListWithGroupsAndSubjectsDF[['group']].drop_duplicates()
        figure = go.Figure(data=[go.Table(
        header=dict(values=list(experimentListWithGroupsDF.columns),
                        fill_color='paleturquoise',
                        align='left'),
        cells=dict(values=experimentListWithGroupsDF.transpose().values.tolist(),
                        fill_color='lavender',
                        align='left'))
        ])
    return dcc.Graph(figure=figure)


def nameToFigureExperimentalUnit(fig_name, connectionParameters):
    figure = go.Figure()
    
    if fig_name == 'Visuomotor functional connectivity':
            experimentListWithGroupsAndSubjectsDF = readVMGroups(connectionParameters)
            experimentListSubjectsDF = experimentListWithGroupsAndSubjectsDF[['ExperimentalUnit']].drop_duplicates()
            figure = go.Figure(data=[go.Table(
            header=dict(values=list(experimentListSubjectsDF.columns),
                            fill_color='paleturquoise',
                            align='left'),
            cells=dict(values=experimentListSubjectsDF.transpose().values.tolist(),
                            fill_color='lavender',
                            align='left'))
            ])
    elif fig_name == 'Pre-autism': 
        experimentListWithGroupsAndSubjectsDF = readPreAutismGroups(connectionParameters)
        experimentListSubjectsDF = experimentListWithGroupsAndSubjectsDF[['ExperimentalUnit']].drop_duplicates()
        figure = go.Figure(data=[go.Table(
        header=dict(values=list(experimentListSubjectsDF.columns),
                        fill_color='paleturquoise',
                        align='left'),
        cells=dict(values=experimentListSubjectsDF.transpose().values.tolist(),
                        fill_color='lavender',
                        align='left'))
        ])
    return dcc.Graph(figure=figure)


if __name__ == '__main__':
    
    connectionParameters = {}
    
    connectionParameters['user']="postgres"
    connectionParameters['password']="Vijay42****@"
    connectionParameters['host']="localhost"
    connectionParameters['port']="5432"
    connectionParameters['database']="smdvault17"
    
    
    df = readObservationDataFromEnterpriseLayer(connectionParameters)
    dflist = df['value'].tolist()
    firstGraphy = np.ravel(dflist).tolist()
    timelist = df['timestamps'].tolist()
    firstGraphx = np.ravel(timelist).tolist()
    
    app = dash.Dash()
    experiment_names = ['Visuomotor functional connectivity', 'Pre-autism']
    experiment_dropdown = html.Div([
        dcc.Dropdown(
            id='fig_dropdown',
            options=[{'label': x, 'value': x} for x in experiment_names],
            value=None
        )])
    groupPlot = html.Div(id='groupPlot')
    experimentalUnitsPlot = html.Div(id='experimentalUnitsPlot')
    
    @app.callback(
    dash.dependencies.Output('groupPlot', 'children'),
    [dash.dependencies.Input('fig_dropdown', 'value')])
    def update_output(fig_name):
        return nameToFigureGroup(fig_name, connectionParameters)
    
    @app.callback(
    dash.dependencies.Output('experimentalUnitsPlot', 'children'),
    [dash.dependencies.Input('fig_dropdown', 'value')])
    def update_output(fig_name):
        return nameToFigureExperimentalUnit(fig_name, connectionParameters)
    
    experimentListWithFactorsDF = readExperimentListWithFactors(connectionParameters)
    table = go.Figure(data=[go.Table(
            header=dict(values=list(experimentListWithFactorsDF.columns),
                        fill_color='paleturquoise',
                        align='left'),
            cells=dict(values=experimentListWithFactorsDF.transpose().values.tolist(),
                        fill_color='lavender',
                        align='left'))
        ])
    
    metadataDF, metadataDict = readKeyvalueFromEnterpriseLayer(connectionParameters)
    metadataTable =  go.Figure(data=[go.Table(
            header=dict(values=list(metadataDF.columns),
                        fill_color='paleturquoise',
                        align='left'),
            cells=dict(values=metadataDF.transpose().values.tolist(),
                        fill_color='lavender',
                        align='left'))
        ])
    
    app.layout = html.Div([ 
        html.H1(children='Storing and Managing Data',
                style={'textAlign': 'center',
                    'color': '#000205'}
                ),
        html.Br(),
        html.H3(children='Module 06-32245 Semester project',
                style={'textAlign': 'center',
                    'color': '#000205'}
                ),
        html.Br(),
        html.Div(children='Individual plotting of the time course of light raw intensity at some wavelength, HbO2 or HbR for some channel.',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        html.Br(),
        dcc.Graph(
            figure={
                'data': [
                    {'y': firstGraphy,
                    'x': firstGraphx,
                    'type': 'line',
                    'name':'Bar Graph'}
                ],
                'layout': go.Layout(
                    title='Line Plot of single channel deoxy-hemoglobin (HbR) data for VM0001',
                    xaxis={'title': 'timestamps'},
                    yaxis={'title': 'deoxy-hemoglobin (HbR)'} 
                )
            }
        ),
        html.Br(),
        html.Br(),
        html.Div(children='A listing of experiments in the database accompanied by the list of factors and treatments given',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        html.Div(dcc.Graph(figure = table)),
        html.Br(),
        html.Br(),
        html.Div(children='For a certain experiment choose at run time, retrieve the groups and the list of experimental units.',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        experiment_dropdown, 
        groupPlot, 
        experimentalUnitsPlot,
        html.Br(),
        html.Br(),
        html.Div(children='Given an individual observation whether of light raw intensity at some wavelength, HbO2 or HbR retrive all available metadata',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        html.Br(),
        html.Br(),
        html.Div(children='metadata for deoxy-hemoglobin (HbR) data for VM0001',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        html.Div(dcc.Graph(figure = metadataTable)),
        html.Br(),
        html.Br()
        ])

 


    app.run_server(port=4050)


