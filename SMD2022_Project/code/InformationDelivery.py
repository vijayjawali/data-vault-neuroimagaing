
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
import datetime as dt
from dateutil import parser


class PostgresReader():
    """
    This class reads data from postgres server and returns the data requested in the form of DataFrame. Data is retrieved from Data Marts of Data vault
    """
    
    
    def readGroupObservationDataFromEnterpriseLayer(self,conn)-> tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame]:
        """
        this method reads data from postgre and returns HbO2 and HbR data for a group of patients in the experiment

        Args:
            conn: Connection parameters

        Returns:
            tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame]: a tuple consisting a two pandas dataframe for HbO2 and HbR data

        """
        try:
            connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
            column_names = ["ObservationMetaDataKey","ObservationKey","name","value","timestamps"]
            
            sql = """SELECT 
                    fo."ObservationMetaDataKey",
                    fo."ObservationKey",
                    dob."name",
                    dob."value",
                    dob."timestamps"
                    FROM "FactObservation" fo 
                    INNER JOIN "DimObservation" dob ON (
                        fo."ObservationKey" = dob."ObservationKey"
                    ) WHERE dob."name" like '%ViMo%' and dob."name" like '%Oxy%'"""
            cursor = connection.cursor()
            cursor.execute(sql)
            tuples_list = cursor.fetchall()

            groupHbo2DF = pd.DataFrame(tuples_list, columns=column_names)
            
            
            sql = """SELECT 
                    fo."ObservationMetaDataKey",
                    fo."ObservationKey",
                    dob."name",
                    dob."value",
                    dob."timestamps"
                    FROM "FactObservation" fo 
                    INNER JOIN "DimObservation" dob ON (
                        fo."ObservationKey" = dob."ObservationKey"
                    ) WHERE dob."name" like '%ViMo%' and dob."name" like '%Deoxy%'"""
            cursor = connection.cursor()
            cursor.execute(sql)
            tuples_list = cursor.fetchall()

            groupHbR2DF = pd.DataFrame(tuples_list, columns=column_names)
            
            return groupHbo2DF,groupHbR2DF

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                
                
    def readBoxPlotGroupObservationDataFromEnterpriseLayer(self,conn)-> pd.core.frame.DataFrame:
        """
        this method reads data from postgre and returns HbO2 data for a group of patients for two intervals in the experiment

        Args:
            conn: Connection parameters

        Returns:
            pd.core.frame.DataFrame: a tuple consisting a two pandas dataframe for HbO2 data

        """
        try:
            connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
            column_names = ["ObservationMetaDataKey","ObservationKey","name","value","timestamps"]
              
            sql = """SELECT 
                    fo."ObservationMetaDataKey",
                    fo."ObservationKey",
                    dob."name",
                    dob."value"[1:array_upper(dob."value", 1)][1:2],
                    dob."timestamps"
                    FROM "FactObservation" fo 
                    INNER JOIN "DimObservation" dob ON (
                        fo."ObservationKey" = dob."ObservationKey"
                    ) WHERE dob."name" like '%Viso%' and dob."name" like '%Oxy%'"""
            cursor = connection.cursor()
            cursor.execute(sql)
            tuples_list = cursor.fetchall()

            groupHbo2DF = pd.DataFrame(tuples_list, columns=column_names)
            
            return groupHbo2DF

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def readObservationDataFromEnterpriseLayer(self,conn)-> pd.core.frame.DataFrame:
        """
        this method reads data from postgre and returns observation for a single subject

        Args:
            conn: Connection parameters

        Returns:
            pd.core.frame.DataFrame: a pandas dataframe containing observation data for a patient for one channel

        """
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
    
    def readBoxPlotObservationDataFromEnterpriseLayer(self,conn)-> pd.core.frame.DataFrame:
        """
        this method reads data from postgre and returns observation for a single subject for two intervals

        Args:
            conn: Connection parameters

        Returns:
            pd.core.frame.DataFrame: a pandas dataframe containing observation data for a patient for one channel

        """
        try:
            connection = psycopg2.connect(user=conn['user'],password=conn['password'],host=conn['host'],port=conn['port'],database=conn['database'])
            column_names = ["ObservationMetaDataKey","ObservationKey","name","value","timestamps"]
            
            sql = """SELECT 
                    fo."ObservationMetaDataKey",
                    fo."ObservationKey",
                    dob."name",
                    dob."value"[1:array_upper(dob."value", 1)][1:2],
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


    def readKeyvalueFromEnterpriseLayer(self,conn)-> tuple[pd.core.frame.DataFrame, dict]:
        """
        this method reads data from postgre and returns metadata for a single experimental unit in the form of pandas dataframe and dictionary of key, value pairs

        Args:
            conn: Connection parameters

        Returns:
            tuple[pd.core.frame.DataFrame, dict]: a pandas dataframe containing metadata of a single experimental unit in the form of dataframe and dictionary

        """
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

    def readExperimentListWithFactors(self,conn)-> pd.core.frame.DataFrame:
        """
        this method reads data from postgre and returns the list of factors abd tratment

        Args:
            conn: Connection parameters

        Returns:
            pd.core.frame.DataFrame: a pandas dataframe containing list of factors abd tratment given 

        """
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


    def readVMGroups(self,conn)-> pd.core.frame.DataFrame:
        """
        this method reads data from postgre a all groups and experimental units within the visuo motor experiment

        Args:
            conn: Connection parameters

        Returns:
            pd.core.frame.DataFrame: a pandas dataframe containing all groups and experimental units within the visuo motor experiment

        """
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
        


    def readPreAutismGroups(self,conn)-> pd.core.frame.DataFrame:
        """
        this method reads data from postgre a all groups and experimental units within the pre-autism experiment

        Args:
            conn: Connection parameters

        Returns:
            pd.core.frame.DataFrame: a pandas dataframe containing all groups and experimental units within the pre-autism experiment

        """
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

class PLotlyFigureGenerator():
    """
    This class generates a plotly Figure based on the condition of experiment chosen in run time
    
    """
    
    def nameToFigureGroup(self,fig_name, connectionParameters):
        """
        
        this method returns a plotly figure for group in the form of table for both Visuomotor functional connectivity and Pre-autism experiment chosen at run-time

        Args:
            fig_name : the condition based on which the figure is selected
            connectionParameters : parameters required to connect to postgres

        Returns:
            Figure: a plotly figure with group data for any one experiment based on the condition chosen at run  time
        """
        figure = go.Figure()
        
        pgr = PostgresReader()
        
        if fig_name == 'Visuomotor functional connectivity':
                experimentListWithGroupsAndSubjectsDF = pgr.readVMGroups(connectionParameters)
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
            experimentListWithGroupsAndSubjectsDF = pgr.readPreAutismGroups(connectionParameters)
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


    def nameToFigureExperimentalUnit(self,fig_name, connectionParameters):
        """
        
        this method returns a plotly figure for Experimental Unit in the form of table for both Visuomotor functional connectivity and Pre-autism experiment chosen at run-time

        Args:
            fig_name : the condition based on which the figure is selected
            connectionParameters : parameters required to connect to postgres

        Returns:
            Figure: a plotly figure with Experimental Unit data for any one experiment based on the condition chosen at run  time
        """
        figure = go.Figure()
        
        if fig_name == 'Visuomotor functional connectivity':
                experimentListWithGroupsAndSubjectsDF = pgr.readVMGroups(connectionParameters)
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
            experimentListWithGroupsAndSubjectsDF = pgr.readPreAutismGroups(connectionParameters)
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
    
    connectionParameters['user']="smd"
    connectionParameters['password']="smd2022"
    connectionParameters['host']="localhost"
    connectionParameters['port']="5432"
    connectionParameters['database']="smdvault"
    
    pgr = PostgresReader()
    pfg = PLotlyFigureGenerator()
    
    
    df = pgr.readObservationDataFromEnterpriseLayer(connectionParameters)
    dflist = df['value'].tolist()
    observationy = np.ravel(dflist).tolist()
    timelist = df['timestamps'].tolist()
    observationx = np.ravel(timelist).tolist()
    
    
    individualBoxPlotData = pgr.readBoxPlotObservationDataFromEnterpriseLayer(connectionParameters)
    individualBoxPlotDataList = np.array(individualBoxPlotData['value'].tolist())
    
    individualBoxPlotDataOne = individualBoxPlotDataList[0,:,0]
    individualBoxPlotDataTwo = individualBoxPlotDataList[0,:,1]
    
    
    individualBoxPlotOne = go.Box(y=individualBoxPlotDataOne, name = "first interval")
    individualBoxPlottwo = go.Box(y=individualBoxPlotDataTwo, name = "second interval")
    
    boxPlotData = [individualBoxPlotOne, individualBoxPlottwo]
    
    
    groupBoxPlotDataRaw = pgr.readBoxPlotGroupObservationDataFromEnterpriseLayer(connectionParameters)
    groupBoxPlotDataList = np.array(groupBoxPlotDataRaw['value'].tolist())
    
    x = ['subject 1 first interval', 'subject 1 second interval', 'subject 2 first interval', 'subject 2 second interval', 'subject 3 first interval', 'subject 3 second interval', 'subject 4 first interval', 'subject 4 second interval', 'subject 5 first interval', 'subject 5 second interval','subject 6 first interval','subject 6 second interval']
    
    groupOneBoxPlotOne = groupBoxPlotDataList[0,:,0]
    groupOneBoxPlotTwo = groupBoxPlotDataList[1,:,0]
    groupOneBoxPlotThree = groupBoxPlotDataList[2,:,0]
    groupOneBoxPlotFour = groupBoxPlotDataList[3,:,0]
    groupOneBoxPlotFive = groupBoxPlotDataList[4,:,0]
    groupOneBoxPlotSix = groupBoxPlotDataList[5,:,0]
    
    groupOneBoxPlotOne = go.Box(y=groupOneBoxPlotOne, name = "subject 1 first interval", fillcolor="Red")
    groupOneBoxPlotTwo = go.Box(y=groupOneBoxPlotTwo, name = 'subject 2 first interval', fillcolor="Red")
    groupOneBoxPlotThree = go.Box(y=groupOneBoxPlotThree, name = 'subject 3 first interval', fillcolor="Red")
    groupOneBoxPlotFour = go.Box(y=groupOneBoxPlotFour, name = 'subject 4 first interval', fillcolor="Red")
    groupOneBoxPlotFive = go.Box(y=groupOneBoxPlotFive, name = 'subject 5 first interval', fillcolor="Red")
    groupOneBoxPlotSix = go.Box(y=groupOneBoxPlotSix, name = 'subject 6 first interval', fillcolor="Red")
    
    groupTwoBoxPlotOne = groupBoxPlotDataList[0,:,1]
    groupTwoBoxPlotTwo = groupBoxPlotDataList[1,:,1]
    groupTwoBoxPlotThree = groupBoxPlotDataList[2,:,1]
    groupTwoBoxPlotFour = groupBoxPlotDataList[3,:,1]
    groupTwoBoxPlotFive = groupBoxPlotDataList[4,:,1]
    groupTwoBoxPlotSix = groupBoxPlotDataList[5,:,1]
    
    groupTwoBoxPlotOne = go.Box(y=groupTwoBoxPlotTwo,name = "subject 1 second interval", fillcolor="Blue")
    groupTwoBoxPlotTwo = go.Box(y=groupTwoBoxPlotTwo,name = "subject 2 second interval", fillcolor="Blue")
    groupTwoBoxPlotThree = go.Box(y=groupTwoBoxPlotThree,name = "subject 3 second interval", fillcolor="Blue")
    groupTwoBoxPlotFour = go.Box(y=groupTwoBoxPlotFour,name = "subject 4 second interval", fillcolor="Blue")
    groupTwoBoxPlotFive = go.Box(y=groupTwoBoxPlotFive,name = "subject 5 second interval", fillcolor="Blue")
    groupTwoBoxPlotSix = go.Box(y=groupTwoBoxPlotSix,name = "subject 6 second interval", fillcolor="Blue")
    
    groupBoxPlotData = [groupTwoBoxPlotOne, groupOneBoxPlotOne, groupTwoBoxPlotTwo, groupOneBoxPlotTwo, groupTwoBoxPlotThree, groupOneBoxPlotThree, groupTwoBoxPlotFour, groupOneBoxPlotFour, groupTwoBoxPlotFive, groupOneBoxPlotFive, groupTwoBoxPlotSix, groupOneBoxPlotSix]
    
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
        return pfg.nameToFigureGroup(fig_name, connectionParameters)
    
    @app.callback(
    dash.dependencies.Output('experimentalUnitsPlot', 'children'),
    [dash.dependencies.Input('fig_dropdown', 'value')])
    def update_output(fig_name):
        return pfg.nameToFigureExperimentalUnit(fig_name, connectionParameters)
    
    experimentListWithFactorsDF = pgr.readExperimentListWithFactors(connectionParameters)
    table = go.Figure(data=[go.Table(
            header=dict(values=list(experimentListWithFactorsDF.columns),
                        fill_color='paleturquoise',
                        align='left'),
            cells=dict(values=experimentListWithFactorsDF.transpose().values.tolist(),
                        fill_color='lavender',
                        align='left'))
        ])
    
    metadataDF, metadataDict = pgr.readKeyvalueFromEnterpriseLayer(connectionParameters)
    metadataTable =  go.Figure(data=[go.Table(
            header=dict(values=list(metadataDF.columns),
                        fill_color='paleturquoise',
                        align='left'),
            cells=dict(values=metadataDF.transpose().values.tolist(),
                        fill_color='lavender',
                        align='left'))
        ])
    
    groupHbo2DF,groupHbR2DF = pgr.readGroupObservationDataFromEnterpriseLayer(connectionParameters)
    
    groupHbo2DFy = groupHbo2DF['value'].tolist()
    
    
    grandAveragedHbO2 = np.empty(np.shape(groupHbo2DFy[0]))
    k = 1
    for value in groupHbo2DFy:
        k = k + 1
        grandAveragedHbO2 = grandAveragedHbO2 + np.array(value)
    
    grandAveragedHbO2 = grandAveragedHbO2/k
    
    grandAveragedHbO2DF = pd.DataFrame(grandAveragedHbO2, columns = ['CH1','CH2','CH3','CH4','CH5','CH6','CH7','CH8','CH9','CH10','CH11','CH12','CH13','CH14','CH15','CH16','CH17','CH18','CH19','CH20','CH21','CH22','CH23','CH24'])
    
    groupHbRDFy = groupHbR2DF['value'].tolist()
    
    
    grandAveragedHbR = np.empty(np.shape(groupHbRDFy[0]))
    k = 1
    for value in groupHbRDFy:
        k = k + 1
        grandAveragedHbR = grandAveragedHbR + np.array(value)
    
    grandAveragedHbR = grandAveragedHbR/k
    
    grandAveragedHbRDF = pd.DataFrame(grandAveragedHbR, columns = ['CH1','CH2','CH3','CH4','CH5','CH6','CH7','CH8','CH9','CH10','CH11','CH12','CH13','CH14','CH15','CH16','CH17','CH18','CH19','CH20','CH21','CH22','CH23','CH24'])
    
    
    def getTimestamps(startTime, shape, samplingRate) -> list:
        """
        gives a list of timestamps starting from srart time in increments of sampling rate

        Args:
            startTime : initial time
            shape : the length of array to be created
            samplingRate : duration between each element of array

        Returns:
            list: a list of timestamps
            
        Example:
        >>> getTimestamps(2022-10-01 00:00:00, 4, 0.5)
        [2022-10-01 00:00:00.000, 2022-10-01 00:00:00.500, 2022-10-01 00:00:01.000, 2022-10-01 00:00:01.500]
        """
            
        startDateTime = dt.datetime.strptime(startTime,'%H:%M:%S.%f')
        sampleNumbers = np.arange(shape)
        samplingRate = float(samplingRate)
        
        timestamps = []
        for sampleNumber in sampleNumbers:
            timestamps.append((startDateTime + dt.timedelta(seconds= samplingRate * sampleNumber)))
        return timestamps

    
    timelineHbo2 = getTimestamps('00:00:00.000', len(groupHbo2DFy[0]), 0.1)
    timelineHbR = getTimestamps('00:00:00.000', len(groupHbRDFy[0]), 0.1)
    
    
    grandAveragedHbO2Res = []
    for col in grandAveragedHbO2DF.columns:
        grandAveragedHbO2Res.append(
            go.Line(
                x=timelineHbo2,
                y=grandAveragedHbO2DF[col].values.tolist(),
                name=col
            )
        )

    grandAveragedHbO2Layout = go.Layout(
        title='Line Plot of oxy-hemoglobin (HbO2) data for all channels belonging to Visuo Motor group',
        xaxis={'title': 'timestamps'},
        yaxis={'title': 'oxy-hemoglobin (HbO2)'} 
    )
    grandAveragedHbO2Fig = go.Figure(data=grandAveragedHbO2Res, layout = grandAveragedHbO2Layout)

    
    
    grandAveragedHbRRes = []
    for col in grandAveragedHbRDF.columns:
        grandAveragedHbRRes.append(
            go.Line(
                x=timelineHbo2,
                y=grandAveragedHbRDF[col].values.tolist(),
                name=col
            )
        )

    grandAveragedHbRLayout = go.Layout(
        title='Line Plot of deoxy-hemoglobin (HbR) data for all channels belonging to Visuo Motor group',
        xaxis={'title': 'timestamps'},
        yaxis={'title': 'deoxy-hemoglobin (HbR)'} 
    )
    grandAveragedHbRFig = go.Figure(data=grandAveragedHbRRes, layout = grandAveragedHbRLayout)

    
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
                    {'y': observationy,
                    'x': observationx,
                    'type': 'line',
                    }
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
        html.Div(children='Grand averaged per channel timecourse pictorial representation of the HbO2 and HbR for a group with dispersion regions.',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        dcc.Graph(figure = grandAveragedHbO2Fig),
        dcc.Graph(figure = grandAveragedHbRFig),
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
        html.Br(),
        html.Div(children='A boxplot comparing the distribution of either HbO2 or HbR concentrations for two intervals of time for a subject',
                style={'textAlign': 'center',
                    'color': '#000205'}),
        dcc.Graph(figure ={'data': boxPlotData, 
                           'layout': go.Layout(title='Box Plot of deoxy-hemoglobin (HbR) data for VM0001 for two intervals'
                )}),
        html.Br(),
        html.Br(),
        html.Div(children='A boxplot comparing the distribution of either HbO2 or HbR concentrations for two intervals of time for a group.',
                style={'textAlign': 'center',
                    'color': '#000205'}),
                dcc.Graph(figure ={'data': groupBoxPlotData, 
                           'layout': go.Layout(title='Box Plot of deoxy-hemoglobin (HbR) data for Visuo Motor for two intervals'
                )}),
        html.Br(),
        html.Br()
        ])

 


    app.run_server(port=4050)