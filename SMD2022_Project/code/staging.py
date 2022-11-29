import os
import glob
import datetime as dt
from dateutil import parser
import pandas as pd
import numpy as np
import psycopg2, pickle
from psycopg2 import Error
import pathlib


"""

Staging
--------------------------------------

This python file is the staging layer of Data Vault implementation to store and mine medical imaging data
The Staging layer consists of ETL process that takes in raw data and loads it into posgres tables.

ETL process includes
a. extract stage - the phase where raw data is read from multiple data sources to staging area
b. transform stage - the phase where data is transformed using analytical processes and broken up into chunks that could be stored in Enterprise layer
c. loading stage - the phase where the transformed data is loaded into intended database (postgres)

"""
class FileReader():  
    """
    FileReader reads the data from multiple source stystem and provides the data to next stage.
    
    This class plays the role of extract stage in ETL process that read's Pre-autism dataset and Visuomotor functional connectivity
    and prepares the data for transformation stage.
    The raw data needs to be present in a folder called VMData_Blinded for Visuomotor data and PreAutismData_Blinded for Pre-autism
    
    """
    def __init__(self):
        """
        this constructor initiates a private filename to empty string
        """
        self.__filename = ""
    
    
    

    def _findField(self,file, fieldName, allLines = []) -> str:
        """
        
        finds the fields from the given input and returns a line in string format if the key is available in file else returns an empty string

        Args:
            file : input file to be processed
            fieldName : name of key to search in file
            allLines : an array of line string, each element corresponds to a single line in the file. Defaults to [].

        Returns:
            str: a line in string form containing the fieldname that is being searched int he file, empty when field name is not found
        
        Example: 
        file
            ID,VM0010_Viso \n
            Name,Subj010_ \n
            Age,  0y \n
        
            
        >>> _findField(file, 'ID')
        ,VM0010_Viso
        >>> _findField(file, 'address)
        ""
        """
        if not allLines:
            allLines = file.readlines()
             
        
        for currentLine in allLines:
            if fieldName in currentLine:
                currentLine = currentLine.split(fieldName)
                return  ''.join(currentLine[1])
        
        return ""
    
        
    def _findFieldPosition(self,file, fieldName) -> None:
        """
        
        finds the position of the field given input key and does not return anything.

        Args:
            file : input file to be processed
            fieldName : name of key to search in file

        Returns:
            None: position of the field key
        """
        currentLine = file.readline()
        if fieldName in currentLine:
            return  None
        else:
            return self._findFieldPosition(file, fieldName)

        
    def readVMFile(self, file) -> tuple[dict, pd.core.frame.DataFrame]:
        """
        
        reads a file for Visuomotor dataset and returns the dataset and metadata from the file
        
        The whole file is taken as input, the fields to be found are provided in the function and it calls _findField function to retrieve values for each field
        After all the metadata values are read, the data is contained below metadata, it is read as a pandas dataframe by finding the position of data by calling _findFieldPosition

        Args:
            file : input file to be processed

        Returns:
            tuple[dict, pd.core.frame.DataFrame]: a tuple consisting a dictionary of key, value pairs and a pandas dataframe of data
            
        Example: 
        Sources=12 \n
        Detectors=8 \n
        ShortBundles=0 \n
        
        data
        col1,col2 \n
        1,3 \n
        2,4 \n
        
        >>> metadata, df = readVMFile(file)
        >>> metadata
        {Sources: '12', Detectors: '8', ShortBundles: '0'}
        >>> df
           col1  col2
        0     1     3
        1     2     4
        """
        metadata = {}
        fields = ['ID','Name','Age','Sex','AnalyzeMode','Pre Time[s]','Post Time[s]','Recovery Time[s]','Base Time[s]','Date','Mode','Wave[nm]','Sampling Period[s]','StimType','Stim Time[s]','Repeat Count']
        for field in fields:
            if 'Stim Time[s]' != field:
                if 'Wave[nm]' != field:
                    metadata[field] = self._findField(file, field).replace('\n','').lstrip(',').rstrip(',')
                    file.seek(0)
                else:    
                    metadata[field] = self._findField(file, field).replace('\n','').lstrip(',').rstrip(',').split(',')
                    file.seek(0)
            else:
                self._findFieldPosition(file, field)
                fieldList = file.readline().replace('\n','').replace(',,','').split(",")
                file.seek(0)
                fieldDict ={}
                for i in range(len(fieldList) - 1):
                    if i % 2 == 0:
                        fieldDict[fieldList[i]] = fieldList[i+1]
                
                metadata[field] = fieldDict

        self._findFieldPosition(file,'Data')
        data = pd.read_csv(file)
        
        return (metadata,data)
    
    def getParameters(self, file, field, params) -> dict:
        """
        
        finds the parameter from the given key and retuens all the nested keys from the file

        Args:
            file : input file to be processed
            field : : name of key to search in file
            params : a list of parameters to be retrieved from the field key

        Returns:
            dict: a dictionary of key, value pairs
            
            
        Example:
        [ImagingParameters] \n
        Sources=12 \n
        Detectors=8 \n
        ShortBundles=0 \n
        
        >>> getParameters(file, 'ImagingParameters', [Sources, Detectors, ShortBundles])
        {Sources: '12', Detectors: '8', ShortBundles: '0'}
        """
        parameters = {}
        self._findFieldPosition(file, field)
        headerLines = file.readlines()   
        for param in params:
            parameters[param] = self._findField(file, param, headerLines).replace('\n','').replace('\"','').replace('=','').replace('\t',',')
        
        file.seek(0)
        return parameters

    
    def getArray(self, file, field, params, arrayFields) -> dict:
        """
        
        reads an array from the field seperated by # and returns a dictionary having key as string and 2D arrayas value

        Args:
            file : input file to be processed
            field : : name of key to search in file
            params : a list of parameters to be retrieved from the field key
            arrayFields : a list of arrays to be retrieved for the key

        Returns:
            dict: a dictionary of key, value pairs, values are 2D arrays for arrayFields keys
        
        
        Example:
        
        [GainSettings] \n
        Sources=12 \n
        Detectors=8 \n
        Gains="# \n
        6	7	\n
        5	5	\n
        #"
           
        >>> getArray(file, 'GainSettings',[Sources, Detectors],[Gains])
        {Sources:'12',Detectors:'8',Gains:[[6, 7],[5, 5]]}
        """
        fieldArray = {}
        fieldArray = self.getParameters(file, field, params)
        
        for fieldValue in arrayFields:
            self._findFieldPosition(file, fieldValue)
            arrayValue = np.array([])
            currentLine = file.readline().replace('\n','')
            while "#" not in currentLine:
                currentLine = currentLine.split('\t')
                if arrayValue.shape[0] == 0:
                    arrayValue = np.array(currentLine)
                else:
                    arrayValue = np.vstack((arrayValue, currentLine))
                currentLine = file.readline().replace('\n','')
            fieldArray[fieldValue] = arrayValue
            
        
        return fieldArray
    
    
    def readPreAutismMetaData(self, file) -> dict:
        """
        
         reads a file for PreAutismData dataset and returns the metadata from the file

        Args:
            file : input file to be processed

        Returns:
            dict: a dictionary of all metadata in the file in the form of key, value
            
            
        Example:
        
        [GainSettings] \n
        Sources=12 \n
        Detectors=8 \n
        Gains="# \n
        6	7	\n
        5	5	\n
        #"
        
        [ImagingParameters]
        Sources=12 \n
        Detectors=8 \n
        ShortBundles=0 \n
        
        >>> readPreAutismMetaData(file)
        {GainSettings: {Sources:'12',Detectors:'8',Gains:[[6, 7],[5, 5]]},ImagingParameters: {Sources: '12', Detectors: '8', ShortBundles: '0'}}       
        """
        
        metadata = {}
        
        GeneralInfoFields = ['FileName','Date','Time','Device','Source','Mod','APD','NIRStar','Subject']
        ImagingParametersFields = ['Sources','Detectors','ShortDetectors','ShortBundles','ShortDetIndex','Steps','Wavelengths','TrigIns','TrigOuts','AnIns','SamplingRate','Mod Amp','Threshold']
        ParadigmFields = ['StimulusType']
        ExperimentNotesFields = ['Notes']
        GainSettingsFields = []
        GainSettingsArrayFields = ['Gains']
        MarkersFields = []
        MarkersArrayFields = ['Events']
        DataStructureFields = ['S-D-Key']
        DataStructureArrayFields = ['S-D-Mask']
        DarkNoiseFields = []
        DarkNoiseArrayFields = ['Wavelength1','Wavelength2']
        ChannelsDistanceFields = ['ChanDis']
        
            
        metadata["GeneralInfo"] = self.getParameters(file, "GeneralInfo", GeneralInfoFields)
        metadata["ImagingParameters"] = self.getParameters(file, "ImagingParameters", ImagingParametersFields)
        metadata["Paradigm"] = self.getParameters(file, "Paradigm", ParadigmFields)
        metadata["ExperimentNotes"] = self.getParameters(file, "ExperimentNotes", ExperimentNotesFields)
        metadata["GainSettings"] = self.getArray(file, "GainSettings", GainSettingsFields, GainSettingsArrayFields)
        metadata["Markers"] = self.getArray(file, "Markers", MarkersFields, MarkersArrayFields)
        metadata["DataStructure"] = self.getArray(file, "DataStructure", DataStructureFields, DataStructureArrayFields)
        metadata["DarkNoise"] = self.getArray(file, "DarkNoise", DarkNoiseFields, DarkNoiseArrayFields)
        metadata["ChannelsDistance"] = self.getParameters(file, "ChannelsDistance", ChannelsDistanceFields)
        
        return metadata    


class FileTransformer():
    """
    FileTransformer reads the data from extract stage, transforms it and sends to loading stage 
    
    This class uses analytic tools to transform the data such as filtering, segregate the data into different dataframes
    according to tables in Enterprise layer and sends it to Loading stage for data to be inserted. Transformations are done on the 
    raw data collected at extract stage and includes transformations for Pre-autism dataset and Visuomotor functional connectivity.
    The final output from transform stage are in the form of a dictionary with keys specific to the tables and values 
    containg the dataframes to be inseted at load stage in Enterprise data vault.
    
    """
    
    def __init__(self):
        """
        this constructor initiates a private filename to empty string
        """
        self.__filename = ""
        
    def transformVMFile(self, fileName, metaData, data) -> dict:
        """
        reads the Visuomotor data received from extract stage and transforms the data into individidual dataframes to be loaded into enterprise data warehouse

        Args:
            fileName : an array of filenames in the directory to be transformed
            metaData : an array of dictionary with metadata ijn the key, value pairs
            data : an array of raw dataframes to be transformed

        Returns:
            dict: a dictionary with keys of table names and values containing the dataframe to be loaded in respective key tables
        """
        transformData = {}
        dates = []
        keys = []
        values = []
        samplePeriods = []
            
        # retrieve date, sampling period, metadata keys and metadata values from all the files and place it in individual arrays
        for meta in metaData:
            dates.append(meta['Date'])
            samplePeriods.append(meta['Sampling Period[s]'])
            keys.append(list(meta.keys()))
            values.append(list(meta.values()))

        # create a pandas dataframe with date and samplePeriod, the date column contains the "Date" field from metadata that will be our start time for experiment
        dateSamplingTimeDF = pd.DataFrame({'date': dates, 'samplePeriod': samplePeriods})
        # retrieve samplePeriod as a float datatype which will be used as incremental value to add on to start time for each sample taken
        dateSamplingTimeDF['samplePeriod'] = dateSamplingTimeDF['samplePeriod'].astype(float)
        # reformat the data field column to date time object in the input format to perfoem date time operations
        dateSamplingTimeDF['date'] =  pd.to_datetime(dateSamplingTimeDF['date'], format="%d/%m/%Y %H:%M:%S")
        
        experimentTitle=[]
        acronym=[]
        
        # for all the files in a directory read from, retrieve experiment title as the file name and acronym which is short form of filename
        for file in fileName:
            acro = file.split('_')
            experimentTitle.append(file.replace('.csv',''))
            acronym.append(acro[1])
        
        
        # create a dataframe to be inserted in SatMetaDataKeyValuePair Satellite table, each of the keys are placed as individual records along with their respective values
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify metadata in SatMetaDataKeyValuePair table
        SatMetaDataKeyValuePairDF = pd.DataFrame([])
        SatMetaDataKeyValuePairDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatMetaDataKeyValuePairDF['key'] = pd.Series(keys)
        SatMetaDataKeyValuePairDF['value'] = pd.Series(values)
        SatMetaDataKeyValuePairDF = SatMetaDataKeyValuePairDF.set_index(['sequence']).apply(pd.Series.explode).reset_index()
        SatMetaDataKeyValuePairDF['value'] =  SatMetaDataKeyValuePairDF['value'].apply(lambda x: pickle.dumps(x))
        
        transformData['SatMetaDataKeyValuePair'] = SatMetaDataKeyValuePairDF

        # create a dataframe to be inserted in HubMetaData hub table, the sequence in this table is created such that it references SatMetaDataKeyValuePair
        HubMetaDataDF = pd.DataFrame([])
        HubMetaDataDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))

        transformData['HubMetaData'] = HubMetaDataDF


        # create a dataframe to be inserted in SatExperimentTitle Satellite table, the title attribute contains the title of experiment retrieved from file names
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify title in SatExperimentTitle table    
        SatExperimentTitleDF = pd.DataFrame([])
        SatExperimentTitleDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatExperimentTitleDF['title']= pd.Series(experimentTitle)

        transformData['SatExperimentTitle'] = SatExperimentTitleDF
        
        # create a dataframe to be inserted in SatExperimentAcronym Satellite table, the acronym attribute contains the acronym which is a shor form of filename
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify acronym in SatExperimentAcronym table            
        SatExperimentAcronymDF = pd.DataFrame([])
        SatExperimentAcronymDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatExperimentAcronymDF['acronym'] = pd.Series(acronym)

        transformData['SatExperimentAcronym'] = SatExperimentAcronymDF
        
        # create a dataframe to be inserted in HubExperiment hub table, the sequence in this table is created such that it references SatExperimentTitle and SatExperimentAcronym table records uniquely
        HubExperimentDF = pd.DataFrame([])
        HubExperimentDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubExperiment'] = HubExperimentDF

        # create a dataframe to be inserted in HubExperimentalUnit hub table, the sequence in this table is created such that it references SatExperimentalUnitIdentifier table records uniquely        
        HubExperimentalUnitDF = pd.DataFrame([])
        HubExperimentalUnitDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubExperimentalUnit'] = HubExperimentalUnitDF
        
        identities=[]
        
        # retrieve ID's of all subjects in the array from metadata
        for meta in metaData:
            identities.append(meta['ID'])
        
        # create a dataframe to be inserted in SatExperimentalUnitIdentifier Satellite table, the ID attribute contains the unique identifier for each experimental unit        
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify ID in SatExperimentalUnitIdentifier table            
        SatExperimentalUnitIdentifierDF = pd.DataFrame([])
        SatExperimentalUnitIdentifierDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatExperimentalUnitIdentifierDF['ID'] = pd.Series(identities)

        transformData['SatExperimentalUnitIdentifier'] = SatExperimentalUnitIdentifierDF
        
        names=[]
        
        # retrieve names of all subjects in the array from metadata
        for meta in metaData:
            names.append(meta['Name'])
        
        # create a dataframe to be inserted in HubSubject Hub table, the name attribute contains the name for each subject taking part in experiment
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify SatSubjectAge and SatSubjectName satellite table             
        HubSubjectDF = pd.DataFrame([])
        HubSubjectDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubSubjectDF['name'] = pd.Series(names)
        
        transformData['HubSubject'] = HubSubjectDF
        
        
        ages=[]
        
        # retrieve ages in integer format of all subjects in the array from metadata
        for meta in metaData:
            ages.append(int(meta['Age'].lstrip().rstrip().replace('y',''))) 
        
        
        # create a dataframe to be inserted in SatSubjectAge Satellite table, the age attribute contains the age for each subject taking part in experiment
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify age in SatSubjectAge satellite table              
        SatSubjectAgeDF = pd.DataFrame([])
        SatSubjectAgeDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatSubjectAgeDF['age'] = pd.Series(ages)

        transformData['SatSubjectAge'] = SatSubjectAgeDF
        
        
        # create a dataframe to be inserted in SatSubjectName Satellite table, the name attribute contains the age for each subject taking part in experiment
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify name in SatSubjectName satellite table              
        SatSubjectNameDF = pd.DataFrame([])
        SatSubjectNameDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatSubjectNameDF['name'] = pd.Series(names)

        transformData['SatSubjectName'] = SatSubjectNameDF
        
        # create a dataframe to be inserted in ParticipatesIn link table, ParticipatesIn links HubExperimentalUnit and HubExperiment
        ParticipatesInDF = pd.DataFrame([])
        ParticipatesInDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ParticipatesInDF['experimentalunit'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ParticipatesInDF['experiment'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['ParticipatesIn'] = ParticipatesInDF
        
        
        # create a dataframe to be inserted in SatFactorName and SatFactorLevel Satellite table
        # there are two factors Visual Stimulus,Motor Stimulus in VisuoMotor data, these are the factor names
        # the level values are defined by presence or absence of the factors treated in the particular experiment
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify title in SatSubjectName satellite table
        SatFactorDF = pd.DataFrame([])
        SatFactorDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatFactorDF['experiment'] = SatFactorDF['sequence']
        SatFactorDF['isCofactor'] = pd.Series([False for x in range(len(SatFactorDF.index))])
        SatFactorDF['name'] = pd.Series([list(['Visual Stimulus','Motor Stimulus']) for x in range(len(SatFactorDF.index))])
        SatFactorDF = pd.merge(SatFactorDF, SatExperimentAcronymDF, how = 'inner', left_on='sequence', right_on='sequence')
        
        levelValueconditions = [
            (SatFactorDF['acronym'] == "ViMo"),
            (SatFactorDF['acronym'] == "Viso"),
            (SatFactorDF['acronym'] == "Moto"),
            (SatFactorDF['acronym'] == "Rest")
        ]
        levelValues = ["True,True","True,False","False,True","False,False"]
        SatFactorDF['levelValue'] = np.select(levelValueconditions, levelValues)
        SatFactorDF = SatFactorDF[['sequence','name','levelValue','experiment','isCofactor']]
        SatFactorDF['levelValue'] = SatFactorDF['levelValue'].apply(lambda x: x.split(','))
        SatFactorDF = SatFactorDF.set_index(['sequence','experiment','isCofactor']).apply(pd.Series.explode).reset_index()
        
        # a combination of dates, experimentTitle and factor are chosen as a sequence to uniquely identify SatFactorName and SatFactorLevel satellite tables              
        SatFactorDF['sequence'] = SatFactorDF['sequence'] + SatFactorDF['name']
        HubFactorDF = SatFactorDF[['sequence','experiment','isCofactor']]
        
        transformData['HubFactor'] = HubFactorDF
        
        SatFactorNameDF = SatFactorDF[['sequence','name']]

        transformData['SatFactorName'] = SatFactorNameDF
        
        SatFactorLevelDF = SatFactorDF[['sequence','levelValue']]
        
        transformData['SatFactorLevel'] = SatFactorLevelDF
        
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify HubTreatment Hub table and HubExperiment         
        HubTreatmentDF = pd.DataFrame([])
        HubTreatmentDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubTreatmentDF['experiment'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubTreatment'] = HubTreatmentDF
        
        # create a dataframe to be inserted in SatTreatmentFactorLevel Satellite table, the factor level is linked to SatFactorLevel satellite table from HubFactor          
        SatTreatmentFactorLevelDF = SatFactorDF[['sequence','experiment']]
        
        transformData['SatTreatmentFactorLevel'] = SatTreatmentFactorLevelDF
        
        # create a dataframe to be inserted in HubGroup Hub table, the sequence in this table is created such that it references SatGroupName table records uniquely
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify SatSubjectAge and SatSubjectName satellite table
        HubGroupDF = pd.DataFrame([])
        HubGroupDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubGroupDF['treatment'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubGroup'] = HubGroupDF
        
        
        # create a dataframe to be inserted in AssignedTo link table, AssignedTo links HubExperimentalUnit and HubGroup
        AssignedToDF= pd.DataFrame([])
        AssignedToDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AssignedToDF['experimentalUnit'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AssignedToDF['group'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['AssignedTo'] = AssignedToDF
        
        # create a dataframe to be inserted in SatGroupName Satellite table, the name attribute contains the group that each subject belongs to within an experiment
        levelNames = ['Visual and Motor Stimulus','Visual Stimulus','Motor Stimulus','Rest']
        SatGroupNameDF = SatExperimentAcronymDF[['sequence','acronym']]
        SatGroupNameDF['name'] = np.select(levelValueconditions, levelNames)
        SatGroupNameDF = SatGroupNameDF[['sequence','name']]

        transformData['SatGroupName'] =SatGroupNameDF
        
        # create a dataframe to be inserted in SatSessionName Satellite table, the name attribute contains the session that each subject attends during the course of an experiment
        SatSessionNameDF = SatGroupNameDF
        
        transformData['SatSessionName'] = SatSessionNameDF
        
        # create a dataframe to be inserted in HubSession Hub table, the sequence in this table is created such that it references SatSessionName table records uniquely
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify SatSessionName satellite table
        HubSessionDF = pd.DataFrame([])
        HubSessionDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubSession'] = HubSessionDF
        
        # create a dataframe to be inserted in AttendsSession link table, AttendsSession links HubExperimentalUnit, HubGroup and HubSession
        AttendsSessionDF = pd.DataFrame([])
        AttendsSessionDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AttendsSessionDF['experimentalUnit'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AttendsSessionDF['group'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AttendsSessionDF['session'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['AttendsSession'] = AttendsSessionDF
        
        
        # create a dataframe to be inserted in SessionMetaData link table, SessionMetaData links HubSession and HubMetaData
        SessionMetaDataDF = pd.DataFrame([])
        SessionMetaDataDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SessionMetaDataDF['session'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SessionMetaDataDF['metadata'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['SessionMetaData'] = SessionMetaDataDF
        
        # create a dataframe to be inserted in HubObservation Hub table, the sequence in this table is created such that it references SatObservationName and SatObservationValue table records uniquely
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify SatObservationName and SatObservationValue satellite table
        HubObservationDF = pd.DataFrame([])
        HubObservationDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubObservationDF['collectedAtSession'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubObservation'] = HubObservationDF
        
        # create a dataframe to be inserted in ObservationMetaData link table, ObservationMetaData links HubObservation and HubMetaData
        ObservationMetaDataDF = pd.DataFrame([])
        ObservationMetaDataDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ObservationMetaDataDF['observation'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ObservationMetaDataDF['metadata'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['ObservationMetaData'] = ObservationMetaDataDF
        
        
        # create a dataframe to be inserted in SatObservationName Satellite table, the name attribute uniquely identifies the observation name in the experiment and set to experimrnt title that is derived from file name
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify name in SatObservationName table
        SatObservationNameDF = pd.DataFrame([])
        SatObservationNameDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatObservationNameDF['name'] = pd.Series(experimentTitle)

        transformData['SatObservationName'] = SatObservationNameDF
        
        arrayData=[]
        timestampData=[]
        for dataValue, date, samplingRate in zip(data, dateSamplingTimeDF['date'].tolist(), dateSamplingTimeDF['samplePeriod'].tolist()):
            if 'MES' in fileName[0]:
                dataDF = dataValue
                dataDF['date'] = date
                dataDF['date'] = dataDF['date'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                dataDF['samplingRate'] = samplingRate
                dataDF['sampleNumber'] = np.arange(dataDF.shape[0])
                dataDF['timestamps'] = pd.to_datetime(dataDF['date']) + pd.to_timedelta((dataDF['samplingRate'] * dataDF['sampleNumber']), unit='s')    # .apply(lambda x: x + datetime.timedelta(seconds = x['samplingRate'] * x['sampleNumber']))
                dataDF['timestamps']=dataDF['timestamps'].astype(str)
               
                timeData = dataDF['timestamps'].tolist()
                
                timeDataFormatted = []
                for t in timeData:
                    timeDataFormatted.append(dt.datetime.strptime(t,'%Y-%m-%d %H:%M:%S.%f'))
                
                arrayData.append(dataValue.loc[:,['CH1(698.1)','CH1(828.7)','CH2(697.1)','CH2(828.2)','CH3(698.1)','CH3(828.7)','CH4(698.3)','CH4(828.4)','CH5(697.1)','CH5(828.2)','CH6(698.3)','CH6(828.4)','CH7(698.3)','CH7(828.4)','CH8(697.5)','CH8(828.7)','CH9(698.3)','CH9(828.4)','CH10(697.9)','CH10(829.0)','CH11(697.5)','CH11(828.7)','CH12(697.9)','CH12(829.0)','CH13(698.7)','CH13(828.2)','CH14(698.2)','CH14(827.5)','CH15(698.7)','CH15(828.2)','CH16(697.7)','CH16(828.6)','CH17(698.2)','CH17(827.5)','CH18(697.7)','CH18(828.6)','CH19(697.7)','CH19(828.6)','CH20(698.4)','CH20(828.9)','CH21(697.7)','CH21(828.6)','CH22(697.1)','CH22(828.8)','CH23(698.4)','CH23(828.9)','CH24(697.1)','CH24(828.8)']].values.tolist())
                timestampData.append(timeData)
            else:
                dataDF = dataValue
                dataDF['date'] = date
                dataDF['date'] = dataDF['date'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                dataDF['samplingRate'] = samplingRate
                dataDF['sampleNumber'] = np.arange(dataDF.shape[0])
                dataDF['timestamps'] = pd.to_datetime(dataDF['date']) + pd.to_timedelta((dataDF['samplingRate'] * dataDF['sampleNumber']), unit='s')
                dataDF['timestamps']=dataDF['timestamps'].astype(str)
                
                timeData = dataDF['timestamps'].tolist()
                
                timeDataFormatted = []
                for t in timeData:
                    timeDataFormatted.append(dt.datetime.strptime(t,'%Y-%m-%d %H:%M:%S.%f'))
                
                arrayData.append(dataValue.loc[:,['CH1','CH2','CH3','CH4','CH5','CH6','CH7','CH8','CH9','CH10','CH11','CH12','CH13','CH14','CH15','CH16','CH17','CH18','CH19','CH20','CH21','CH22','CH23','CH24']].values.tolist())
                timestampData.append(timeData)
        
        
        # create a dataframe to be inserted in SatObservationValue Satellite table
        # the value attribute contains a 2D array representaion of data in each of the observation
        # the timestamps contain the timestamp during the observation in steps of sampling rate, this is 1D array representaion of time when the observation value was captured 
        # a combination of dates and experimentTitle are chosen as a sequence to uniquely identify value and timestamps in SatObservationValue table
        SatObservationValueDF = pd.DataFrame([])
        SatObservationValueDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatObservationValueDF['value'] = pd.Series(arrayData)
        SatObservationValueDF['timestamps'] = pd.Series(timestampData)

        transformData['SatObservationValue'] = SatObservationValueDF
        
        return transformData
    
    
    def transformPreAutismFile(self,preAutismFileNames, preAutismMetaData, preAutismData, preAutismWavelengthOneData, preAutismWavelengthTwoData, preAutismEventonsData) -> dict:
        """
        
        reads the Pre-autism data received from extract stage and transforms the data into individidual dataframes to be loaded into enterprise data warehouse
        

        Args:
            preAutismFileNames : an array of filenames to be transformed
            preAutismMetaData : an array of metadata for all files in the folder
            preAutismData : an array of .dat files from Normal and Stressed conversations in pre-autism data
            preAutismWavelengthOneData : an array of .wl1 files from Normal and Stressed conversations in pre-autism data
            preAutismWavelengthTwoData : an array of .wl2 files from Normal and Stressed conversations in pre-autism data
            preAutismEventonsData : an array of .evt files from Normal and Stressed conversations in pre-autism data

        Returns:
            dict: a dictionary with keys of table names and values containing the dataframe to be loaded in respective key tables

        """
        
        transformData = {}

        
        def nestedKeyValue(dictionary):
            """
            
            takes an input dictionary with nested key value pairs and returns a list of keys and values in a an unnested form 

            Args:
                dictionary : a dictionary of key, value pairs in nested form 

            Yields:
                (key, value): a tuple containg key and vaule pair
                
            Example:
            
            >>> nestedKeyValue({GainSettings: {Sources:'12',Detectors:'8',Gains:[[6, 7],[5, 5]]},ImagingParameters: {TrigIns: '0', TrigOuts: '0', AnIns: '4'}})  
            (Sources,12)
            (Detectors,8)
            (Gains,[[6, 7],[5, 5]])
            (TrigIns,4)
            (TrigOuts,0)
            (AnIns,0)
            """
            for key, value in dictionary.items():
                if type(value) is dict:
                     yield from nestedKeyValue(value)
                else:
                    yield (key, value)

        def getKeyArrays(input):
            """
            
            gives the keys from a dictionary in an array format

            Args:
                input: a dictionary of key, value pairs in nested form 

            Returns:
                list: a list of keys in a nested dictionary
             
             Example:
                
            >>> nestedKeyValue({GainSettings: {Sources:'12',Detectors:'8',Gains:[[6, 7],[5, 5]]},ImagingParameters: {TrigIns: '0', TrigOuts: '0', AnIns: '4'}})  
            [Sources,Detectors,Gains,TrigIns,TrigOuts,AnIns]
            """
            keyArray = []
            x = input
            for key, value in nestedKeyValue(x):
                keyArray.append(key)
            
            return keyArray

        def getValueArrays(input):
            """
            
            gives the values from a dictionary in an array format

            Args:
                input : a dictionary of key, value pairs in nested form 

            Returns:
                list: a list of values in a nested dictionary
            
            Example:
            
            >>> nestedKeyValue({GainSettings: {Sources:'12',Detectors:'8',Gains:[[6, 7],[5, 5]]},ImagingParameters: {TrigIns: '0', TrigOuts: '0', AnIns: '4'}})  
            [12,8,[[6, 7],[5, 5]],4,0,0]
            """
            valueArray =[]
            x = input
            for key, value in nestedKeyValue(x):
                valueArray.append(value)
            
            return valueArray
        
        
        fileName = []
        date = []
        time = []
        samplingRates = []
    
        simplifiedMetaDataList =[]
        
        # read the metadata received from extract and unwrap into key, value if the metadata dictionary is in nested form 
        for metaData in preAutismMetaData:
            simplifiedMetaData = {}
            for key, value in nestedKeyValue(metaData):
                simplifiedMetaData[key] = value
            simplifiedMetaDataList.append(simplifiedMetaData)
        
        # retrieve date, sampling period, time and file names from all the files and place it in individual arrays
        for meta in simplifiedMetaDataList:
            fileName.append(meta['FileName'])
            date.append(meta['Date'])
            time.append(meta['Time'])
            samplingRates.append(meta['SamplingRate'])
        
        
        # create a dataframe from all the values retrieved and add the .dat, .wl1, .wl2 and .evt data
        preAutismDF = pd.DataFrame([])
        preAutismDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        preAutismDF['preAutismFileNames'] = pd.Series(preAutismFileNames)
        preAutismDF['preAutismMetaData'] = pd.Series(preAutismMetaData)
        preAutismDF['preAutismData'] = pd.Series(preAutismData)
        preAutismDF['preAutismWavelengthOneData'] = pd.Series(preAutismWavelengthOneData)
        preAutismDF['preAutismWavelengthTwoData'] = pd.Series(preAutismWavelengthTwoData)
        preAutismDF['preAutismEventonsData'] = pd.Series(preAutismEventonsData)
        
        # create a dataframe to be inserted in SatMetaDataKeyValuePair Satellite table, each of the keys are placed as individual records along with their respective values
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify metadata in SatMetaDataKeyValuePair table
        SatMetaDataKeyValuePairDF = pd.DataFrame([])
        SatMetaDataKeyValuePairDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatMetaDataKeyValuePairDF['key'] = preAutismDF['preAutismMetaData'].apply(lambda x: getKeyArrays(x))
        SatMetaDataKeyValuePairDF['value'] = preAutismDF['preAutismMetaData'].apply(lambda x: getValueArrays(x))
        SatMetaDataKeyValuePairDF = SatMetaDataKeyValuePairDF[['sequence','key','value']] 
        SatMetaDataKeyValuePairDF = SatMetaDataKeyValuePairDF.set_index(['sequence']).apply(pd.Series.explode).reset_index()
        SatMetaDataKeyValuePairDF['value'] = SatMetaDataKeyValuePairDF['value'].apply(lambda x: pickle.dumps(x))

        transformData['SatMetaDataKeyValuePair'] = SatMetaDataKeyValuePairDF

        # create a dataframe to be inserted in HubMetaData hub table, the sequence in this table is created such that it references SatMetaDataKeyValuePair
        HubMetaDataDF = pd.DataFrame([])
        HubMetaDataDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubMetaData'] = HubMetaDataDF
     
        # create a dataframe to be inserted in SatExperimentTitle Satellite table, the title attribute contains the title of experiment retrieved from file names
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify title in SatExperimentTitle table 
        SatExperimentTitleDF = pd.DataFrame([])
        SatExperimentTitleDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatExperimentTitleDF['title']= preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[1].replace('.hdr',''))

        transformData['SatExperimentTitle'] = SatExperimentTitleDF
        
        # create a dataframe to be inserted in SatExperimentAcronym Satellite table, the acronym attribute contains the acronym which is a short form of filename
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify acronym in SatExperimentAcronym table       
        SatExperimentAcronymDF = pd.DataFrame([])
        SatExperimentAcronymDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatExperimentAcronymDF['acronym'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[0].replace('Autism','').replace('Conversation',''))

        transformData['SatExperimentAcronym'] = SatExperimentAcronymDF
        
        # create a dataframe to be inserted in HubExperiment hub table, the sequence in this table is created such that it references SatExperimentTitle and SatExperimentAcronym table records uniquely
        HubExperimentDF = pd.DataFrame([])
        HubExperimentDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubExperiment'] = HubExperimentDF
        
        # create a dataframe to be inserted in HubExperimentalUnit hub table, the sequence in this table is created such that it references SatExperimentalUnitIdentifier table records uniquely
        HubExperimentalUnitDF = pd.DataFrame([])
        HubExperimentalUnitDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubExperimentalUnit'] = HubExperimentalUnitDF
        
        # create a dataframe to be inserted in SatExperimentalUnitIdentifier Satellite table, the ID attribute contains the unique identifier for each experimental unit        
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify ID in SatExperimentalUnitIdentifier table
        SatExperimentalUnitIdentifierDF = pd.DataFrame([])
        SatExperimentalUnitIdentifierDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatExperimentalUnitIdentifierDF['ID'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[0].split("_")[0])

        transformData['SatExperimentalUnitIdentifier'] = SatExperimentalUnitIdentifierDF
        
        # create a dataframe to be inserted in HubSubject Hub table, the name attribute contains the name for each subject taking part in experiment
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify SatSubjectAge and SatSubjectName satellite table    
        HubSubjectDF = pd.DataFrame([])
        HubSubjectDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        HubSubjectDF['name'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[0].split("-")[0])

        transformData['HubSubject'] = HubSubjectDF
        
        # create a dataframe to be inserted in SatSubjectAge Satellite table, the age attribute contains the age for each subject taking part in experiment
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify age in SatSubjectAge satellite table     
        SatSubjectAgeDF = pd.DataFrame([])
        SatSubjectAgeDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatSubjectAgeDF['age'] = pd.Series([0 for x in range(len(SatSubjectAgeDF.index))])
        
        transformData['SatSubjectAge'] = SatSubjectAgeDF
        
        # create a dataframe to be inserted in SatSubjectName Satellite table, the name attribute contains the age for each subject taking part in experiment
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify name in SatSubjectName satellite table 
        SatSubjectNameDF = pd.DataFrame([])
        SatSubjectNameDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatSubjectNameDF['name'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[0].split("-")[0])

        transformData['SatSubjectName'] = SatSubjectNameDF
        
        # create a dataframe to be inserted in ParticipatesIn link table, ParticipatesIn links HubExperimentalUnit and HubExperiment
        ParticipatesInDF = pd.DataFrame([])
        ParticipatesInDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        ParticipatesInDF['experimentalunit'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        ParticipatesInDF['experiment'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['ParticipatesIn'] = ParticipatesInDF
        
        # create a dataframe to be inserted in SatFactorName and SatFactorLevel Satellite table
        # there is ine factor called conversation  in pre-autism  data, these are the factor names
        # the level values are Normal and Stressed Conversation
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify title in SatSubjectName satellite table
        SatFactorDF = pd.DataFrame([])
        SatFactorDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatFactorDF['experiment'] = SatFactorDF['sequence']
        SatFactorDF['isCofactor'] = pd.Series([False for x in range(len(SatFactorDF.index))])
        SatFactorDF['name'] = pd.Series(['Conversation' for x in range(len(SatFactorDF.index))])
        SatFactorDF['preAutismFileNames']= pd.Series(preAutismFileNames)

        SatFactorDF['levelValue'] = SatFactorDF['preAutismFileNames'].apply(lambda x: "Normal" if "NormalConversation" in x else "Stressed")
        SatFactorDF = SatFactorDF[['sequence','name','levelValue','experiment','isCofactor']]
        HubFactorDF = SatFactorDF[['sequence','experiment','isCofactor']]
        
        transformData['HubFactor'] = HubFactorDF
        
        SatFactorNameDF = SatFactorDF[['sequence','name']]

        transformData['SatFactorName'] = SatFactorNameDF
        
        SatFactorLevelDF = SatFactorDF[['sequence','levelValue']]

        transformData['SatFactorLevel'] = SatFactorLevelDF
        
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify HubTreatment Hub table and HubExperiment 
        HubTreatmentDF = pd.DataFrame([])
        HubTreatmentDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        HubTreatmentDF['experiment'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubTreatment'] = HubTreatmentDF
        
        # create a dataframe to be inserted in SatTreatmentFactorLevel Satellite table, the factor level is linked to SatFactorLevel satellite table from HubFactor
        SatTreatmentFactorLevelDF = SatFactorDF[['sequence','experiment']]
        
        transformData['SatTreatmentFactorLevel'] = SatTreatmentFactorLevelDF
        
        # create a dataframe to be inserted in HubGroup Hub table, the sequence in this table is created such that it references SatGroupName table records uniquely
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify SatSubjectAge and SatSubjectName satellite table
        HubGroupDF = pd.DataFrame([])
        HubGroupDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        HubGroupDF['treatment'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubGroup'] = HubGroupDF
        
        # create a dataframe to be inserted in AssignedTo link table, AssignedTo links HubExperimentalUnit and HubGroup
        AssignedToDF= pd.DataFrame([])
        AssignedToDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AssignedToDF['experimentalUnit'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AssignedToDF['group'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['AssignedTo'] = AssignedToDF

        # create a dataframe to be inserted in SatGroupName Satellite table, the name attribute contains the group that each subject belongs to within an experiment
        SatGroupNameDF = SatFactorDF[['sequence','levelValue']]
        SatGroupNameDF = SatGroupNameDF.rename(columns = {'levelValue' : 'name'})
        SatGroupNameDF = SatGroupNameDF[['sequence','name']]
        
        transformData['SatGroupName'] =SatGroupNameDF
        
        # create a dataframe to be inserted in SatSessionName Satellite table, the name attribute contains the session that each subject attends during the course of an experiment
        SatSessionNameDF = SatGroupNameDF
        
        transformData['SatSessionName'] = SatSessionNameDF
        
        # create a dataframe to be inserted in HubSession Hub table, the sequence in this table is created such that it references SatSessionName table records uniquely
        # a combination of dates, time and file name are chosen as a sequence to uniquely identify SatSessionName satellite table
        HubSessionDF = pd.DataFrame([])
        HubSessionDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubSession'] = HubSessionDF
        
        # create a dataframe to be inserted in AttendsSession link table, AttendsSession links HubExperimentalUnit, HubGroup and HubSession
        AttendsSessionDF = pd.DataFrame([])
        AttendsSessionDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AttendsSessionDF['experimentalUnit'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AttendsSessionDF['group'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AttendsSessionDF['session'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['AttendsSession'] = AttendsSessionDF
        
        # create a dataframe to be inserted in SessionMetaData link table, SessionMetaData links HubSession and HubMetaData
        SessionMetaDataDF = pd.DataFrame([])
        SessionMetaDataDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SessionMetaDataDF['session'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SessionMetaDataDF['metadata'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['SessionMetaData'] = SessionMetaDataDF
        
        # create a dataframe to be inserted in SatObservationName Satellite table, the name attribute uniquely identifies the observation name in the experiment and set to experimrnt title that is derived from file name
        # a combination of dates, time and file are chosen as a sequence to uniquely identify name in SatObservationName table
        SatObservationNameDF = pd.DataFrame([])
        SatObservationNameDF['initialSequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatObservationNameDF['name'] = SatExperimentTitleDF['title']
        SatObservationNameDF['observationType'] = pd.Series([list(['data', 'wavelengthOneData', 'wavelengthTwoData', 'eventonsData']) for x in range(len(SatObservationNameDF.index))])
        SatObservationNameDF = SatObservationNameDF.set_index(['initialSequence','name']).apply(pd.Series.explode).reset_index()
        SatObservationNameDF['name'] =  ['_'.join(i) for i in zip(SatObservationNameDF['name'],SatObservationNameDF['observationType'])]
        SatObservationNameDF['name'] = SatObservationNameDF['name'].apply(lambda x: x.replace("Conversation","").replace("Autism",""))
        SatObservationNameDF['sequence'] = ['_'.join(i) for i in zip(SatObservationNameDF['initialSequence'],SatObservationNameDF['observationType'])]

        transformData['SatObservationName'] = SatObservationNameDF
        
        # create a dataframe to be inserted in HubObservation Hub table, the sequence in this table is created such that it references SatObservationName and SatObservationValue table records uniquely
        # a combination of dates, time and file are chosen as a sequence to uniquely identify SatObservationName and SatObservationValue satellite table
        HubObservationDF = pd.DataFrame([])
        HubObservationDF['sequence'] = SatObservationNameDF['sequence']
        HubObservationDF['collectedAtSession'] = SatObservationNameDF['initialSequence']
        
        transformData['HubObservation'] = HubObservationDF
        
        # create a dataframe to be inserted in ObservationMetaData link table, ObservationMetaData links HubObservation and HubMetaData
        ObservationMetaDataDF = pd.DataFrame([])
        ObservationMetaDataDF['sequence'] = SatObservationNameDF['initialSequence']
        ObservationMetaDataDF['observation'] = SatObservationNameDF['sequence']
        ObservationMetaDataDF['metadata'] = SatObservationNameDF['initialSequence']
        
        transformData['ObservationMetaData'] = ObservationMetaDataDF
        
        
        SatObservationTimeStampsDF = pd.DataFrame([])
        SatObservationTimeStampsDF['initialSequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatObservationTimeStampsDF['fileName'] = pd.Series(fileName)
        SatObservationTimeStampsDF['fileName'] = SatObservationTimeStampsDF['fileName'].apply(lambda x : x.replace("NIRS-","").split("_")[0])
        SatObservationTimeStampsDF['time'] = pd.Series(time)
        SatObservationTimeStampsDF['time'] = [' '.join(i) for i in zip(SatObservationTimeStampsDF['fileName'], SatObservationTimeStampsDF['time'])]
        SatObservationTimeStampsDF['SamplingRate'] = pd.Series(samplingRates)
        SatObservationTimeStampsDF['observationType'] = pd.Series([list(['data', 'wavelengthOneData', 'wavelengthTwoData', 'eventonsData']) for x in range(len(SatObservationTimeStampsDF.index))])
        SatObservationTimeStampsDF = SatObservationTimeStampsDF.set_index(['initialSequence','fileName','time','SamplingRate']).apply(pd.Series.explode).reset_index()
        SatObservationTimeStampsDF['sequence'] = ['_'.join(i) for i in zip(SatObservationTimeStampsDF['initialSequence'] ,SatObservationTimeStampsDF['observationType'])]      

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
             
            startDateTime = parser.parse(startTime)       
            sampleNumbers = np.arange(shape)
            samplingRate = float(samplingRate)
            
            timestamps = []
            for sampleNumber in sampleNumbers:
                timestamps.append(str((startDateTime + dt.timedelta(seconds= samplingRate * sampleNumber))))
            return timestamps

        #get observationvalues for each data (.dat, .wl1, .wl3) and generate a timestamp with start time and in steps of sampling rate
        SatObservationValueDataDF = SatObservationNameDF.set_index('name').filter(like='data', axis=0).reset_index() 
        SatObservationValueDataDF = pd.concat([SatObservationValueDataDF, preAutismDF[['preAutismData']]], axis=1)
        SatObservationValueDataDF = SatObservationValueDataDF[['sequence','preAutismData']]
        SatObservationValueDataDF = SatObservationValueDataDF.rename(columns = {'preAutismData' : 'value'})
        SatObservationValueDataDF = pd.merge(SatObservationValueDataDF, SatObservationTimeStampsDF, how = 'inner', left_on='sequence', right_on='sequence')
        SatObservationValueDataDF['timestamps'] = SatObservationValueDataDF.apply(lambda x: getTimestamps(x['time'], x['value'].shape[0], x['SamplingRate']), axis = 1)
        
        SatObservationValueWavelengthOneDataDF = SatObservationNameDF.set_index('name').filter(like='wavelengthOneData', axis=0).reset_index() 
        SatObservationValueWavelengthOneDataDF = pd.concat([SatObservationValueDataDF, preAutismDF[['preAutismWavelengthOneData']]], axis=1)
        SatObservationValueWavelengthOneDataDF = SatObservationValueWavelengthOneDataDF[['sequence','preAutismWavelengthOneData']]
        SatObservationValueWavelengthOneDataDF = SatObservationValueWavelengthOneDataDF.rename(columns = {'preAutismWavelengthOneData' : 'value'})
        SatObservationValueWavelengthOneDataDF = pd.merge(SatObservationValueWavelengthOneDataDF, SatObservationTimeStampsDF, how = 'inner', left_on='sequence', right_on='sequence')
        SatObservationValueWavelengthOneDataDF['timestamps'] = SatObservationValueWavelengthOneDataDF.apply(lambda x: getTimestamps(x['time'], x['value'].shape[0], x['SamplingRate']), axis = 1)
        
        SatObservationValueWavelengthTwoDataDF = SatObservationNameDF.set_index('name').filter(like='wavelengthTwoData', axis=0).reset_index() 
        SatObservationValueWavelengthTwoDataDF = pd.concat([SatObservationValueDataDF, preAutismDF[['preAutismWavelengthTwoData']]], axis=1)
        SatObservationValueWavelengthTwoDataDF = SatObservationValueWavelengthTwoDataDF[['sequence','preAutismWavelengthTwoData']]
        SatObservationValueWavelengthTwoDataDF = SatObservationValueWavelengthTwoDataDF.rename(columns = {'preAutismWavelengthTwoData' : 'value'})
        SatObservationValueWavelengthTwoDataDF = pd.merge(SatObservationValueWavelengthTwoDataDF, SatObservationTimeStampsDF, how = 'inner', left_on='sequence', right_on='sequence')
        SatObservationValueWavelengthTwoDataDF['timestamps'] = SatObservationValueWavelengthTwoDataDF.apply(lambda x: getTimestamps(x['time'], x['value'].shape[0], x['SamplingRate']), axis = 1)    
        
        # combine all the datasets in a single dataframe to be inserted into SatObservationValue table
        SatObservationValueDF = pd.concat([SatObservationValueDataDF, SatObservationValueWavelengthOneDataDF, SatObservationValueWavelengthTwoDataDF]).reset_index()
        SatObservationValueDF['value'] = SatObservationValueDF['value'].apply(lambda x: x.values.tolist())
        
        transformData['SatObservationValue'] = SatObservationValueDF
        
        return transformData


class FileLoader():
    """
    
    FileLoader takes the input from transform stage and loads the data in Enterprise data warehouse called data vault
    
    This class takes the input from transform stage, connects to a postgres database using pyscopg2 and loads the individual
    links, hubs and satellites using INSERT sql query. Along the data the current timestamp and postgres user name is also added to table columns
    
    
    """
    
    def loadDataToEnterpriseLayer(self,inputs) -> None:
        """
        
        this function connects to a postgre server, takes the input dictionary and retrieves each of the dataframe based on keys
        the data is inserted using pyscopg2 library
        
        Args:
            inputs : a dictionary with key as table name and value as the dataframe to be inserted in "key" table

        Returns:
            None
        
        
        """
        
        user="smd"
        password="smd2022"
        host="localhost"
        port="5432"
        database="smdvault"
    
        try:
            connection = psycopg2.connect(user=user,password=password,host=host,port=port,database=database)

            for input in inputs:
                
                HubMetaDataDF = input['HubMetaData']
                for i in HubMetaDataDF.index:
                    cursor = connection.cursor()
                    query = f"""INSERT INTO "HubMetaData" (sequence,timestamp,source) VALUES (md5('%s'),current_timestamp,'{user}'); """ % (HubMetaDataDF['sequence'][i])
                    cursor.execute(query)
                    connection.commit()
                
                SatMetaDataKeyValuePairDF = input['SatMetaDataKeyValuePair']
                for i in SatMetaDataKeyValuePairDF.index:
                    cursor = connection.cursor()
                    query = f"""INSERT INTO "SatMetaDataKeyValuePair" (sequence,timestamp,source,key,value) VALUES (md5('%s'),current_timestamp,'{user}','%s',%s); """ % (SatMetaDataKeyValuePairDF['sequence'][i],SatMetaDataKeyValuePairDF['key'][i], psycopg2.Binary(SatMetaDataKeyValuePairDF['value'][i]) )
                    cursor.execute(query)
                    connection.commit()
                
                HubExperimentDF = input['HubExperiment']
                for i in HubExperimentDF.index:
                    cursor = connection.cursor()
                    query = f"""INSERT INTO "HubExperiment" (sequence,timestamp,source) VALUES (md5('%s'),current_timestamp,'{user}'); """ % (HubExperimentDF['sequence'][i])
                    cursor.execute(query)
                    connection.commit()

                SatExperimentTitleDF = input['SatExperimentTitle']
                for i in SatExperimentTitleDF.index:
                    cursor = connection.cursor()
                    query = f"""INSERT INTO "SatExperimentTitle" (sequence,timestamp,source,title) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatExperimentTitleDF['sequence'][i],SatExperimentTitleDF['title'][i])
                    cursor.execute(query)
                    connection.commit()
                    
                SatExperimentAcronymDF = input['SatExperimentAcronym']
                for i in SatExperimentAcronymDF.index:
                    cursor = connection.cursor()
                    query = f"""INSERT INTO "SatExperimentAcronym" (sequence,timestamp,source,acronym) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatExperimentAcronymDF['sequence'][i],SatExperimentAcronymDF['acronym'][i])
                    cursor.execute(query)
                    connection.commit()
                
                HubExperimentalUnitDF = input['HubExperimentalUnit']
                for i in HubExperimentalUnitDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubExperimentalUnit" (sequence,timestamp,source) VALUES (md5('%s'),current_timestamp,'{user}'); """ % (HubExperimentalUnitDF['sequence'][i])
                        cursor.execute(query)
                        connection.commit()
                
                HubSubjectDF = input['HubSubject']
                for i in HubSubjectDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubSubject" (sequence,timestamp,source,name) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (HubSubjectDF['sequence'][i],HubSubjectDF['name'][i])
                        cursor.execute(query)
                        connection.commit()
                        
                SatSubjectAgeDF = input['SatSubjectAge']
                for i in SatSubjectAgeDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatSubjectAge" (sequence,timestamp,source,age) VALUES (md5('%s'),current_timestamp,'{user}',%s); """ % (SatSubjectAgeDF['sequence'][i],SatSubjectAgeDF['age'][i])
                        cursor.execute(query)
                        connection.commit()
                
                SatSubjectNameDF = input['SatSubjectName']
                for i in SatSubjectNameDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatSubjectName" (sequence,timestamp,source,name) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatSubjectNameDF['sequence'][i],SatSubjectNameDF['name'][i])
                        cursor.execute(query)
                        connection.commit()
                        
                ParticipatesInDF = input['ParticipatesIn']
                for i in ParticipatesInDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "ParticipatesIn" (sequence,timestamp,source,"experimentalUnit",experiment) VALUES (md5('%s'),current_timestamp,'{user}',md5('%s'),md5('%s')); """ % (ParticipatesInDF['sequence'][i],ParticipatesInDF['experimentalunit'][i],ParticipatesInDF['experiment'][i])
                        cursor.execute(query)
                        connection.commit()
                        
                SatExperimentalUnitIdentifierDF = input['SatExperimentalUnitIdentifier']
                for i in SatExperimentalUnitIdentifierDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatExperimentalUnitIdentifier" (sequence,timestamp,source,"ID") VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatExperimentalUnitIdentifierDF['sequence'][i],SatExperimentalUnitIdentifierDF['ID'][i])
                        cursor.execute(query)
                        connection.commit()
                
                HubFactorDF = input['HubFactor']
                for i in HubFactorDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubFactor" (sequence,timestamp,source,experiment) VALUES (md5('%s'),current_timestamp,'{user}',md5('%s')); """ % (HubFactorDF['sequence'][i],HubFactorDF['experiment'][i])
                        cursor.execute(query)
                        connection.commit()     
                        
                SatFactorNameDF = input['SatFactorName']
                for i in SatFactorNameDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatFactorName" (sequence,timestamp,source,name) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatFactorNameDF['sequence'][i],SatFactorNameDF['name'][i])
                        cursor.execute(query)
                        connection.commit()         

        
                SatFactorLevelDF = input['SatFactorLevel']          
                for i in SatFactorLevelDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatFactorLevel" (sequence,timestamp,source,"levelValue") VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatFactorLevelDF['sequence'][i],SatFactorLevelDF['levelValue'][i])
                        cursor.execute(query)
                        connection.commit()  
                
                HubTreatmentDF = input['HubTreatment']
                for i in HubTreatmentDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubTreatment" (sequence,timestamp,source,experiment) VALUES (md5('%s'),current_timestamp,'{user}',md5('%s')); """ % (HubTreatmentDF['sequence'][i],HubTreatmentDF['experiment'][i])
                        cursor.execute(query)
                        connection.commit()  
                
                SatTreatmentFactorLevelDF = input['SatTreatmentFactorLevel']
                for i in SatTreatmentFactorLevelDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatTreatmentFactorLevel" (sequence,timestamp,source,"factorLevel") VALUES (md5('%s'),current_timestamp,'{user}',md5('%s')); """ % (SatTreatmentFactorLevelDF['experiment'][i],SatTreatmentFactorLevelDF['sequence'][i])
                        cursor.execute(query)
                        connection.commit()  
                
                HubGroupDF = input['HubGroup']
                for i in HubGroupDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubGroup" (sequence,timestamp,source,treatment) VALUES (md5('%s'),current_timestamp,'{user}',md5('%s')); """ % (HubGroupDF['sequence'][i],HubGroupDF['treatment'][i])
                        cursor.execute(query)
                        connection.commit()  
                
                SatGroupNameDF = input['SatGroupName']
                for i in SatGroupNameDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatGroupName" (sequence,timestamp,source,name) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatGroupNameDF['sequence'][i],SatGroupNameDF['name'][i])
                        cursor.execute(query)
                        connection.commit() 
                
                AssignedToDF = input['AssignedTo']
                for i in AssignedToDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "AssignedTo" (sequence,timestamp,source,"experimentalUnit","group") VALUES (md5('%s'),current_timestamp,'{user}',md5('%s'),md5('%s')); """ % (AssignedToDF['sequence'][i],AssignedToDF['experimentalUnit'][i],AssignedToDF['group'][i])
                        cursor.execute(query)
                        connection.commit() 
                        
                HubSessionDF = input['HubSession']
                for i in HubSessionDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubSession" (sequence,timestamp,source) VALUES (md5('%s'),current_timestamp,'{user}'); """ % (HubSessionDF['sequence'][i])
                        cursor.execute(query)
                        connection.commit() 
                        
                SatSessionNameDF = input['SatSessionName']
                for i in SatSessionNameDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatSessionName" (sequence,timestamp,source,name) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatSessionNameDF['sequence'][i],SatSessionNameDF['name'][i])
                        cursor.execute(query)
                        connection.commit() 
                        
                SessionMetaDataDF = input['SessionMetaData']
                for i in SessionMetaDataDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SessionMetaData" (sequence,timestamp,source,session,metadata) VALUES (md5('%s'),current_timestamp,'{user}',md5('%s'),md5('%s')); """ % (SessionMetaDataDF['sequence'][i],SessionMetaDataDF['session'][i],SessionMetaDataDF['metadata'][i])
                        cursor.execute(query)
                        connection.commit() 
                
                HubObservationDF = input['HubObservation']
                for i in HubObservationDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "HubObservation" (sequence,timestamp,source,"collectedAtSession") VALUES (md5('%s'),current_timestamp,'{user}',md5('%s')); """ % (HubObservationDF['sequence'][i],HubObservationDF['collectedAtSession'][i])
                        cursor.execute(query)
                        connection.commit() 
                
                ObservationMetaDataDF = input['ObservationMetaData']
                for i in ObservationMetaDataDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "ObservationMetaData" (sequence,timestamp,source,observation,metadata) VALUES (md5('%s'),current_timestamp,'{user}',md5('%s'),md5('%s')); """ % (ObservationMetaDataDF['sequence'][i],ObservationMetaDataDF['observation'][i],ObservationMetaDataDF['metadata'][i])
                        cursor.execute(query)
                        connection.commit()                 
                
                AttendsSessionDF = input['AttendsSession']
                for i in AttendsSessionDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "AttendsSession" (sequence,timestamp,source,"experimentalUnit","group","session") VALUES (md5('%s'),current_timestamp,'{user}',md5('%s'),md5('%s'),md5('%s')); """ % (AttendsSessionDF['sequence'][i],AttendsSessionDF['experimentalUnit'][i],AttendsSessionDF['group'][i],AttendsSessionDF['session'][i])
                        cursor.execute(query)
                        connection.commit() 
                
                SatObservationNameDF = input['SatObservationName']
                for i in SatObservationNameDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatObservationName" (sequence,timestamp,source,name) VALUES (md5('%s'),current_timestamp,'{user}','%s'); """ % (SatObservationNameDF['sequence'][i],SatObservationNameDF['name'][i])
                        cursor.execute(query)
                        connection.commit() 
                
                SatObservationValueDF = input['SatObservationValue']
                for i in SatObservationValueDF.index:
                        cursor = connection.cursor()
                        query = f"""INSERT INTO "SatObservationValue" (sequence,timestamp,source,value,timestamps) VALUES (md5('%s'),current_timestamp,'{user}',array%s,array%s::timestamp[]); """ % (SatObservationValueDF['sequence'][i],SatObservationValueDF['value'][i],SatObservationValueDF['timestamps'][i])
                        cursor.execute(query)
                        connection.commit() 

            print("Inserted data successfully in PostgreSQL ")

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")



class ExtractTransformLoadHelper:
    """
    
    ExtractTransformLoadHelper helps us implement ETL process
    
    
    step 1:  a. Read all the HbR files in VMData_Blinded folder and call readVMFile method to read metadata and data for VM Deoxy files
             b. the metadata received is added to a list called vmDeoxyMetaData that is to be sent for transformation stage
             c. the data received is added to a list called vmDeoxyData that is to be sent for transformation stage
             d. all the names of files are added to a list called vmDeoxyFileNames that is to be sent for transformation stage
             e. transformVMFile method is called to transform all the HbR data and appended to a list caleed transformedData that is to be sent for loading stage
    
    step 2:  a. Read all the HbO2 files in VMData_Blinded folder and call readVMFile method to read metadata and data for VM Oxy files
             b. the metadata received is added to a list called vmOxyMetaData that is to be sent for transformation stage
             c. the data received is added to a list called vmOxyData that is to be sent for transformation stage
             d. all the names of files are added to a list called vmOxyFileNames that is to be sent for transformation stage
             e. transformVMFile method is called to transform all the HbO2 data and appended to a list caleed transformedData that is to be sent for loading stage
    
    step 3:  a. Read all the raw intensity files in VMData_Blinded folder and call readVMFile method to read metadata and data for VM MES files
             b. the metadata received is added to a list called vmMesMetaData that is to be sent for transformation stage
             c. the data received is added to a list called vmMesData that is to be sent for transformation stage
             d. all the names of files are added to a list called vmMesFileNames that is to be sent for transformation stage
             e. transformVMFile method is called to transform all the raw data and appended to a list caleed transformedData that is to be sent for loading stage
             
    step 4:  read .dat files for pre-autism data for both normal ans stressed conversation and store in preAutismData to be sent for transformation stage
    
    step 5: read .wl1 files for pre-autism data for both normal ans stressed conversation and store in preAutismWavelengthOneData to be sent for transformation stage
    
    step 6: read .wl2 files for pre-autism data for both normal ans stressed conversation and store in preAutismWavelengthTwoData to be sent for transformation stage
    
    step 7: read .evt files for pre-autism data for both normal ans stressed conversation and store in preAutismEventonsData to be sent for transformation stage
    
    step 8: read .hdr files for pre-autism data for both normal ans stressed conversation to retrieve metadata and add it to preAutismMetaData to be sent for transformation stage
    
    step 9: transformPreAutismFile is called to transform all the pre-autism data for both Normal and Stressed conversation, the transformed data is added to transformedData list to be sent for loading stage
    
    step 10: loadDataToEnterpriseLayer is called and it loads all the data to Enterprise data warehouse
    """
    def main():
        
        r=FileReader()
        t=FileTransformer()
        l=FileLoader()
        
        
        config = open('config.txt', 'r', errors="ignore")
        vmDataPath = r._findField(config, "VMDataFolder").lstrip(',').replace('\n','')
        config.seek(0)
        preAutismDataPath = r._findField(config, "PreAutismDataFolder").lstrip(',').replace('\n','')
        
        print("vmDataPath :",vmDataPath)
        print("preAutismDataPath : ", preAutismDataPath)
        
        transformedData = []
        
        vmDeoxyMetaData=[]
        vmDeoxyData=[]
        vmDeoxyFileNames=[]


        os.chdir(vmDataPath)
        #step 1
        for fileName in glob.glob('*_HBA_Probe1_Deoxy.csv'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData, data = r.readVMFile(file)
                vmDeoxyMetaData.append(metaData)
                vmDeoxyData.append(data)
                vmDeoxyFileNames.append(fileName)
        
        transformedVmDeoxyData = t.transformVMFile(vmDeoxyFileNames, vmDeoxyMetaData, vmDeoxyData)
        
        transformedData.append(transformedVmDeoxyData)

        vmOxyMetaData=[]
        vmOxyData=[]
        vmOxyFileNames=[]
        
        #step 2
        for fileName in glob.glob('*_HBA_Probe1_Oxy.csv'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData, data = r.readVMFile(file)
                vmOxyMetaData.append(metaData)
                vmOxyData.append(data)
                vmOxyFileNames.append(fileName)
        
        transformedVmOxyData = t.transformVMFile(vmOxyFileNames, vmOxyMetaData, vmOxyData)
        
        transformedData.append(transformedVmOxyData)
        
        vmMesMetaData=[]
        vmMesData=[]
        vmMesFileNames=[]
        
        #step 3
        for fileName in glob.glob('*_MES_Probe1.csv'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData, data = r.readVMFile(file)
                vmMesMetaData.append(metaData)
                vmMesData.append(data)
                vmMesFileNames.append(fileName)
        
        transformedVmMesData = t.transformVMFile(vmMesFileNames, vmMesMetaData, vmMesData)
        
        transformedData.append(transformedVmMesData)     
        
        preAutismMetaData=[]
        preAutismFileNames=[]
        preAutismData=[]
        preAutismWavelengthOneData=[]
        preAutismWavelengthTwoData=[]
        preAutismEventonsData=[]
        
        os.chdir(preAutismDataPath)
        # step 4
        for fileName in glob.glob('*_NormalConversation/*.dat'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismData.append(data)
        
        for fileName in glob.glob('*_StressedConversation/*.dat'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismData.append(data)
        
        # step 5    
        for fileName in glob.glob('*_NormalConversation/*.wl1'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthOneData.append(data)
        
        for fileName in glob.glob('*_StressedConversation/*.wl1'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthOneData.append(data)
        
        # step 6  
        for fileName in glob.glob('*_NormalConversation/*.wl2'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthTwoData.append(data)
        
        for fileName in glob.glob('*_StressedConversation/*.wl2'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthTwoData.append(data)
        
        # step 7            
        for fileName in glob.glob('*_NormalConversation/*.evt'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismEventonsData.append(data)
        
        for fileName in glob.glob('*_StressedConversation/*.evt'):
            data = pd.DataFrame(np.genfromtxt(fileName))  
            preAutismEventonsData.append(data)
        
        # step 8     
        for fileName in glob.glob('*_NormalConversation/*.hdr'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData = r.readPreAutismMetaData(file)
                preAutismMetaData.append(metaData)
                preAutismFileNames.append(fileName)    
        
        for fileName in glob.glob('*_StressedConversation/*.hdr'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData = r.readPreAutismMetaData(file)
                preAutismMetaData.append(metaData)
                preAutismFileNames.append(fileName)
        
        # step 9
        transformedData.append(t.transformPreAutismFile(preAutismFileNames, preAutismMetaData, preAutismData, preAutismWavelengthOneData, preAutismWavelengthTwoData, preAutismEventonsData))
        
        # step 10
        l.loadDataToEnterpriseLayer(transformedData)

        


if __name__ == "__main__":
    ExtractTransformLoadHelper.main()