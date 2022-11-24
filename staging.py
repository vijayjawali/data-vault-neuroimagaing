import os
import glob
import datetime as dt
from dateutil import parser
import pandas as pd
import numpy as np
import psycopg2, pickle
from psycopg2 import Error


class FileReader():  
    
    def __init__(self):
        self.__filename = ""
    
    def _findField(self,file, fieldName, allLines = []):
        
        if not allLines:
            allLines = file.readlines()
             
        
        for currentLine in allLines:
            if fieldName in currentLine:
                currentLine = currentLine.split(fieldName)
                return  ''.join(currentLine[1])
        
        return ""
    
        
    def _findFieldPosition(self,file, fieldName):
        currentLine = file.readline()
        if fieldName in currentLine:
            return  None
        else:
            return self._findFieldPosition(file, fieldName)

        
    def readVMFile(self, file):

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
    
    def getParameters(self, file, field, params):
        parameters = {}
        self._findFieldPosition(file, field)
        headerLines = file.readlines()   
        for param in params:
            parameters[param] = self._findField(file, param, headerLines).replace('\n','').replace('\"','').replace('=','').replace('\t',',')
        
        file.seek(0)
        return parameters

    
    def getArray(self, file, field, params, arrayFields):
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
    
    
    def readPreAutismMetaData(self, file):
        
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
    
    def __init__(self):
        self.__filename = ""
        
    def transformVMFile(self, fileName, metaData, data):

        transformData = {}
        dates = []
        keys = []
        values = []
        samplePeriods = []
            
        for meta in metaData:
            dates.append(meta['Date'])
            samplePeriods.append(meta['Sampling Period[s]'])
            keys.append(list(meta.keys()))
            values.append(list(meta.values()))

        dateSamplingTimeDF = pd.DataFrame({'date': dates, 'samplePeriod': samplePeriods})
        dateSamplingTimeDF['samplePeriod'] = dateSamplingTimeDF['samplePeriod'].astype(float)
        dateSamplingTimeDF['date'] =  pd.to_datetime(dateSamplingTimeDF['date'], format="%d/%m/%Y %H:%M:%S")
        
        experimentTitle=[]
        acronym=[]
        for file in fileName:
            title = file.split('\\')
            acro = title[1].split('_')
            experimentTitle.append(title[1].replace('.csv',''))
            acronym.append(acro[1])
        
        SatMetaDataKeyValuePairDF = pd.DataFrame([])
        SatMetaDataKeyValuePairDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatMetaDataKeyValuePairDF['key'] = pd.Series(keys)
        SatMetaDataKeyValuePairDF['value'] = pd.Series(values)
        SatMetaDataKeyValuePairDF = SatMetaDataKeyValuePairDF.set_index(['sequence']).apply(pd.Series.explode).reset_index()
        SatMetaDataKeyValuePairDF['value'] = pickle.dumps(SatMetaDataKeyValuePairDF['value'])
        
        transformData['SatMetaDataKeyValuePair'] = SatMetaDataKeyValuePairDF

        HubMetaDataDF = pd.DataFrame([])
        HubMetaDataDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubMetaData'] = HubMetaDataDF


            
        SatExperimentTitleDF = pd.DataFrame([])
        SatExperimentTitleDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatExperimentTitleDF['title']= pd.Series(experimentTitle)
        
        transformData['SatExperimentTitle'] = SatExperimentTitleDF
        
        
        SatExperimentAcronymDF = pd.DataFrame([])
        SatExperimentAcronymDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatExperimentAcronymDF['acronym'] = pd.Series(acronym)
        
        transformData['SatExperimentAcronym'] = SatExperimentAcronymDF
        
        
        HubExperimentDF = pd.DataFrame([])
        HubExperimentDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubExperiment'] = HubExperimentDF
        
        HubExperimentalUnitDF = pd.DataFrame([])
        HubExperimentalUnitDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubExperimentalUnit'] = HubExperimentalUnitDF
        
        identities=[]
        for meta in metaData:
            identities.append(meta['ID'])
        
        SatExperimentalUnitIdentifierDF = pd.DataFrame([])
        SatExperimentalUnitIdentifierDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatExperimentalUnitIdentifierDF['ID'] = pd.Series(identities)
        
        transformData['SatExperimentalUnitIdentifier'] = SatExperimentalUnitIdentifierDF
        
        names=[]
        for meta in metaData:
            names.append(meta['Name'])
            
        HubSubjectDF = pd.DataFrame([])
        HubSubjectDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubSubjectDF['name'] = pd.Series(names)
        
        transformData['HubSubject'] = HubSubjectDF
        
        ages=[]
        for meta in metaData:
            ages.append(int(meta['Age'].lstrip().rstrip().replace('y',''))) 
            
        SatSubjectAgeDF = pd.DataFrame([])
        SatSubjectAgeDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatSubjectAgeDF['age'] = pd.Series(ages)
        
        transformData['SatSubjectAge'] = SatSubjectAgeDF
        
        SatSubjectNameDF = pd.DataFrame([])
        SatSubjectNameDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatSubjectNameDF['name'] = pd.Series(names)
        
        transformData['SatSubjectName'] = SatSubjectNameDF
        
        ParticipatesInDF = pd.DataFrame([])
        ParticipatesInDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ParticipatesInDF['experimentalunit'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ParticipatesInDF['experiment'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['ParticipatesIn'] = ParticipatesInDF
        
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
        SatFactorDF['sequence'] = SatFactorDF['sequence'] + SatFactorDF['name']
        HubFactorDF = SatFactorDF[['sequence','experiment','isCofactor']]
        
        transformData['HubFactor'] = HubFactorDF
        
        SatFactorNameDF = SatFactorDF[['sequence','name']]
        
        transformData['SatFactorName'] = SatFactorNameDF
        
        SatFactorLevelDF = SatFactorDF[['sequence','levelValue']]
        
        transformData['SatFactorLevel'] = SatFactorLevelDF
        
        HubTreatmentDF = pd.DataFrame([])
        HubTreatmentDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubTreatmentDF['experiment'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubTreatment'] = HubTreatmentDF
        
        SatTreatmentFactorLevelDF = SatFactorDF[['sequence','experiment']]
        
        transformData['SatTreatmentFactorLevel'] = SatTreatmentFactorLevelDF
        
        HubGroupDF = pd.DataFrame([])
        HubGroupDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubGroupDF['treatment'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubGroup'] = HubGroupDF
        
        AssignedToDF= pd.DataFrame([])
        AssignedToDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AssignedToDF['experimentalUnit'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AssignedToDF['group'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['AssignedTo'] = AssignedToDF
        
        levelNames = ['Visual and Motor Stimulus','Visual Stimulus','Motor Stimulus','Rest']
        SatGroupNameDF = SatExperimentAcronymDF[['sequence','acronym']]
        SatGroupNameDF['name'] = np.select(levelValueconditions, levelNames)
        SatGroupNameDF = SatGroupNameDF[['sequence','name']]
        
        transformData['SatGroupName'] =SatGroupNameDF
        
        SatSessionNameDF = SatGroupNameDF
        
        transformData['SatSessionName'] = SatSessionNameDF
        
        HubSessionDF = pd.DataFrame([])
        HubSessionDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubSession'] = HubSessionDF
        
        AttendsSessionDF = pd.DataFrame([])
        AttendsSessionDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AttendsSessionDF['experimentalUnit'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AttendsSessionDF['group'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        AttendsSessionDF['session'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['AttendsSession'] = AttendsSessionDF
        
        SessionMetaDataDF = pd.DataFrame([])
        SessionMetaDataDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SessionMetaDataDF['session'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SessionMetaDataDF['metadata'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['SessionMetaData'] = SessionMetaDataDF
        
        HubObservationDF = pd.DataFrame([])
        HubObservationDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        HubObservationDF['collectedAtSession'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['HubObservation'] = HubObservationDF
        
        ObservationMetaDataDF = pd.DataFrame([])
        ObservationMetaDataDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ObservationMetaDataDF['observation'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        ObservationMetaDataDF['metadata'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        
        transformData['ObservationMetaData'] = ObservationMetaDataDF
        
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
        
        SatObservationValueDF = pd.DataFrame([])
        SatObservationValueDF['sequence'] = pd.Series(a + '_' +b for a, b in zip(dates, experimentTitle))
        SatObservationValueDF['value'] = pd.Series(arrayData)
        SatObservationValueDF['timestamps'] = pd.Series(timestampData)
        
        transformData['SatObservationValue'] = SatObservationValueDF
        
        return transformData
    
    
    def transformPreAutismFile(self,preAutismFileNames, preAutismMetaData, preAutismData, preAutismWavelengthOneData, preAutismWavelengthTwoData, preAutismEventonsData):
        
        transformData = {}

        
        def recursive_items_key(dictionary):
            for key, value in dictionary.items():
                if type(value) is dict:
                     yield from recursive_items_key(value)
                else:
                    yield (key, value)

        def getKeyArrays(input):
            keyArray = []
            x = input
            for key, value in recursive_items_key(x):
                keyArray.append(key)
            
            return keyArray

        def getValueArrays(input):
            valueArray =[]
            x = input
            for key, value in recursive_items_key(x):
                valueArray.append(value)
            
            return valueArray
        
        
        fileName = []
        date = []
        time = []
        samplingRates = []
    
        simplifiedMetaDataList =[]
        for metaData in preAutismMetaData:
            simplifiedMetaData = {}
            for key, value in recursive_items_key(metaData):
                simplifiedMetaData[key] = value
            simplifiedMetaDataList.append(simplifiedMetaData)
        
        for meta in simplifiedMetaDataList:
            fileName.append(meta['FileName'])
            date.append(meta['Date'])
            time.append(meta['Time'])
            samplingRates.append(meta['SamplingRate'])
        
        preAutismDF = pd.DataFrame([])
        preAutismDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        preAutismDF['preAutismFileNames'] = pd.Series(preAutismFileNames)
        preAutismDF['preAutismMetaData'] = pd.Series(preAutismMetaData)
        preAutismDF['preAutismData'] = pd.Series(preAutismData)
        preAutismDF['preAutismWavelengthOneData'] = pd.Series(preAutismWavelengthOneData)
        preAutismDF['preAutismWavelengthTwoData'] = pd.Series(preAutismWavelengthTwoData)
        preAutismDF['preAutismEventonsData'] = pd.Series(preAutismEventonsData)
        
        SatMetaDataKeyValuePairDF = pd.DataFrame([])
        SatMetaDataKeyValuePairDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatMetaDataKeyValuePairDF['key'] = preAutismDF['preAutismMetaData'].apply(lambda x: getKeyArrays(x))
        SatMetaDataKeyValuePairDF['value'] = preAutismDF['preAutismMetaData'].apply(lambda x: getValueArrays(x))
        SatMetaDataKeyValuePairDF = SatMetaDataKeyValuePairDF[['sequence','key','value']] 
        SatMetaDataKeyValuePairDF = SatMetaDataKeyValuePairDF.set_index(['sequence']).apply(pd.Series.explode).reset_index()
        SatMetaDataKeyValuePairDF['value'] = pickle.dumps(SatMetaDataKeyValuePairDF['value'])
        
        transformData['SatMetaDataKeyValuePair'] = SatMetaDataKeyValuePairDF

        HubMetaDataDF = pd.DataFrame([])
        HubMetaDataDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubMetaData'] = HubMetaDataDF
     
        SatExperimentTitleDF = pd.DataFrame([])
        SatExperimentTitleDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatExperimentTitleDF['title']= preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[1])

        transformData['SatExperimentTitle'] = SatExperimentTitleDF
        
            
        SatExperimentAcronymDF = pd.DataFrame([])
        SatExperimentAcronymDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatExperimentAcronymDF['acronym'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[1].replace('Autism','').replace('Conversation',''))
        
        transformData['SatExperimentAcronym'] = SatExperimentAcronymDF
        
        
        HubExperimentDF = pd.DataFrame([])
        HubExperimentDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubExperiment'] = HubExperimentDF
        
        HubExperimentalUnitDF = pd.DataFrame([])
        HubExperimentalUnitDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubExperimentalUnit'] = HubExperimentalUnitDF
        
        SatExperimentalUnitIdentifierDF = pd.DataFrame([])
        SatExperimentalUnitIdentifierDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatExperimentalUnitIdentifierDF['ID'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[1].split("_")[0])
        
        transformData['SatExperimentalUnitIdentifier'] = SatExperimentalUnitIdentifierDF
            
        HubSubjectDF = pd.DataFrame([])
        HubSubjectDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        HubSubjectDF['name'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[1].split("-")[0])
        
        transformData['HubSubject'] = HubSubjectDF
            
        SatSubjectAgeDF = pd.DataFrame([])
        SatSubjectAgeDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatSubjectAgeDF['age'] = pd.Series([0 for x in range(len(SatSubjectAgeDF.index))])
        
        transformData['SatSubjectAge'] = SatSubjectAgeDF
        
        SatSubjectNameDF = pd.DataFrame([])
        SatSubjectNameDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatSubjectNameDF['name'] = preAutismDF['preAutismFileNames'].apply(lambda x: x.split('\\')[1].split("-")[0])
        
        transformData['SatSubjectName'] = SatSubjectNameDF
        
        ParticipatesInDF = pd.DataFrame([])
        ParticipatesInDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        ParticipatesInDF['experimentalunit'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        ParticipatesInDF['experiment'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['ParticipatesIn'] = ParticipatesInDF
        
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
        
        HubTreatmentDF = pd.DataFrame([])
        HubTreatmentDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        HubTreatmentDF['experiment'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubTreatment'] = HubTreatmentDF
        
        SatTreatmentFactorLevelDF = SatFactorDF[['sequence','experiment']]
        
        transformData['SatTreatmentFactorLevel'] = SatTreatmentFactorLevelDF
        
        HubGroupDF = pd.DataFrame([])
        HubGroupDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        HubGroupDF['treatment'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubGroup'] = HubGroupDF
        
        AssignedToDF= pd.DataFrame([])
        AssignedToDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AssignedToDF['experimentalUnit'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AssignedToDF['group'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['AssignedTo'] = AssignedToDF

        SatGroupNameDF = SatFactorDF[['sequence','levelValue']]
        SatGroupNameDF = SatGroupNameDF.rename(columns = {'levelValue' : 'name'})
        SatGroupNameDF = SatGroupNameDF[['sequence','name']]
        
        transformData['SatGroupName'] =SatGroupNameDF
        
        SatSessionNameDF = SatGroupNameDF
        
        transformData['SatSessionName'] = SatSessionNameDF
        
        HubSessionDF = pd.DataFrame([])
        HubSessionDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['HubSession'] = HubSessionDF
        
        AttendsSessionDF = pd.DataFrame([])
        AttendsSessionDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AttendsSessionDF['experimentalUnit'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AttendsSessionDF['group'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        AttendsSessionDF['session'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['AttendsSession'] = AttendsSessionDF
        
        SessionMetaDataDF = pd.DataFrame([])
        SessionMetaDataDF['sequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SessionMetaDataDF['session'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SessionMetaDataDF['metadata'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        
        transformData['SessionMetaData'] = SessionMetaDataDF
        
        SatObservationNameDF = pd.DataFrame([])
        SatObservationNameDF['initialSequence'] = pd.Series(a + '_' + b + '_'+ c for a, b, c in zip(fileName, date, time))
        SatObservationNameDF['name'] = SatExperimentTitleDF['title']
        SatObservationNameDF['observationType'] = pd.Series([list(['data', 'wavelengthOneData', 'wavelengthTwoData', 'eventonsData']) for x in range(len(SatObservationNameDF.index))])
        SatObservationNameDF = SatObservationNameDF.set_index(['initialSequence','name']).apply(pd.Series.explode).reset_index()
        SatObservationNameDF['name'] =  ['_'.join(i) for i in zip(SatObservationNameDF['name'],SatObservationNameDF['observationType'])]
        SatObservationNameDF['name'] = SatObservationNameDF['name'].apply(lambda x: x.replace("Conversation","").replace("Autism",""))
        SatObservationNameDF['sequence'] = ['_'.join(i) for i in zip(SatObservationNameDF['initialSequence'],SatObservationNameDF['observationType'])]

        transformData['SatObservationName'] = SatObservationNameDF
        
        HubObservationDF = pd.DataFrame([])
        HubObservationDF['sequence'] = SatObservationNameDF['sequence']
        HubObservationDF['collectedAtSession'] = SatObservationNameDF['initialSequence']
        
        transformData['HubObservation'] = HubObservationDF
        
        
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
        

        def getTimestamps(startTime, shape, samplingRate):
             
            startDateTime = parser.parse(startTime)       
            sampleNumbers = np.arange(shape)
            samplingRate = float(samplingRate)
            
            timestamps = []
            for sampleNumber in sampleNumbers:
                timestamps.append(str((startDateTime + dt.timedelta(seconds= samplingRate * sampleNumber))))
            return timestamps

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
        
        SatObservationValueDF = pd.concat([SatObservationValueDataDF, SatObservationValueWavelengthOneDataDF, SatObservationValueWavelengthTwoDataDF]).reset_index()
        SatObservationValueDF['value'] = SatObservationValueDF['value'].apply(lambda x: x.values.tolist())
        print(SatObservationValueDF)
        transformData['SatObservationValue'] = SatObservationValueDF
        
        return transformData


class FileLoader():
    
    def loadDataToEnterpriseLayer(self,inputs):
        user="postgres"
        password="Vijay42****@"
        host="localhost"
        port="5432"
        database="smdvault14"
    
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
    def main():
        r=FileReader()
        t=FileTransformer()
        l=FileLoader()
        
        transformedData = []
        
        vmDeoxyMetaData=[]
        vmDeoxyData=[]
        vmDeoxyFileNames=[]
        os.chdir('F:/University of Birmingham/Storing and Managing Data/Semester Project')
        #Vm Data
        for fileName in glob.glob('/data/VMData_Blinded/*_HBA_Probe1_Deoxy.csv'):
            print('fileNamefileNamefileNamefileName')
            print(fileName)
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
        for fileName in glob.glob('data/VMData_Blinded/*_HBA_Probe1_Oxy.csv'):
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
        for fileName in glob.glob('data/VMData_Blinded/*_MES_Probe1.csv'):
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
        
        # Pre Autism Data
        for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.dat'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismData.append(data)
            
        for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.wl1'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthOneData.append(data)
          
        for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.wl2'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthTwoData.append(data)
                    
        for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.evt'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismEventonsData.append(data)
             
        for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.hdr'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData = r.readPreAutismMetaData(file)
                preAutismMetaData.append(metaData)
                preAutismFileNames.append(fileName)
         
        for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.dat'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismData.append(data)
         
        for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.wl1'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthOneData.append(data)
         
        for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.wl2'):
            data = pd.DataFrame(np.genfromtxt(fileName))
            preAutismWavelengthTwoData.append(data)
                
        for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.evt'):
            data = pd.DataFrame(np.genfromtxt(fileName))  
            preAutismEventonsData.append(data)     
        
        for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.hdr'):
            with open(fileName, 'r', errors="ignore") as file:
                metaData = r.readPreAutismMetaData(file)
                preAutismMetaData.append(metaData)
                preAutismFileNames.append(fileName)
        
        transformedData.append(t.transformPreAutismFile(preAutismFileNames, preAutismMetaData, preAutismData, preAutismWavelengthOneData, preAutismWavelengthTwoData, preAutismEventonsData))
        
        l.loadDataToEnterpriseLayer(transformedData)

        


if __name__ == "__main__":
    ExtractTransformLoadHelper.main()


