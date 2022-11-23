# %%
import os
import sys
import glob
import pandas as pd
import numpy as np

# %%
class FileReader():  
    
    #Class constructor
    def __init__(self):
        """Class constructor"""
        self.__filename = ""
    
    def _findField(self,file, fieldName):
        
        currentLine = file.readline()
        if fieldName in currentLine:
            currentLine = currentLine.split(fieldName)
            return  ''.join(currentLine[1])
        else:
            return self._findField(file, fieldName)
        
    def readVMFile(self,filename, file):

        self._filename = filename

        metadata = {}
        fields = ['ID','Name','Age','Sex','AnalyzeMode','Pre Time[s]','Post Time[s]','Recovery Time[s]','Base Time[s]','Date','Mode','Wave[nm]','Sampling Period[s]','StimType','Stim Time[s]','Repeat Count']
        for field in fields:
            if 'Stim Time[s]' != field:
                metadata[field] = self._findField(file, field).replace('\n','').lstrip(',').rstrip(',')
            else:
                temp = self._findField(file, field)
                fieldList = file.readline().replace('\n','').replace(',,','').split(",")
                fieldDict ={}
                for i in range(len(fieldList) - 1):
                    if i % 2 == 0:
                        fieldDict[fieldList[i]] = fieldList[i+1]
                
                metadata[field] = fieldDict
            
        lineString = self._findField(file,'Data')
        data = pd.read_csv(file)
        
        return (metadata,data)
    
    def getParameters(self, file, field, params):
        parameters = {}
        self._findField(file, field)
        
        for field in params:
            parameters[field] = self._findField(file, field).replace('\n','').replace('\"','').replace('=','').replace('\t',',')
            
        return parameters
    
    def getArray(self, file, field, params, arrayFields):
        fieldArray = {}
        fieldArray = self.getParameters(file, field, params)
        
        for key, value in arrayFields.items():
            self._findField(file, key)
            arrayRows = []
            for i in range(value):
                arrayRows.append(file.readline())
            fieldArray[key] = arrayRows
            
        
        return fieldArray
        
    
    
    def readPreAutismMetaData(self,filename, file):
        
        metadata = {}
        
        GeneralInfoFields = ['FileName','Date','Time','Device','Source','Mod','APD','NIRStar','Subject']
        # ImagingParametersFields = ['Sources','Detectors','ShortDetectors','Steps','Wavelengths','TrigIns','TrigOuts','AnIns','SamplingRate','Mod Amp','Threshold']
        ImagingParametersFields = ['Sources','Detectors','ShortBundles','ShortDetIndex','Steps','Wavelengths','TrigIns','TrigOuts','AnIns','SamplingRate','Mod Amp','Threshold']
        ParadigmFields = ['StimulusType']
        ExperimentNotesFields = ['Notes']
        GainSettingsFields = []
        GainSettingsArrayFields = {'Gains': 11}
        MarkersFields = []
        MarkersArrayFields = {'Events': 28}
        DataStructureFields = ['S-D-Key']
        DataStructureArrayFields = {'S-D-Mask': 11}
            
        metadata["GeneralInfo"] = self.getParameters(file, "GeneralInfo", GeneralInfoFields)
        metadata["ImagingParameters"] = self.getParameters(file, "ImagingParameters", ImagingParametersFields)
        metadata["Paradigm"] = self.getParameters(file, "Paradigm", ParadigmFields)
        metadata["ExperimentNotes"] = self.getParameters(file, "ExperimentNotes", ExperimentNotesFields)
        metadata["GainSettings"] = self.getArray(file, "GainSettings", GainSettingsFields, GainSettingsArrayFields)
        metadata["Markers"] = self.getArray(file, "Markers", MarkersFields, MarkersArrayFields)
        metadata["DataStructure"] = self.getArray(file, "DataStructure", DataStructureFields, DataStructureArrayFields)
        
        return metadata
        
        
    
        

# %%
import numbers

from sympy import python


class FileReaderHelper:
    def main():
        r=FileReader()
        
        # #Vm Data
        # for fileName in glob.glob('data/VMData_Blinded/*_HBA_Probe1_Deoxy.csv'):
        #     print(fileName)
        #     with open(fileName, 'r', errors="ignore") as file:
        #         metadata, data = r.readVMFile(fileName, file)
        #         print(metadata)
        #         print(data.shape)
            
        # for fileName in glob.glob('data/VMData_Blinded/*_HBA_Probe1_Oxy.csv'):
        #     print(fileName)
        #     with open(fileName, 'r', errors="ignore") as file:
        #         metadata, data = r.readVMFile(fileName, file)
        #         print(metadata)
        #         print(data.shape)
        
        # for fileName in glob.glob('data/VMData_Blinded/*_MES_Probe1.csv'):
        #     print(fileName)
        #     with open(fileName, 'r', errors="ignore") as file:
        #         metadata, data = r.readVMFile(fileName, file)
        #         print(metadata)
        #         print(data.shape)
                
        # # Pre Autism Data
        
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.dat'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
        
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.dat'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
            
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.wl1'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
          
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.wl1'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
            
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.wl2'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
        #     data = np.genfromtxt(fileName)
        #     print(data)
            
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.wl2'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
                    
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.evt'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
            
        # for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.evt'):
        #     print(fileName)
        #     data = np.genfromtxt(fileName)
        #     print(data)
            
        for fileName in glob.glob('data/PreAutismData_Blinded/*_NormalConversation/*.hdr'):
            print(fileName)
            with open(fileName, 'r', errors="ignore") as file:
                metadata = r.readPreAutismMetaData(fileName, file)
                print(metadata)
        
        for fileName in glob.glob('data/PreAutismData_Blinded/*_StressedConversation/*.hdr'):
            print(fileName)
            with open(fileName, 'r', errors="ignore") as file:
                metadata = r.readPreAutismMetaData(fileName, file)
                print(metadata)
            
        
        


        
        
        
    
        

# %%
if __name__ == "__main__":
    ##COMPLETE THIS PART
    FileReaderHelper.main()


