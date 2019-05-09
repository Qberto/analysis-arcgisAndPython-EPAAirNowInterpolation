from arcgis.gis import GIS  # ArcGIS API for Python
from arcgis.features import GeoAccessor, GeoSeriesAccessor # Module functions to work with spatially-enablded dataframes
import pandas as pd         # Pandas is how we handle data in Python
import arcpy                # Access to ArcGIS Pro geoprocessing tools 
import numpy as np          # Access to python's scientific computing library
import scipy                # Modules for mathematics, science, and engineering
import os                   # Access to operating system commands
import requests             # Access to HTTPS commands to retrieve data
import getpass              # Access to secure handling of sensitive values (i.e. passwords, API keys)
from datetime import date, timedelta # Access to current date and time operations
import time                 # Time helper functions

class Toolbox(object):
    def __init__(self):
        self.label = "AirNow GeoProcessing Tools"
        self.alias = "AirNow"
        self.tools = [GetAirNowData_mostRecent,
                      GetAirNowData_forSpecifiedDate,
                      GetAirNowData_forSpecifiedDateRange,
                      CleanData_ForSpecifiedVariable]

class GetAirNowData_mostRecent(object):

    def __init__(self):
        self.label = "1. Get AirNow Data: Most Recent"
        self.description = "Uses the AirNow API to retrieve AirNow monitoring "+\
                           "station data."
        self.canRunInBackground = True

    def getParameterInfo(self):
        # Define parameter definitions

        out_featureclass = arcpy.Parameter(
            displayName="Output Feature Class",
            name="out_featureclass",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")

        # Input AirNow API Key
        airnow_api_key = arcpy.Parameter(
            displayName="AirNow API Key",
            name="airnow_api_key",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        airnow_api_key.value = "E7481512-D56F-E511-A1314CE18CD4A2B0"

        parameters = [out_featureclass, 
                      airnow_api_key]

        return parameters

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def create_airnow_api_url_request(self, date_string, airnow_api_key):
        # Helper function to build the AirNow API URL Request
        url = "https://www.airnowapi.org/airnowsitedata_{0}?API_KEY={1}".format(date_string, airnow_api_key)
        return url

    def get_most_recent_datestring(self):
        yesterday = date.today() - timedelta(1)
        date_string = yesterday.strftime('%Y%m%d')+"04"
        return date_string

    def execute(self, parameters, messages):
        
        # Instantiate parameters
        out_featureclass = parameters[0].valueAsText
        airnow_api_key = parameters[1].valueAsText

        arcpy.AddMessage("Determining most recent date...")
        date_string = self.get_most_recent_datestring()
        arcpy.AddMessage(f"Most recent date: {date_string}")

        arcpy.AddMessage("Retrieving AirNow data...")
        # Create request URL
        url = self.create_airnow_api_url_request(date_string, airnow_api_key)

        # Use Pandas to retrieve a csv from the AirNow API
        airnow_df = pd.read_csv(url)

        # Determine amount of records in the original data
        original_number_of_records = len(airnow_df)
        arcpy.AddMessage(f"There are {original_number_of_records} records in the dataset.")

        # Convert the dataframe to a spatially-enabled dataframe
        airnow_sdf = pd.DataFrame.spatial.from_xy(df=airnow_df, x_column="Longitude", y_column="Latitude", sr=4326)

        # Export the spatially-enabled dataframe to a feature class
        airnow_fc = airnow_sdf.spatial.to_featureclass(location=out_featureclass)

        return

class GetAirNowData_forSpecifiedDate(object):

    def __init__(self):
        self.label = "1. Get AirNow Data: For Specified Date"
        self.description = "Uses the AirNow API to retrieve AirNow monitoring "+\
                           "station data."
        self.canRunInBackground = True

    def getParameterInfo(self):
        # Define parameter definitions

        # Input Date String
        date = arcpy.Parameter(
            displayName="Date",
            name="date",
            datatype="GPDate",
            parameterType="Required",
            direction="Input")

        out_featureclass = arcpy.Parameter(
            displayName="Output Feature Class",
            name="out_featureclass",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")

        # Input AirNow API Key
        airnow_api_key = arcpy.Parameter(
            displayName="AirNow API Key",
            name="airnow_api_key",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        airnow_api_key.value = "E7481512-D56F-E511-A1314CE18CD4A2B0"

        parameters = [date, 
                      out_featureclass, 
                      airnow_api_key]

        return parameters

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def create_airnow_api_url_request(self, date_string, airnow_api_key):
        # Helper function to build the AirNow API URL Request
        url = "https://www.airnowapi.org/airnowsitedata_{0}?API_KEY={1}".format(date_string, airnow_api_key)
        return url

    def execute(self, parameters, messages):
        
        # Instantiate parameters
        date = parameters[0].value
        out_featureclass = parameters[1].valueAsText
        airnow_api_key = parameters[2].valueAsText

        arcpy.AddMessage("<<<Devnote>>> "+str(date))
        date_string = date.strftime('%Y%m%d%H')
        arcpy.AddMessage("<<<Devnote>>> "+str(date_string))

        arcpy.AddMessage("Retrieving AirNow data...")
        # Create request URL
        url = self.create_airnow_api_url_request(date_string, airnow_api_key)

        # Use Pandas to retrieve a csv from the AirNow API
        airnow_df = pd.read_csv(url)

        # Determine amount of records in the original data
        original_number_of_records = len(airnow_df)
        arcpy.AddMessage(f"There are {original_number_of_records} records in the dataset.")

        # Convert the dataframe to a spatially-enabled dataframe
        airnow_sdf = pd.DataFrame.spatial.from_xy(df=airnow_df, x_column="Longitude", y_column="Latitude", sr=4326)

        # Export the spatially-enabled dataframe to a feature class
        airnow_fc = airnow_sdf.spatial.to_featureclass(location=out_featureclass)

        return

class GetAirNowData_forSpecifiedDateRange(object):

    def __init__(self):
        self.label = "1. Get AirNow Data: For Specified Date Range"
        self.description = "Uses the AirNow API to retrieve AirNow monitoring "+\
                           "station data."
        self.canRunInBackground = True

    def getParameterInfo(self):
        # Define parameter definitions

        # Input Date String
        start_date = arcpy.Parameter(
            displayName="Start Date",
            name="date",
            datatype="GPDate",
            parameterType="Required",
            direction="Input")

        # Input Date String
        end_date = arcpy.Parameter(
            displayName="End Date",
            name="date",
            datatype="GPDate",
            parameterType="Required",
            direction="Input")

        interval = arcpy.Parameter(
            displayName="Time Interval",
            name="interval",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        interval.filter.type = "ValueList"
        interval.filter.list = ['Hourly','Daily','Weekly','Monthly']   

        out_featureclass = arcpy.Parameter(
            displayName="Output Feature Class",
            name="out_featureclass",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")

        # Input AirNow API Key
        airnow_api_key = arcpy.Parameter(
            displayName="AirNow API Key",
            name="airnow_api_key",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        airnow_api_key.value = "E7481512-D56F-E511-A1314CE18CD4A2B0"

        parameters = [start_date, 
                      end_date,
                      interval,
                      out_featureclass, 
                      airnow_api_key]

        return parameters

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def create_airnow_api_url_request(self, date_string, airnow_api_key):
        # Helper function to build the AirNow API URL Request
        url = "https://www.airnowapi.org/airnowsitedata_{0}?API_KEY={1}".format(date_string, airnow_api_key)
        return url

    def get_time_steps(start_date, end_date, interval):
        """ 
        Helper function: Given a start date object, end date object, and time step interval,
        returns a list of strings that can be used in the AirNow API.
        """
        

        return


    def execute(self, parameters, messages):
        
        # Instantiate parameters
        start_date = parameters[0].value
        end_date = parameters[1].value
        interval = parameterType[2].valueAsText
        out_featureclass = parameters[3].valueAsText
        airnow_api_key = parameters[4].valueAsText

        start_date_string = start_date.strftime('%Y%m%d%H')
        end_date_string = end_date.strftime('%Y%m%d%H')
        
        arcpy.AddMessage(str(date_string))

        arcpy.AddMessage("Retrieving AirNow data...")
        # Create request URL
        url = self.create_airnow_api_url_request(date_string, airnow_api_key)

        # Use Pandas to retrieve a csv from the AirNow API
        airnow_df = pd.read_csv(url)

        # Determine amount of records in the original data
        original_number_of_records = len(airnow_df)
        arcpy.AddMessage(f"There are {original_number_of_records} records in the dataset.")

        # Convert the dataframe to a spatially-enabled dataframe
        airnow_sdf = pd.DataFrame.spatial.from_xy(df=airnow_df, x_column="Longitude", y_column="Latitude", sr=4326)

        # Export the spatially-enabled dataframe to a feature class
        airnow_fc = airnow_sdf.spatial.to_featureclass(location=out_featureclass)

        return        


class CleanData_ForSpecifiedVariable(object):

    def __init__(self):
        self.label = "2. Clean Data: Using Specified Variable"
        self.description = "Cleans station data using expected missing data value of -999."
        self.canRunInBackground = True

    def getParameterInfo(self):
        # Define parameter definitions

        # Input Date String
        date = arcpy.Parameter(
            displayName="Date",
            name="date",
            datatype="GPDate",
            parameterType="Required",
            direction="Input")

        out_featureclass = arcpy.Parameter(
            displayName="Output Feature Class",
            name="out_featureclass",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")

        # Input AirNow API Key
        airnow_api_key = arcpy.Parameter(
            displayName="AirNow API Key",
            name="airnow_api_key",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        airnow_api_key.value = "E7481512-D56F-E511-A1314CE18CD4A2B0"

        remove_missing_data = arcpy.Parameter(
            displayName="Remove Missing Data",
            name="remove_missing_data",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")
        remove_missing_data.value = False

        parameters = [date, 
                      out_featureclass, 
                      airnow_api_key, 
                      remove_missing_data]

        return parameters

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def create_airnow_api_url_request(self, date_string, airnow_api_key):
        # Helper function to build the AirNow API URL Request
        url = "https://www.airnowapi.org/airnowsitedata_{0}?API_KEY={1}".format(date_string, airnow_api_key)
        return url

    def execute(self, parameters, messages):
        
        # Instantiate parameters
        date = parameters[0].value
        out_featureclass = parameters[1].valueAsText
        airnow_api_key = parameters[2].valueAsText
        remove_missing_data = parameters[3].value

        arcpy.AddMessage(str(date))

        date_string = date.strftime('%Y%m%d%H')
        arcpy.AddMessage(str(date_string))

        arcpy.AddMessage("Retrieving AirNow data...")
        # Create request URL
        url = self.create_airnow_api_url_request(date_string, airnow_api_key)

        # Use Pandas to retrieve a csv from the AirNow API
        airnow_df = pd.read_csv(url)

        # Determine amount of records in the original data
        original_number_of_records = len(airnow_df)
        arcpy.AddMessage(f"There are {original_number_of_records} records in the dataset.")

        # Perform a query to remove stations with missing data
        if remove_missing_data:
            arcpy.AddMessage("Pre-query record count: {0}".format(airnow_df.shape[0]))
            airnow_df = airnow_df.query("PM25_AQI > -999")
            arcpy.AddMessage("Post-query record count: {0}".format(airnow_df.shape[0]))

        # Convert the dataframe to a spatially-enabled dataframe
        airnow_sdf = pd.DataFrame.spatial.from_xy(df=airnow_df, x_column="Longitude", y_column="Latitude", sr=4326)

        # Export the spatially-enabled dataframe to a feature class
        airnow_fc = airnow_sdf.spatial.to_featureclass(location=out_featureclass)

        return