import pandas as pd
import numpy as np
import io
import requests
from requests_ntlm import HttpNtlmAuth
from pathlib import Path

def xlsxUrlToDf(url, usr, pswrd, sheet=0):
    '''
        This function loads a protected Excel file (.xlsx) stored in a
        remote data repository directly to a Pandas DataFrame.
        
        Params
        ------
            * url <str>: The url of the protected .xlsx file.
            * usr <str>: Username to login to the data repository.
            * pswrd <str>: Password to login to the data repository.
            * sheet <str, int, list, or None, default 0>: Sheet(s) to read data from.
        
        Returns
        -------
        <pandas.DataFrame>: Pandas dataframe containing the .xlsx data.
    '''

    # HTTP requesting for the .xlsx file
    response = requests.get(url, 
                        auth=HttpNtlmAuth(usr, pswrd))
    
    # Exception to reporteHistorico 2221
    if 'REPORTE%20HISTORICO-2221.xlsx' in url:
        sheet = 'Hoja1'
        
    # writing the content of the HTTP response to a Pandas Dataframe
    with io.BytesIO(response.content) as fh:
        if 'hist-2141-28Enero2014.csv' in url:
            df = pd.read_csv(fh, encoding='latin-1')
        else:

            if 'xlsx' in url:
                if "hist-2161-28012016.xlsx" in url or "hist-2152-25082015.xlsx" in url or "hist-2151-11022015.xlsx" in url or "hist-2142-todos-23septiembre2014.xlsx" in url:
                    df = pd.read_excel(fh, sheet_name=sheet, engine='openpyxl', skiprows=1)
                else:
                    df = pd.read_excel(fh, sheet_name=sheet, engine='openpyxl')
            elif  'xls' in url:
                df = pd.read_excel(fh, sheet_name=sheet)

    
    return df

# ---------------------------------------------------------------------------------------------

def applyCorrectedSchema(df, schema):
    '''
        This function applies some schema to a dataframe
        
        Params
        ------
            * df <pandas.DataFrame>: Original dataframe (as downloaded).
            * schema <str> Path to the .xlsx schema to be applied.
        
        Returns
        -------
        <pandas.DataFrame>: Pandas dataframe with the applied schema.
    '''
    schema = pd.read_excel(schema)

    # Removing the not present variables in "original" column of the schema 
    df = df[schema.original.values]

    # Renaming the columns
    df = df.rename(columns=dict(zip(schema.original, schema.nuevo)))

    # If "ciclo" variables is written as str, then fix it (for "procesoAdmision" data)
    if 'ciclo' in df.columns.values:
        if df.ciclo.dtype == 'O':
            df.ciclo = df.ciclo.str.slice(0,4)

    # Assigning correct data type to all variables
    df = df.astype(dict(zip(schema.nuevo, schema.tipo)), errors='ignore')

    # Replace 'nan' string with pd.NA value
    df = df.replace('nan', np.nan)

    return df

# ---------------------------------------------------------------------------------------------

def downloadAndCorrectSchema(file, usr, pwrd, indicatorString, correctedSchema):
    '''
        This function automatically downloads and apply the corrected
        schema to the datasets.
        
        Params
        ------
            * file <str>: File URL.
            * usr <str>: Username to login to the data repository.
            * pwrd <str>: Password to login to the data repository.
            * indicatorString <str>: String to print while process is completed.
            * correctedSchema <str>: Path to the .xlsx schema to be applied.
        
        Returns
        -------
        <pandas.DataFrame>: Pandas dataframe with the dataset and its corrected
                            schema applied.
    '''
    print(indicatorString, end=' ')
    df = applyCorrectedSchema(
        xlsxUrlToDf(file, usr, pwrd),
        correctedSchema
    )
    print('OK')

    return df