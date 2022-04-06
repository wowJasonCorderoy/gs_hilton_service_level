import os
import re
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from datetime import datetime,timezone
import hashlib
import openpyxl

#### Declare functions
def is_correctFileName(fileName: str = None, regex: str = r".*service level report.*xls[x]?$") -> bool:
    """[summary]

    Args:
        fileName (str, optional): name of file. Defaults to None.
        regex (str, optional): regex expression to evaluate. Defaults to r".*service level report.*xls[x]?$".

    Returns:
        bool: if True then regex matched fileName else False
    """
    import re

    if re.match(regex, fileName, re.IGNORECASE):
        return True
    else:
        return False

def infer_site(filename: str):
    if 'trug' in filename.lower():
        return 'Truganina'
    elif 'heathwood' in filename.lower():
        return 'Heathwood'
    elif 'hw' in filename.lower():
        return 'Heathwood'
    elif 'bun' in filename.lower():
        return 'Bunbury'
    else:
        return 'Other'

# def get_date(file_path: str, sheet_name: str = "Overview"):
#     import re
#     import openpyxl
#     import datetime
#     wb = openpyxl.load_workbook(file_path, data_only=True)
#     dat = wb[sheet_name]['D1'].value
#     extract_date = re.findall("([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,})",dat)[-1]
#     formatted_date = datetime.datetime.strptime(extract_date, "%d/%m/%Y")#.strftime("%Y-%m-%d")
#     return formatted_date

def get_date(filename: str):
    import re
    import datetime
    extract_date = re.findall("([0-9]{1,2}[./-]?[0-9]{1,2}[./-]?[0-9]{2,})",filename)[-1]
    extract_date_clean = re.sub("[./-]","",extract_date)
    formatted_date = datetime.datetime.strptime(extract_date_clean, "%d%m%Y")#.strftime("%Y-%m-%d")
    return formatted_date

def load_condensed_masterdata(file_path: str, da_date: datetime.date, site: str, sheet_name: str = "Master Data"):
    headers = ['WOW_MATERIAL_CODE', 'MATERIAL_DESCRIPTION', 'PRODUCT_SOURCE', 'VALUE_ADD_FLAG']
    dtypes = {'WOW_MATERIAL_CODE': 'str',
              'MATERIAL_DESCRIPTION': 'str',
              'PRODUCT_SOURCE': 'str',
              'VALUE_ADD_FLAG': 'str',
            }
    df = pd.read_excel(file_path, names=headers, dtype=dtypes, usecols="D:G", sheet_name=sheet_name)
    df['filename_date'] = da_date
    df['filename_site'] = site
    return ("hilton_masterdata", df)

def load_service_level_data(file_path: str, da_date: datetime.date, site: str, sheet_name: str = "Service Level Data"):
    headers = ['SERVICE_GROUP', 'PRODUCT_SOURCE', 'VALUE_ADD_FLAG', 'REASON_CODE', 'REASON_DESCRIPTION', 'SHORTAGE_QTY', 'PROMO_FLAG', 'COMMENTS', 'STATE', 'PLNT',
               'ITEM', 'SOLD_TO_PT', 'PURCHASE_ORDER_NO', 'SOLD_TO_PARTY', 'MATERIAL', 'MATERIAL_NUMBER', 'CUSTOMER_MATERIAL_NUMBER', 'EAN_UPC', 'DC_FLAG',
               'MAT_AV_DT', 'ORDER_QUANTITY', 'DELIVERED_QTY', 'DELIVERED_WEIGHT']
    dtypes = {'SERVICE_GROUP': 'str', 
              'PRODUCT_SOURCE': 'str', 
              'VALUE_ADD_FLAG': 'str',
              'REASON_CODE': 'str',
              'REASON_DESCRIPTION': 'str',
              'SHORTAGE_QTY': np.float64,
              'PROMO_FLAG': np.float64,
              'COMMENTS': 'str',
              'STATE': 'str',
              'PLNT': 'str',
              'ITEM': 'str',
              'SOLD_TO_PT': 'str',
              'PURCHASE_ORDER_NO': 'str',
              'SOLD_TO_PARTY': 'str',
              'MATERIAL': 'str',
              'MATERIAL_NUMBER': 'str',
              'CUSTOMER_MATERIAL_NUMBER': 'str',
              'EAN_UPC': 'str',
              'DC_FLAG': 'str',
              'ORDER_QUANTITY': np.float64,
              'DELIVERED_QTY': np.float64,
              'DELIVERED_WEIGHT': np.float64,
            }
    parse_dates = ['MAT_AV_DT']
    df = pd.read_excel(file_path, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:W", sheet_name=sheet_name)
    df['filename_date'] = da_date
    df['filename_site'] = site
    # drop useless rows... where ITEM is missing
    df = df[df['ITEM'].notna()]
    return ("hilton_servicelevel",df)

def load_servicegroup_data(file_path: str, da_date: datetime.date, site: str, sheet_name: str = "Service Group"):
    headers = ['DEPT', 'WOW_MATERIAL_CODE', 'MATERIAL_DESCRIPTION', 'PRODUCT_SOURCE', 'SPECIES', 'SERVICE_GROUP']
    dtypes = {'DEPT': 'str',
              'WOW_MATERIAL_CODE': 'str',
              'MATERIAL_DESCRIPTION': 'str',
              'PRODUCT_SOURCE': 'str',
              'SPECIES': 'str',
              'SERVICE_GROUP': 'str',
            }
    df = pd.read_excel(file_path, names=headers, dtype=dtypes, usecols="A:F", sheet_name=sheet_name)
    df['filename_date'] = da_date
    df['filename_site'] = site
    return ("hilton_servicegroup",df)

def load_forecast_data(file_path: str, da_date: datetime.date, site: str, sheet_name: str = "Forecast Data"):
    headers = ['PROMO_FLAG', 'PRODUCT_SOURCE', 'PLANT', 'MATERIAL_NUMBER', 'WOWNR', 'DESCRIPTION', 'PLANT_MATERIAL_STATUS', 'DATE', 'FORECAST', 
               'ACTUAL_SALES', 'ACTUAL_TPRP', 'LAST_OLD_TPRP', '_1_WEEK_OLD_FORECAST', '_2_WEEKS_OLD_FORECAST', '_3_WEEKS_OLD_FORECAST', 
               '_4_WEEKS_OLD_FORECAST',	'_5_WEEKS_OLD_FORECAST', '_1_WEEK_OLD_TPRP', '_2_WEEKS_OLD_TPRP', '_3_WEEKS_OLD_TPRP',
               '_4_WEEKS_OLD_TPRP', '_5_WEEKS_OLD_TPRP']
    dtypes = {'PROMO_FLAG': np.float64,
               'PRODUCT_SOURCE': 'str',
               'PLANT': 'str',
               'MATERIAL_NUMBER': 'str',
               'WOWNR': 'str',
               'DESCRIPTION': 'str',
               'PLANT_MATERIAL_STATUS': 'str',
               'FORECAST': np.float64, 
               'ACTUAL_SALES': np.float64, 
               'ACTUAL_TPRP': np.float64,
               'LAST_OLD_TPRP': np.float64,
               '_1_WEEK_OLD_FORECAST': np.float64,
               '_2_WEEKS_OLD_FORECAST': np.float64,
               '_3_WEEKS_OLD_FORECAST': np.float64, 
               '_4_WEEKS_OLD_FORECAST': np.float64,
               '_5_WEEKS_OLD_FORECAST': np.float64,
               '_1_WEEK_OLD_TPRP': np.float64,
               '_2_WEEKS_OLD_TPRP': np.float64,
               '_3_WEEKS_OLD_TPRP': np.float64,
               '_4_WEEKS_OLD_TPRP': np.float64,
               '_5_WEEKS_OLD_TPRP': np.float64
            }
    parse_dates = ['DATE']
    df = pd.read_excel(file_path, names=headers, dtype=dtypes, parse_dates=parse_dates, usecols="A:V", sheet_name=sheet_name)
    df['filename_date'] = da_date
    df['filename_site'] = site
    # drop useless rows... where ITEM is missing
    df = df[df['MATERIAL_NUMBER'].notna()]
    return ("hilton_forecast",df)

def load_customer_data(file_path: str, da_date: datetime.date, site: str, sheet_name: str = "Customer Master"):
    headers = ['CUSTOMER', 'NAME', 'STATE', 'FAIR_SHARE', 'PALLET_HEI', 'LOW_CODE', 'STORE_SIZE', 'STORE_LEAD',
               'SALES_ORG', 'DISTR_CHANNEL', 'DIVISION', 'SHIP_CONDITIONS', 'DEL_PLANT', 'DC', 'ORDER_COMB',
               'MAX_PART_D', 'PART_DEL_PER_ITEM']
    dtypes = {'CUSTOMER': 'str',
              'NAME': 'str',
              'STATE': 'str',
              'FAIR_SHARE': 'str',
              'PALLET_HEI': 'str',
              'LOW_CODE': 'str',
              'STORE_SIZE': 'str',
              'STORE_LEAD': 'str',
              'SALES_ORG': 'str',
              'DISTR_CHANNEL': 'str',
              'DIVISION': 'str',
              'SHIP_CONDITIONS': 'str',
              'DEL_PLANT': 'str',
              'DC': 'str',
              'ORDER_COMB': 'str',
              'MAX_PART_D': 'str',
              'PART_DEL_PER_ITEM': np.float64,
            }
    df = pd.read_excel(file_path, names=headers, dtype=dtypes, usecols="A:Q", sheet_name=sheet_name)
    df['filename_date'] = da_date
    df['filename_site'] = site
    return ("hilton_customer", df)

### Save to bigquery
def get_bq_credentials():
    try:
        client = service_account.Credentials.from_service_account_file(**{
        'filename':CREDS_FILE_LOC, 
        'scopes':["https://www.googleapis.com/auth/cloud-platform"],
        })
    except:
        client = bigquery.Client(project=PROJECT_ID)
    return client
    
#### Declare constants
PROJECT_ID = 'gcp-wow-pvc-grnstck-prod'

project_creds_file_map = {
    'gcp-wow-pvc-grnstck-prod':r"C:\dev\greenstock\optimiser_files\key_prod.json",
    'gcp-wow-pvc-grnstck-dev':r"C:\dev\greenstock\optimiser_files\key_dev.json",
}
CREDS_FILE_LOC = project_creds_file_map.get(PROJECT_ID)

now_utc = datetime.now(timezone.utc) # timezone aware object, unlike datetime.utcnow().
#now_utc_str = now_utc.strftime("%Y/%m/%d %H:%M:%S")

filename = "Heathwood Service Level Report - 16032022.xlsx" #"Truganina Service Level Report -29.03.2022.xlsx"
file_path = os.path.abspath(filename)
da_date = get_date(filename)
da_date_string = da_date.strftime("%Y-%m-%d")
site = infer_site(filename)

#### run time
def run_local():
    if not is_correctFileName(filename):
            print(f"File {filename} is not correctly named. ABORTING.")
            return

    file_contents_md5 = hashlib.md5(open(filename,'rb').read()).hexdigest()
    
    func_2_run = [load_condensed_masterdata, load_service_level_data, load_servicegroup_data, load_forecast_data, load_customer_data]
    for func in func_2_run:
        tbl_name, df = func(file_path, da_date, site)
        df['upload_utc_dt'] = now_utc
        df['filename'] = filename
        df['file_contents_md5'] = file_contents_md5
        # save to BQ  
        credentials = get_bq_credentials()
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = PROJECT_ID
        bq_ds_tbl = f'hilton.{tbl_name}'
        print(f"writing {bq_ds_tbl}")
        pd.io.gbq.to_gbq(df, bq_ds_tbl, PROJECT_ID, chunksize=100000, reauth=False, if_exists='append')

if __name__ == "__main__":
    run_local()
