# %%
from inference import inference
import os
import fname_ret as fnr
import shutil
import pandas as pd

# %%


def get_cattle_task(self, side_path, rear_path):
    data: dict = {}
    res = inference.predict(side_path, rear_path)
    # os.remove(side_path)
    # os.remove(rear_path)
    data.update(res)
    data.update({"cattle_id": celery.current_task.request.id})
    return data


dataPath = 'data/'
mPowerCsvPath = 'data/output tables/animals_management_weightsession.csv'
valoSanitizedExcelPath = 'data/output tables/221130_bhalo_Acme AI_Oct 2022_sanitized.xlsx'
csvDestination = 'data/output tables'
csv_name = 'valoAndMpowerMergedWithID.csv'

# %%


def mergedCSV(imgDataPath=dataPath, mpCsvPath=mPowerCsvPath, vExlPath=valoSanitizedExcelPath, outPath=csvDestination, outFname=csv_name):
    # raed imagaes from directory and list
    files = fnr.filname_ret(rootpath=imgDataPath,
                            file_types=('.jpg')).fileDirectory

    # read excel & csv files and convert all data to data frames
    # dfm = pd.read_csv(mpCsvPath, index_col=0)
    dfm = pd.read_csv(mpCsvPath)
    # dfv = pd.read_excel(vExlPath, index_col='ID Number (App)')
    dfv = pd.read_excel(vExlPath)
    dftf = pd.DataFrame(files, columns=['fileWithDirectory'])

    # extract names and expand dataframe
    dfm['sideImgNameOnly'] = dfm.side_img.str.split(pat='/', expand=True)[2]
    dfm['rearImgNameOnly'] = dfm.rear_img.str.split(pat='/', expand=True)[2]
    dftf['NameOnly'] = dftf.fileWithDirectory.str.split(
        pat='/', expand=True)[2]

    # merge valo and mpower dataframe
    df_mv = pd.merge(dfm, dfv, how='right', left_on='animal_id', right_on='ID Number (App)')


    df_mv = pd.merge(df_mv, dftf, how='left', left_on='sideImgNameOnly',
                     right_on='NameOnly').rename({'fileWithDirectory': 'sideImgLoc'}, axis='columns')
 
    df_mv = pd.merge(df_mv, dftf, how='left', left_on='rearImgNameOnly',
                     right_on='NameOnly').rename({'fileWithDirectory': 'rearImgLoc'}, axis='columns')

    # # filter image path  from dataframe
    # dftf_side = dftf[dftf.NameOnly.isin(df_mv.sideImgNameOnly)]

    # # copy image file path to dataframe
    # df_mv['sideImgLoc'] = dftf_side.fileWithDirectory.values
    # dftf_rear = dftf[dftf.NameOnly.isin(df_mv.rearImgNameOnly)]
    # df_mv['rearImgLoc'] = dftf_rear.fileWithDirectory.values

    # export to CSV
    df_mv.to_csv(os.path.join(outPath, outFname))


# %%
mergedCSV(imgDataPath=dataPath, mpCsvPath=mPowerCsvPath,
          vExlPath=valoSanitizedExcelPath, outPath=csvDestination, outFname=csv_name)
