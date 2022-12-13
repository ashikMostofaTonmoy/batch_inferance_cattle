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


# %%
path = 'data/'
files = fnr.filname_ret(rootpath=path, file_types=('.jpg')).fileDirectory
dfm = pd.read_csv(
    'data/output tables/animals_management_weightsession.csv', index_col=0)
# dfm = pd.read_csv('data/output tables/animals_management_weightsession.csv')
dfv = pd.read_excel(
    'data/output tables/221130_bhalo_Acme AI_Oct 2022_sanitized.xlsx', index_col='ID Number (App)')
# %%
dfside = dfm.side_img.str.split(pat='/', expand=True)[2]
dfrear = dfm.rear_img.str.split(pat='/', expand=True)[2]

# %%
dftf = pd.DataFrame(files, columns=['fileWithDirectory'])
# %%
dfm['sideImgNameOnly'] = dfm.side_img.str.split(pat='/', expand=True)[2]
# %%
dfm['rearImgNameOnly'] = dfm.rear_img.str.split(pat='/', expand=True)[2]
# %%
# dftf = dftf.set_index('fileWithDirectory')
# %%
dftf['NameOnly'] = dftf.fileWithDirectory.str.split(pat='/', expand=True)[2]

# %%
df_mv = pd.merge(dfm, dfv, how='right', left_on='id',
                 right_on='ID Number (App)')

# %%
dftf_side = dftf[dftf.NameOnly.isin(df_mv.sideImgNameOnly)]
df_mv['sideImgLoc'] = dftf_side.fileWithDirectory.values

# %%
dftf_rear = dftf[dftf.NameOnly.isin(df_mv.rearImgNameOnly)]

df_mv['rearImgLoc'] = dftf_rear.fileWithDirectory.values

# %%
destination = 'data/output tables'
savefile_name = 'valoAndMpowerMergedWithID.csv'
df_mv.to_csv(os.path.join(destination, savefile_name))


# %%
