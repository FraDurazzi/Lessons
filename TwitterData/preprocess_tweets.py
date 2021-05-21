import json
import pandas as pd
import numpy as np
import os
import time
import multiprocessing as mp
import re
import matplotlib.dates as mdates
from functools import partial
import pickle
import glob
from tqdm import tqdm 
from datetime import date

### This script can be used to preprocess all the tweets collected by streamer.py
#   The tweets are preprocessed and relevant attributes are stored to a pandas DataFrame, saved to hard disk in .pickle format.
#   To load the DataFrame saved with this script, use:
#   with open("data/df_"+day_max+'.pickle','rb') as f:
#       df=pickle.load(f)

# You will preprocess only the tweets before this date
# In order to avoid incomplete days at beginning and end of the collection, 
# the first and last day are discarded
day_max='2021-05-31'

directory='data/tweets'
dictionary_list=[]
data=[]
lst=[f for f in glob.glob(directory+'/'+"*.jsonl")]
lst.sort()

file_max=directory+'/'+day_max.replace('-','_')+'.jsonl'
print('Last file to be loaded: ',file_max)
lst=[lf for lf in lst if lf[12:22] <file_max[12:22]]

temp= pd.read_json(lst[0], lines=True)
temp.sort_values('created_at',inplace=True)
print('Min date: ',temp.created_at.iloc[0], ' id: ',temp['id_str'].iloc[0])
temp= pd.read_json(lst[-1], lines=True)
temp.sort_values('created_at',inplace=True)
print('Max date: ',temp['created_at'].iloc[-1], ' id: ',temp['id_str'].iloc[-1])


def parse_jsonl(f):
    temp= pd.read_json(f, lines=True)
    df=pd.DataFrame()
    df['created_at']=pd.DatetimeIndex(pd.to_datetime(temp.created_at,unit='ns')).tz_convert('Europe/Rome')
    df['id']=[str(i) for i in temp.id_str.values]
    df['text']=temp.text
    df['full_text']=[str(st['full_text']) if isinstance(st,dict) else np.nan for st in temp.extended_retweet.values]
    df['user.id']=[str(u['id_str'] )for u in temp.user.values]
    df['lang']=temp.lang
    df['user.screen_name']=[u['screen_name'] for u in temp.user.values]
    #df['geo']=temp.geo
    #df['coordinates']=temp.coordinates
    df['place']=temp.place
    df['url']=[[ur['expanded_url']  for ur in st['urls']] if isinstance(st,dict) else np.nan for st in temp.entities.values]
    df['retweeted_status.id']=[str(st['id_str']) if isinstance(st,dict) else np.nan for st in temp.retweeted_status.values]
    df['retweeted_status.user.id']=[str(st['user']['id_str']) if isinstance(st,dict) else np.nan for st in temp.retweeted_status.values]
    df['retweeted_status.created_at']=[pd.to_datetime(st['created_at'],unit='ns').tz_convert('Europe/Rome') 
                                       if isinstance(st,dict) else np.nan for st in temp.retweeted_status.values]
    df['retweeted_status.place']=[str(st['place']) if isinstance(st,dict) else np.nan for st in temp.retweeted_status.values]
    df['retweeted_status.url']=[[ur['expanded_url']  for ur in st['entities']['urls']] if isinstance(st,dict) else np.nan for st in temp.retweeted_status.values]
    return df

# Parallelize the preprocessing to speed up linearly the execution
ncores=4
pool=mp.Pool(ncores)
df=pd.concat(list(tqdm(pool.imap(parse_jsonl, lst), total=len(lst))),
            sort=False,ignore_index=True)
pool.close()
pool.join()

print('Loading before ',day_max)
df=df.loc[(df.created_at>=df.created_at.min()+pd.DateOffset(days=1,normalize=True)) & (df.created_at<day_max)]

# We use 'text' as the unique textual column, also for tweets having an extended text
df.loc[~df['full_text'].isnull(),'text']=df.loc[~df['full_text'].isnull(),'full_text']
df.drop('full_text',axis=1,inplace=True)
df['text']=df.text.apply(str)

print(df.shape[0], ' tweets saved to df')
print(df.head())
print(df.tail())
with open('data/df_'+str(day_max)+'.pickle','wb') as f:
    pickle.dump(df,f)
print('DataFrame saved to pickle!')

# Uncomment this if you want to create directly an edgelist of the directed retweet network:
#df_edgelist=df[['user.id','retweeted_status.user.id']].copy()
#df_edgelist=df_edgelist.dropna(how='any')
#df_edgelist=df_edgelist.groupby(df_edgelist.columns.tolist()).size().reset_index().rename(
#    columns={0:'weight'})
#print('Edgelist: ',df_edgelist.shape)
#df_edgelist.to_csv('data/edgelist.txt', header=None, index=None, sep='\t')
#print('Edgelist saved to txt! (until {})'.format(day_max))


