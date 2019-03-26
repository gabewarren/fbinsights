
# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adset import AdSet
import pandas as pd
import sqlalchemy as sa
import csv
from yaml import safe_load, load, dump




stream = open('config.yaml', 'r') 
config = safe_load(stream)

engine = sa.create_engine(f"redshift+psycopg2://{config['redshift']['user']}:{config['redshift']['pass']}@{config['redshift']['host']}:{config['redshift']['port']}/{config['redshift']['db']}")

access_token = config['Facebook']['access_token']
ad_account_id = config['Facebook']['ad_account_id']
app_secret = config['Facebook']['app_secret']
app_id = config['Facebook']['app_id']
FacebookAdsApi.init(access_token=access_token)


fields = [
   'reach',
    'impressions',
    'spend',
    'cpm',
    'cpp',    
    'frequency',    
    'cpc',
    'account_name',
    'account_id',
]
params = {
    'level': 'account',
    'filtering': [],
    'breakdowns': ['dma'],
    'time_range': {'since': config['params']['time_range']['since'],'until':config['params']['time_range']['until']},
}

#Export FB AD insights and write to CSV
def getAccountInsights():
    insights = AdAccount(ad_account_id).get_insights(
        fields=fields,
        params=params,
    )

    

    addata = [x for x in  insights]
    df = pd.DataFrame(addata)
    df.to_csv(config['params']['csvfilename'], encoding='utf-8', index=False)

#Create or update Redshift table using CSV dump, replaces existing rows if exists
def writeInsightsToRedis():
  fbdata = pd.read_csv(config['params']['csvfilename'])
  fbdata.to_sql(config['redshift']['tablename'], con=engine, if_exists='replace', index=False)

getAccountInsights()
writeInsightsToRedis()