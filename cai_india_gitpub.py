# -*- coding: utf-8 -*-


"""
    writen by:     mua
    ver:     1.0
    date:     2017/06
    func：      catch india  cotton spot rates datas every day
    data from ： http://www.caionline.in/site/spot_rates

"""



# 印度棉花协会
import os
import pandas as pd
os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'
import cx_Oracle

from datetime import datetime
from datetime import date
from sqlalchemy import create_engine


print '\n\n-----------------------------BEGIN CAI INDIA--------------------\n\n'
url5="http://www.caionline.in/site/spot_rates"
data3=pd.read_html(url5)



data6 =data3[1]

data6.columns.tolist()



data30=data3[0]

cotime=datetime.strptime(data3[0].iloc[0,0],'%a %d %B, %Y')

data5 =pd.read_html(url5)[1]



data5.fillna(0,inplace=True)

data5['sportday']=cotime


data5['PerCandy']=data5.iloc[:,8].astype(float)
data5[' Per Candy']=data5[' Per Candy'].astype(float)






# 调整新插入列sportday的顺序，使得看起来更舒服，习惯
col1=['No.','sportday','Growth','Grade Standard','Grade','Staple','Micronaire','Strength/ GPT','Per Quintal',' Per Candy','PerCandy']
# print parms_pd.reindex(columns=col1)
data5=data5.reindex(columns=col1)





col2={'No.':'no','sportday':'sportday','Growth':'growth','Grade Standard':'gradestandard','Grade':'grade','Staple':'staple','Micronaire':'micronaire','Strength/ GPT':'strengthgpt','Per Quintal':'perquintal',' Per Candy':'PerCandy_del','PerCandy':'percandy'}
data5=data5.rename(columns=col2)



data5=data5.drop('PerCandy_del',axis=1)


data5['inserttime']=datetime.now()




data51=data5.iloc[[8,11,13,15,18]]
print '\n \n data51 is \n',data51

# change to your oracle env ,if not error to oracle
engine = create_engine('oracle+cx_oracle://xxx:xxx@192.xxx/xx')

cnx = engine.connect()
data51.to_sql('ods_india_cai',engine,if_exists='append',index=False)
cnx.close()


print "\n \n Success \n"
print cotime
print '\n\n-----------------------------END CAI INDIA--------------------\n\n'