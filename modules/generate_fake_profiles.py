#generate_data.py  
from faker import Faker 
import pandas as pd 
import time
#from fastparquet import write

faker = Faker()
df = pd.DataFrame() 

for i in range(1000): 
	df = df.append(faker.profile(),ignore_index=True)

timestr = time.strftime("%Y%m%d-%H%M%S") 


df.to_csv(r'/Users//Fake-Apache-Log-Generator/fake_profiles_access_log_'+timestr+'.csv',index=None)





