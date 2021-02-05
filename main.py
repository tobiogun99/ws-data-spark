#!/usr/bin/env python
# coding: utf-8

# # Table of Contents
#     0. Preparing Data
#     1. Cleanup
#     2. Labels
#     3. Analysis
#     4. Model

# In[140]:


#import necessary libraries
import findspark
findspark.init()
import math
import pandas as pd
import pyspark.sql
import pyspark.sql.functions as F
sc = SparkSession     .builder     .master("spark://master:7077")     .appName("EQ Work Sample Problems ")     .getOrCreate()


# # 0. Preparing Data

# In[135]:


df = pd.read_csv("Data/DataSample.csv" )


# In[117]:


#see the data we're dealing with
df.head(3)


# In[109]:


#see how many rows before sorting the data (so we know it works)
df.count()


# # 1. Cleanup

# In[207]:


#change some of the column headings to be more user friendly
df = data_sample.rename(columns ={'_ID':'ID',' TimeSt':'TimeSt'})


# In[209]:


#remove records with duplicate time stamps and geoinfo, not keeping the initial record
df.drop_duplicates(subset=["TimeSt","Latitude", "Longitude"], keep = False, inplace =True)


# In[130]:


df.count()
#Now we can see 4052 suspicious logs were removed


# In[184]:


poi = pd.read_csv("data/POIList.csv")
df = data_sample.rename(columns ={' Latitude':'Latitude'})
poi.head()


# In[232]:


poi = poi.values.tolist()


# # 2. Labels
# 
# ### To solve this problem, I have broken it down into 3 steps:
# 
#  1. Find euclidean distance of requests from POI
#  2. Get nearest POI
#  3. Store the labels in order then append the list as a column to the DataFrame
#     

# In[244]:


#create a copy of the data samples dataframe but only with latitude and longitude columns, repeat for poi
cdf = df[["Latitude","Longitude"]]
#convert the copy into a list
cdf = cdf.values.tolist()

lst = [3,1,3,6]
al = [1,[5,34],3,4]
al[lst.index(min(lst))][0]


# In[250]:



labels = []
for i in cdf:
    closest = []
    for x in poi:
        closest.append(math.dist(i, [x[1],x[2]]))
    #append the label of the POI to the labels list
    labels.append(poi[closest.index(min(closest))][0])
    
    
    


# In[266]:


label = pd.Series(labels)
df['Label'] = label.values
df.head(2567)
#Labels seems to be working well


# # 3. Analysis
# 
#     This problem will heavily depend on external libraries and functions

# In[274]:


get_ipython().system('python3 -m ipykernel install --user')

