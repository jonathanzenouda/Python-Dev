#!/usr/bin/env python
# coding: utf-8

# In[55]:



# Load libraries
import pandas as pd
import sklearn_pandas
import numpy as np
import streamlit as st
from sklearn.tree import DecisionTreeClassifier # Import Decision Tree Classifier
from sklearn.model_selection import train_test_split # Import train_test_split function
from sklearn import metrics #Import scikit-learn metrics module for accuracy calculation
import os # used to create necessary folders
import json
import sys
from swat import *
import seaborn as sns
import pickle
import zipfile
from math import sqrt
from scipy.stats import kendalltau
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.metrics import confusion_matrix
from sklearn.metrics import fbeta_score
from scipy.stats import ks_2samp
from PIL import Image
from zipfile import ZipFile

sns.set()
st.sidebar.title('Framework of Documents Import')
image=Image.open('C:\\Users\\frajoz\\OneDrive - SAS\\Pictures\\Image1.png')
st.sidebar.image(image,use_column_width=True)


pd.set_option('display.max_colwidth', -1)
cas_host = 'raceimage.sas.com'
cas_port= 5570

@st.cache(allow_output_mutation=True)
def connect(cas_host):
    s = CAS(cas_host, cas_port, 'sasdemo', 'Orion123')
    return s

s = connect(cas_host)
# In[3]:
s.sessionprop.setsessopt(caslib='Public')

path="C:\\Users\\frajoz\\OneDrive - SAS\\"
    
datatype=st.sidebar.selectbox('What Type of Data?',('CAS','EXCEL','SAS','PATH','CSV'))
   
    
if datatype=='CAS':
    table_name=st.sidebar.text_input('Provide the table name', 'CONSUMER_COMPLAINTS_FILTERED')
    LIB_OUT=st.sidebar.selectbox('In which Library?',('PUBLIC','CASUSER','OTHER'))
    if LIB_OUT=='OTHER':
        LIB_OUT=st.sidebar.text_input('Provide the name of the library:',"frcltmp")
    s.dropTable(name=table_name,caslib=LIB_OUT)
    s.table.loadTable(path=table_name+'.sashdat',caslib=LIB_OUT,casout={"name":table_name,"caslib":LIB_OUT},promote=True)
    #st.write(loadvar)
    dataframe=s.CASTable(name=table_name,caslib=LIB_OUT,replace=True).to_frame()    
    st.title('The file '+table_name+' has been imported with success in the library '+LIB_OUT)
    #s.table.promote(name=table_name)
    #dataframe
    st.write(dataframe)

if datatype=="CSV":
#   s.datastep.runcode('data Public.'+table_name+';x=1;run;')
#   s.table.dropTable(name=table_name)
    filename=path+st.sidebar.text_input('Fulfill the path of the CSV:','Domain\\HealthCare\\Démos\\KGL\\overview-of-recordings')+'.csv'
    table_name=st.sidebar.text_input('Provide the table name', 'HC_KGL')
    LIB_OUT=st.sidebar.selectbox('In which Library?',('PUBLIC','CASUSER','OTHER'))
    if LIB_OUT=='OTHER':
        LIB_OUT=st.sidebar.text_input('Provide the name of the library:',"frcltmp")    
    s.dropTable(name=table_name,caslib=LIB_OUT)
    table=pd.read_csv(filename)
    s.upload_frame(table, importoptions=None, casout={'name':table_name,'caslib':LIB_OUT,'replace':'yes'})
    s.table.save(table=table_name, name=table_name+'.sashdat', replace=True)
    s.table.loadTable(path=table_name+'.sashdat',casout={"name":table_name,"caslib":LIB_OUT,'replace':True},promote=True)
    dataframe=s.CASTable(name=table_name,caslib=LIB_OUT,replace=True).to_frame()    
    st.title('The file '+table_name+' has been imported with success in the library '+LIB_OUT)
    #s.table.promote(name=table_name)
    #dataframe
    st.write(dataframe)    
   # s.table.promote(name=table_name)

if datatype.upper()=="SAS":
    filename=path+st.sidebar.text_input('Fulfill the path:','Domain\\Banking\\Demo\\Octroi\\DATA\\HMEQ')+'.sas7bdat'
    table_name=st.sidebar.text_input('Provide the table name', 'HMEQ')
    LIB_OUT=st.sidebar.selectbox('In which Library?',('PUBLIC','CASUSER','OTHER'))   
    if LIB_OUT=='OTHER':
        LIB_OUT=st.sidebar.text_input('Provide the name of the library:',"frcltmp")  
    s.dropTable(name=table_name,caslib=LIB_OUT)
    s.read_sas(filename,casout={"name":table_name,"caslib":LIB_OUT,'promote':True})
    #dataframe=s.CASTable(name=table_name,caslib=LIB_OUT,replace=True).to_frame()    
    st.title('The file '+table_name+' has been imported with success in the library '+LIB_OUT)
    #s.table.promote(name=table_name)
    #dataframe
    #st.write(dataframe)    
   # s.table.promote(name=table_name)

if datatype=="EXCEL":
#   s.datastep.runcode('data Public.'+table_name+';x=1;run;')
#   s.table.dropTable(name=table_name)
    filename=path+st.sidebar.text_input('Fulfill the path of the CSV:','Domain\\HealthCare\\Démos\\DEMO IJL\\Codage_IJL.xls')
    table_name=st.sidebar.text_input('Provide the table name', 'LISTING_REGIONS')
    LIB_OUT=st.sidebar.selectbox('In which Library?',('PUBLIC','CASUSER','OTHER'))
    if LIB_OUT=='OTHER':
        LIB_OUT=st.sidebar.text_input('Provide the name of the library:',"frcltmp")    
    s.dropTable(name=table_name,caslib=LIB_OUT)
    table=pd.read_excel(filename)
    s.upload_frame(table, importoptions=None, casout={'name':table_name,'caslib':LIB_OUT,'replace':'yes'})
    s.table.save(table=table_name, name=table_name+'.sashdat', replace=True)
    s.table.loadTable(path=table_name+'.sashdat',casout={"name":table_name,"caslib":LIB_OUT,'replace':True},promote=True)
    dataframe=s.CASTable(name=table_name,caslib=LIB_OUT,replace=True).to_frame()    
    st.title('The file '+table_name+' has been imported with success in the library '+LIB_OUT)
    #s.table.promote(name=table_name)
    #dataframe
    st.write(dataframe)    
   # s.table.promote(name=table_name)
#    table

    
if datatype.upper()=="PATH":
    chemin=path+st.sidebar.text_input('indiquer le chemin contenant les fichiers','Domain\\HealthCare\\Démos\\DEMO IJL\\ANONYMISE DATA\\FILE\\')
    table_name=st.sidebar.text_input('Provide the table name', 'IJL_FILES')
    LIB_OUT=st.sidebar.selectbox('In which Library?',('PUBLIC','CASUSER','OTHER'))
    if LIB_OUT=='OTHER':
        LIB_OUT=st.sidebar.text_input('Provide the name of the library:',"frcltmp")       
    s.dropTable(name=table_name,caslib=LIB_OUT)
    s.table.addCaslib(dataSource={"srcType":"PATH"},name="lib",path=chemin,subDirectories=True)
    s.table.loadTable(casOut={"name":table_name,"caslib":LIB_OUT,"replace":True}, caslib="lib",
                  importOptions={"fileType":"DOCUMENT",
                                 "fileExtList":["DOCX", "PDF","TXT"],
                                 "recurse":True,
                                 "tikaConv":True,
                                 "tikaPath":"/opt/sas/viya/home/SASFoundation/lib/docconvjars"
                                },
                  path="")
    dataframe=s.CASTable(name=table_name,caslib=LIB_OUT,replace=True).to_frame()    
    st.title('The file '+table_name+' has been imported with success in the library '+LIB_OUT)
    #s.table.promote(name=table_name)
    #dataframe
    st.write(dataframe)    
   # s.table.promote(name=table_name)
#    table
   

#try:
#   table_out=s.CASTable('table_out').to_frame()
#   st.table(table_out)
#except SWATError:
#   st.error('An error occured, check your entries')





