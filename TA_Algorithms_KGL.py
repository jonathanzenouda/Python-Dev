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
import re
import zipfile
from math import sqrt
from scipy.stats import kendalltau
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.metrics import confusion_matrix
from sklearn.metrics import fbeta_score
from scipy.stats import ks_2samp
import time
from PIL import Image
sns.set()
st.sidebar.title('Framework de Text Analytics')
image=Image.open('C:\\Users\\frajoz\\Downloads\\TA.png')
st.sidebar.image(image,use_column_width=True)


pd.set_option('display.max_colwidth', -1)

cas_host = 'https://viyawaves.sas.com/cas-shared-default-http/'
cas_port= 5570

@st.cache(allow_output_mutation=True)
def connect(cas_host):
    s = CAS(cas_host, cas_port, 'frajoz', 'y42tCohuVh4D')
    return s

s = connect(cas_host)
# In[3]:

def split_it(term):
    return re.findall('[A-Za-z]+', term)
# In[54]:
table_name=st.sidebar.text_input('Provide the data name you want to provide','BIORXIV_CLEAN_JOZ')
LIB_OUT=st.sidebar.text_input('Library','Public')

var_text = st.sidebar.text_input('Text Variable', 'text')

#Language 
language=st.sidebar.selectbox('Which Language?', ('ENGLISH','FRENCH','ITALIAN','SPANISH','DUTCH','GERMAN','ARABIC'))

if language=='ARABIC':
    stopl="ar_stoplist"
elif language=='FRENCH':
    stopl="fr_stoplist"
elif language=='ENGLISH':
    stopl="en_stoplist"
elif language=="SPANISH":
    stopl="es_stoplist"
elif language=="ITALIAN":
    stopl="it_stoplist"
elif language=="GERMAN":
    stopl="de_stoplist"
elif language=="DUTCH":
    stopl="nl_stoplist"

s.table.loadTable(path=stopl+".sashdat",caslib="ReferenceData", casout={"name":stopl, "replace":True})

s.sessionprop.setsessopt(caslib='Public')
s.datastep.runcode('data Public.'+table_name+';set '+LIB_OUT+'.'+table_name+";if '"+var_text+"'n ne ' ';docid=uuidgen();run;")


#Nom de l'application souhaitée
app=st.sidebar.selectbox('Quelle opération souhaitez-vous réaliser?',('SUMMARIZATION', 'TOPICS', 'SENTIMENT','CONCEPTS','CATEGORIZATION'))

if app.upper()=="SENTIMENT":
    s.builtins.loadActionSet(actionSet="sentimentAnalysis") 
    s.sentimentAnalysis.applySent(casOut={"name":"table_out", "replace":True},language=language,docId="docid",table={"name":table_name},text=var_text)
    table_out=s.CASTable('table_out').to_frame()
    ch = sns.countplot(table_out['_sentiment_'])
    st.bar_chart(table_out['_sentiment_'])

    
if app.upper()=="SUMMARIZATION":
    nb=st.sidebar.slider('How many sentences?', 0, 10, 1)
    type_sum=st.sidebar.selectbox('Global summarization (corpus), or per document?',('CORPUS','DOCUMENT'))       
    st.subheader('Résultats')
    if table_name != '' and var_text != '' and nb!= 0:
        if type_sum.upper()=="CORPUS":
            s.loadactionset(actionset='textSummarization') 
            s.textSummarization.textSummarize(language=language,corpusSummaries={'name':'table_out','replace':'True'},documentSummaries={'name':'documentSummaries','replace':'True'},table={'name':table_name},numberOfSentences=nb,id='docid',text = var_text)
            table_out=s.CASTable('table_out').to_frame()
            st.write(table_out)    

            
        elif type_sum.upper()=="DOCUMENT":
            s.loadactionset(actionset='textSummarization') 
            s.textSummarization.textSummarize(language=language,corpusSummaries={'name':'corpussum','replace':'True'},documentSummaries={'name':'table_out','replace':'True'},table={'name':table_name},numberOfSentences=nb,id='docid',text = var_text)
            s.datastep.runcode('data Public.table_out(single=yes);set Public.table_out;i+1;if i<10;run;')
            table_out=s.CASTable('table_out').to_frame()
            st.write(table_out)
        else:
            print('Requête non comprise')

        
elif app.upper()=="TOPICS":
    s.table.dropTable(name='Topics',quiet=True)
    topic_ct=st.sidebar.slider('How many topics?', 0, 100, 10)+1
    if table_name != '' and var_text != '' and topic_ct!= 0:

        method = st.sidebar.selectbox('Which method?',('SVD', 'LDA'))
        if method.upper()=='SVD':

            s.loadactionset(actionset='textmining')                       # 3
            s.textMining.tmMine(docId="docid",                                           # 2
                                            docPro={"name":"docpro", "replace":True},
                                            wordPro={"name":"wordpro", "replace":True},
                                            documents={"name":table_name},
                                            nounGroups=False,
                                            numLabels=5,
                                            language=language,
                                            k=topic_ct,
                                            offset={"name":"offset", "replace":True},
                                            parent={"name":"parent", "replace":True},
                                            parseConfig={"name":"config", "replace":True},
                                            reduce=2,
                                            stopList={"name":stopl},
                                            tagging=True,
                                            terms={"name":"terms", "replace":True},
                                            text=var_text,
                                            topicDecision=True,
                                            topics={"name":"topics", "replace":True},
                                            u={"name":"svdu", "replace":True}
                                           )
            s.textMining.tmScore(docId="docid",                                          # 1
                     docPro={"name":"topics_out", "replace":True},
                     documents={"name":table_name},
                     parseConfig={"name":"config"},
                     terms={"name":"terms"},
                     text=var_text,
                     topics={"name":"topics"},
                     u={"name":"svdu"}                   
                    )

            Texttopics=[ '_TextTopic_'+str(i) for i in range(1,topic_ct)]
            
            s.datastep.runcode('data Public.topics_out;set Public.topics_out;ID="TopicAppartenance";run;')

            s.loadactionset(actionset='transpose')                       # 3    
            s.transpose.transpose ( 
                           table={"name":"topics_out","groupBy":[{"name":"docid"}]},        #1
                           attributes=[{"name":"docid", "label":"Identifiant document"}],             #2
                           transpose=Texttopics,
                            id={"ID"},                                           #4
                        #   prefix="Apt_",                                                #5
                           casOut={"name":"table_out", "replace":True}                  #6
                        )
            s.datastep.runcode('data Public.table_out;set Public.table_out;where TopicAppartenance=1;rename _LABEL_=Topic;drop _NAME_ TopicAppartenance;run;')
            table_out=s.CASTable('table_out').to_frame()
            ch = sns.countplot(table_out['Topic'])
            st.subheader('Résultats')
            st.bar_chart(table_out['Topic'])  


        elif method.upper()=='LDA':
            s.loadactionset(actionset='ldaTopic')    
            s.ldaTopic.ldaTrain(casOut={"name":"model_out","replace":True},docDistOut={"name":"DocDist","replace":True},docId="docid",table={"name":table_name},k=topic_ct,text=[{"name":var_text}],tm={"entities":"STD","language":language,"nounGroups":True,"Stemming":True})
            s.datastep.runcode('data table_out;set model_out;rename _Term_=TERM;run;')
            s.datastep.runcode('data table_out;merge table_out(in=in1) '+stopl+'(in=in2);by TERM;if not in2 and length(TERM) ge 2;run;')
            table_out=s.CASTable('table_out').to_frame()
            g = table_out.groupby(["_TopicID_"]).apply(lambda x: x.sort_values(["_Probability_"], ascending = False)).reset_index(drop=True)
            table_drop=g.groupby('_TopicID_').head(30*topic_ct)
            table_drop['TERM2'] = table_drop['TERM'].apply(split_it)
            s.upload_frame(table_drop, importoptions=None, casout={'name':"table_drop",'caslib':"Public",'replace':'yes'})
            s.datastep.runcode('data table_d;set table_drop;if TERM=compress(TERM2,"[]'+"'"+',") and length(TERM)>2;drop TERM2;run;')
            table_drop=s.CASTable('table_d').to_frame()
            table_dedup=table_drop.drop_duplicates('TERM')
            table_top=table_dedup.groupby('_TopicID_').head(5)
            table_top['IDTERM'] = table_top.groupby(['_TopicID_']).cumcount()+1
            s.upload_frame(table_top, importoptions=None, casout={'name':"table_top",'caslib':"Public",'replace':'yes'})

            s.loadactionset(actionset='transpose')                       # 3    
            s.transpose.transpose ( 
                   table={"name":"table_top","groupBy":[{"name":"_TopicID_"}]},        #1
                   attributes=[{"name":"_TopicID_", "label":"Thèmes"}],             #2
                   transpose="TERM",
                    id={"IDTERM"},
                    prefix="TERM_",
                #   prefix="Apt_",                                                #5
                   casOut={"name":"TRANSP_TOP", "replace":True}                  #6
                )
            s.datastep.runcode('data TOPICS;set TRANSP_TOP;topic=TERM_1||","||TERM_2||","||TERM_3||","||TERM_4||","||TERM_5;drop TERM_: _NAME_;run;')
            topics=s.CASTable('TOPICS').to_frame()
            s.ldaTopic.ldaScore(docDistOut={"name":"DocDist","replace":True},docId="docid",modelTable={"name":"model_out"},table={"name":table_name},text=[{"name":var_text}],tm={"entities":"STD","language":language,"nounGroups":True,"Stemming":True})
            s.datastep.runcode('data DocDist;set DocDist ;if _Proportion_>0.1;run;')
            Docdist_out=s.CASTable("DocDist").to_frame()
            joindiab = Docdist_out.merge(topics, left_on='_TopicID_', right_on='_TopicID_')
            ch = sns.countplot(joindiab['topic'])



            st.subheader('Résultats')
            st.bar_chart(joindiab['topic'])      
    else:
                    print('PAS DE METHODE SELECTIONNEE, LE SYSTEME NE PEUT RIEN FAIRE DE CA ! ')

elif app.upper()=='CONCEPTS':
    table_model=st.sidebar.text_input('Name of the model table','8ae08ee8710e4101017116e0df630049_CONCEPT_BINARY')
    caslib_model=st.sidebar.text_input('Name of the model caslib','Analytics_Project_428c7a2b-7e3e-4e82-997b-dba4477f3c21')
    LIB_OUT=st.sidebar.text_input('Name of the caslib of destination',"CASUSER")
    table_full=table_name+"_C"
    table_full_fact=table_name+"_F"
    s.dropTable(name=table_full,caslib=LIB_OUT)
    s.dropTable(name=table_full_fact,caslib=LIB_OUT)
    s.builtins.loadActionSet(actionSet="textRuleDevelop")
    s.builtins.loadActionSet(actionSet="textRuleScore")
    s.textRuleScore.applyConcept(casOut={"name":"outconcept", "replace":True},     # 4
                             docId="docid",
                             factOut={"name":"outfact", "replace":True},
                             model={"name":table_model,"caslib":caslib_model},
                             ruleMatchOut={"name":"out_rule_match", 
                                           "replace":True
                                          },
                             matchType='ALL',
                             table={"name":table_name},
                             language='ENGLISH',
                             text=var_text
                            )
    datastep='data '+table_full+';merge '+table_name+' out_rule_match;by docid;drop _NAME_ _LABEL_;if index(_concept_,"nlp")=0;run;'
    s.datastep.runcode(datastep)
    
    s.builtins.loadActionSet(actionSet="transpose")                              

    s.transpose.transpose (                                                       
   table={"name":"outfact","groupBy":[{"name":"docid"},{'name':'_result_id_'}]},                 
   attributes=[{"name":"docid", "label":"Identifiant document"},{'name':"_fact_"}],                  
   transpose={"_match_text_"},                                          
   id={"_fact_argument_"},
    let=True,
   casOut={"name":"out_trsp_fact", "replace":True}                           
        )
    s.datastep.runcode('data '+table_full_fact+';merge '+table_name+' out_trsp_fact;by docid;drop _NAME_ _LABEL_;run;')
    s.table.save(table=table_full, name=table_full+'.sashdat', replace=True)
    s.table.loadTable(path=table_full+'.sashdat',casout={"name":table_full,"caslib":LIB_OUT},promote=True)
    s.table.save(table=table_full_fact, name=table_full_fact+'.sashdat', replace=True)
    s.table.loadTable(path=table_full_fact+'.sashdat',casout={"name":table_full_fact,"caslib":LIB_OUT},promote=True)
    st.subheader(table_full +" and "+table_full_fact+" have been created with success")
    dataframe_c=s.CASTable(table_full).to_frame()
    st.dataframe(dataframe_c[['_concept_', '_match_text_']] )
    #st.dataframe(dataframe_c)
    
elif app.upper()=='CATEGORIZATION':
    table_model_cat=st.sidebar.text_input('Name of the model table','8ae08ee8710e3f30017116e0e39b0002_CATEGORY_BINARY')
    caslib_model=st.sidebar.text_input('Name of the model caslib','Analytics_Project_428c7a2b-7e3e-4e82-997b-dba4477f3c21')
    LIB_OUT=st.sidebar.text_input('Name of the caslib of destination',"CASUSER")
    table_full_cat=table_name+"CT"
    var_text="text"
    s.builtins.loadActionSet(actionSet="textRuleDevelop")
    s.builtins.loadActionSet(actionSet="textRuleScore")

    s.textRuleScore.applyCategory(casOut={"name":"categoryOut",                   # 4
                                      "replace":True
                                     },
                              docId="docId",
                              matchOut={"name":"termOut", 
                                        "replace":True
                                       },
                              modelOut={"name":"modelout",'replace':True},
                              model={"name":table_model_cat,"caslib":caslib_model},
                              nThreads=4,
                              table={"name":table_name},
                              text=var_text
                             )
    s.builtins.loadActionSet(actionSet="transpose")                              

    s.transpose.transpose (                                                       
   table={"name":"CategoryOut","groupBy":[{"name":"docid"}]},                 
   attributes=[{"name":"docid", "label":"Identifiant document"}],                  
   transpose={"_score_"},                                          
   id={"_category_"},
    prefix="CT_",
    let=True,
   casOut={"name":"out_trsp_cat", "replace":True}                           
        )

    s.datastep.runcode('data '+table_full_cat+';merge '+table_name+' out_trsp_cat;by docid; array var{*} CT_:;do i=1 to dim(var);if missing(var(i)) then var(i)=0;else if var(i) ge 1 then var(i)=1;end;drop _NAME_ _LABEL_ i;run;')

    s.table.save(table=table_full_cat, name=table_full_cat+'.sashdat', replace=True)
    s.table.loadTable(path=table_full_cat+'.sashdat',casout={"name":table_full_cat,"caslib":LIB_OUT},promote=True)
    st.subheader(table_full_cat+" has been created with success")
    dataframe_c=s.CASTable("CategoryOut").to_frame()
    st.dataframe(dataframe_c[['_category_', '_score_']] )
    



