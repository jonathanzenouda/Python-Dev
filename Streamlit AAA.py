import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import swat
from getpass import getpass
import streamlit as st

st.title("Interface de scoring Look-a-like")

with st.sidebar:
    with st.form("my_form"):
        st.header("Connection")
        user=st.text_input(label="Nom d'utilisateur", value="frajoz")
        mdp=st.text_input(label="Mot de passe", value="y42tCohuVh4D",type="password")
        cas_host ='https://viyawaves.sas.com/cas-shared-default-http/'
        cas_port= 80
        st.header("Sélection du fichier")
        path=st.file_uploader(label="Parcourir")
        submitted = st.form_submit_button("Exécuter")
        st.header("Sélection des paramètres")
        st.number_input(label="Nombre de lignes en sortie")
        if submitted:
            s = swat.CAS(cas_host, cas_port, user, mdp)
            print(s)
            df=pd.read_csv(path)
            s.upload_frame(df,casout={'name':'totrain','caslib':'CASUSER','replace':True})
            s.sessionprop.setsessopt(caslib='Public',timeout=31536000)
            indata="BAL_LYON_MATCHED"
            print(indata)
            outdata="BAL_TOULON"
            if s.table.tableExists(indata)==0:
                op = s.table.loadTable(path=str(indata).upper()+".sashdat", casout={"name":indata, "replace":True})
            if s.table.tableExists(outdata)==0:
                op2 = s.table.loadTable(path=str(outdata).upper()+".sashdat", casout={"name":outdata, "replace":True})
            s.datastep.runcode('data '+outdata+'_MATCHED;set '+outdata+';voie_nom_match=dqmatch ("voie_nom"n, "Address (Street Only)", 90, "FRFRA");run;')
            s.builtins.loadActionSet("fedSql")
            s.fedsql.execDirect(query='''create table INMODEL{options replace=true} as 
            select distinct t1.*,t2.voie_nom as voie_nom_1
            from '''+indata+''' t1 left join '''+outdata+'''_MATCHED t2 on t1.voie_nom_match=t2.voie_nom_match  ''')
            inmodel=s.datastep.runcode('data INMODEL;set INMODEL;if missing(voie_nom_1) then CIBLE=0; else CIBLE=1;run; ')
            print(inmodel)
            viewdf=s.CASTable('INMODEL').nlargest(n=5,columns=['CIBLE'])
            print('AutoML')
            s.loadactionset('dataSciencePilot')
            trans_out = dict(name = 'trans_out', replace=True)
            feat_out = dict(name = 'feat_out', replace=True)
            pipeline_out = dict(name = 'pipeline_out', replace=True)
            #save_state_out = dict(name = 'save_state_out', replace=True)
            automl_model = dict(name = 'automl_model', replace=True) 
            automl=s.dataSciencePilot.dsAutoMl(table=ech_apprent, target=target,kFolds=5,
                            modelTypes=["LOGISTIC","FOREST","GRADBOOST","DECISIONTREE", "NEURALNET"], 
                            transformationOut = trans_out,
                            featureOut = feat_out,
                            pipelineOut = pipeline_out,
                            objective='AUC',
                            saveState = automl_model)
            pipelinedf=s.CASTable('pipeline_out').nlargest(n=10,columns=['Objective'])
            featdf=s.CASTable('feat_out').nlargest(n=10,columns=['FeatureId'])
            print('AutoML done')

try:
    st.table(viewdf)
    st.table(pipelinedf)
    st.table(featdf)
    print('ON EST LES CHAMPIONS')
    
    s.terminate()
except:
    print('ERROR')
    
    

            
