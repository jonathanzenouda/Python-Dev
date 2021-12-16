import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import swat
from getpass import getpass
import streamlit as st
import plotly.express as px
st.title("Interface de scoring Look-a-like")

with st.sidebar:
    with st.form("my_form"):
        st.header("Connection")
        user=st.text_input(label="Nom d'utilisateur", value="sasadm")
        mdp=st.text_input(label="Mot de passe", value="Go4thsas",type="password")
        cas_host ='https://aaa2021.frafed-yl1-azure-nginx-ceafe719.unx.sas.com/cas-shared-default-http/'
        cas_port= 80
        st.header("Sélection du fichier")
        path=st.file_uploader(label="Parcourir")
        st.header("Réconciliation des bases de données")
        var_match=st.text_input(label="Variable à matcher",value="voie_nom")
        type_match=st.selectbox(label="Type de match",options=("Address (Street Only)","Address","Name","Account Number","Date (DMY)","Date (YMD)","Date (MDY)","Organization","Phone","Text"))
        st.header("AutoML")
        nb=st.number_input(label="Nombre de lignes en sortie",value=100)
        submitted = st.form_submit_button("Exécuter")
        if submitted:
            s = swat.CAS(cas_host, cas_port, user, mdp)
            s.sessionprop.setsessopt(caslib='Public',timeout=31536000)
            print(s)
            totrain=s.read_csv(path,sep=";",casout='TOTRAIN')
            indata="BAL_LYON_MATCHED"
            print(indata)
            outdata="BAL_TOULON"
            if s.table.tableExists(indata)==0:
                op = s.table.loadTable(path=str(indata).upper()+".sashdat", casout={"name":indata, "replace":True})
            if s.table.tableExists(outdata)==0:
                op2 = s.table.loadTable(path=str(outdata).upper()+".sashdat", casout={"name":outdata, "replace":True})
            s.datastep.runcode('data '+outdata+'_MATCHED;set '+outdata+';voie_nom_match=dqmatch ("'+var_match+'"n, "'+type_match+'", 90, "FRFRA");run;')
            print("matchcode end")
            s.builtins.loadActionSet("fedSql")
            s.fedsql.execDirect(query='''create table INMODEL{options replace=true} as 
            select distinct t1.*,t2.voie_nom as voie_nom_1
            from '''+indata+''' t1 left join '''+outdata+'''_MATCHED t2 on t1.voie_nom_match=t2.voie_nom_match  ''')
            inmodel=s.datastep.runcode('data INMODEL;set INMODEL;if missing(voie_nom_1) then CIBLE=0; else CIBLE=1;run; ')
            print("fedsql end")
            viewdf=s.CASTable('INMODEL').nlargest(n=5,columns=['CIBLE'])
            print('AutoML begin')
            s.loadactionset('dataSciencePilot')
            trans_out = dict(name = 'trans_out', replace=True)
            feat_out = dict(name = 'feat_out', replace=True)
            pipeline_out = dict(name = 'pipeline_out', replace=True)
            #save_state_out = dict(name = 'save_state_out', replace=True)
            automl_model = dict(name = 'automl_model', replace=True) 
            s.loadactionset(actionset="sampling")
            s.stratified(display={"names":"STRAFreq"},output={"casOut":{"name":"samptrain","replace":True}, "copyVars":"ALL"},samppct=3,partind=False, seed=10,table={"name":"totrain", "groupBy":{"CIBLE"}},outputTables={"names":{"STRAFreq"},"replace":True})
            automl=s.dataSciencePilot.dsAutoMl(table="samptrain", target="CIBLE",kFolds=3,
                            modelTypes=["DECISIONTREE","GRADBOOST"], 
                            transformationOut = trans_out,
                            featureOut = feat_out,
                            pipelineOut = pipeline_out,
                            objective='AUC',
                            saveState = automl_model)
            s.datastep.runcode('data pipeline_graph;set pipeline_out;PIPELINE=PipelineID||"_"||MLType||"_nbfeat:"||put(NFeatures,$4.);keep PIPELINE Objective;rename Objective=AUC;run;')
            pipelinedf=s.CASTable('pipeline_graph').nlargest(n=10,columns=['AUC'])
            featdf=s.CASTable('feat_out').nlargest(n=10,columns=['FeatureId'])
            print('AutoML done')
            

            # Load modelPublishing action set
            s.loadactionset('modelPublishing')

            # Score Model in CAS
            s.modelPublishing.runModelLocal(
                modelName="GradientBoosting_0add20ae20a44b2984c2dc03e969dcf7",                   # Model Name
                modelTable={"caslib":"Casuser","name":"sas_model_table"},    # CAS destination
                intable={"name":"CH_APPRENT_READY"},           # Input Table
                outTable={"name":"SCORE"})        # Output Table
            larg=s.CASTable('SCORE').nlargest(n=100,columns=['EM_EVENTPROBABILITY'])

try:
    if s.table.tableExists(indata):
        st.header("Réconciliation effectuée")
    st.table(viewdf)
    fig = px.bar(pipelinedf, x='AUC', y='PIPELINE',orientation='h')
    
    st.header("Meilleurs pipelines après AutoML")
    st.plotly_chart(fig, use_container_width=True)

    st.header("Meilleures transformations après AutoML")
    st.table(featdf)
    
    st.header("Plus proches clients")
    st.table(larg)
    print('ON EST LES CHAMPIONS')
    
    s.terminate()
except:
    print("ERROR")
    
    

            
