import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import swat
from getpass import getpass
import streamlit as st

st.title("Interface de scoring Look-a-like")

with st.sidebar:
    with st.form("my_form"):
        st.header("Connection")
        user=st.text_input(label="Nom d'utilisateur", value="sastest1")
        mdp=st.text_input(label="Mot de passe", value="Go4thsas",type="password")
        cas_host ='https://d40184.frafed-wv1-azure-nginx-e111fd2c.unx.sas.com/cas-shared-default-http/'
        cas_port= 80
        st.header("Sélection du fichier")
        path=st.file_uploader(label="Parcourir")
        submitted = st.form_submit_button("Exécuter")
        st.header("Sélection des paramètres")
        st.number_input(label="Nombre de lignes en sortie")
        if submitted:
            s = swat.CAS(cas_host, cas_port, user, mdp)
            df=pd.read_csv(path)
            s.upload_frame(df,casout={'name':'totrain','caslib':'CASUSER','replace':True})
            indata="BAL_LYON_MATCHED"
            outdata="BAL_TOULON"
            op = s.table.loadTable(path=str(indata).upper()+".sashdat", casout={"name":indata, "replace":True})
            op2 = s.table.loadTable(path=str(outdata).upper()+".sashdat", casout={"name":outdata, "replace":True})
            s.datastep.runcode('data '+outdata+'_MATCHED;set '+outdata+';voie_nom_match=dqmatch ("voie_nom"n, "Address (Street Only)", 90, "FRFRA");run;')
            s.builtins.loadActionSet("fedSql")
            s.fedsql.execDirect(query='''create table INMODEL{options replace=true} as 
            select distinct t1.*,t2.voie_nom as voie_nom_1
            from '''+indata+''' t1 left join '''+outdata+'''_MATCHED t2 on t1.voie_nom_match=t2.voie_nom_match  ''')
            inmodel=s.datastep.runcode('data INMODEL;set INMODEL;if missing(voie_nom_1) then CIBLE=0; else CIBLE=1;run; ')
            view=s.datastep.runcode('data VIEWMODEL;set INMODEL;if CIBLE=1;run; ')
            viewdf=s.CASTable('INMODEL').to_frame()
            st.table(viewdf.head())
            

            
