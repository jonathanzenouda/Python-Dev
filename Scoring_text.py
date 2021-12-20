import swat 
import pandas as pd
import nltk

def scoring_text(host_connection,flag_concept,flag_fact,flag_categ,flag_active_learning,table,var_text,LIB_IN,LIB_OUT,table_model,table_model_cat,caslib_model,table_active_model,caslib_active_model):
            
    def connection_CAS(host,ip=''):
        if host=='VIYAWAVES':

            cas_host ='https://viyawaves.sas.com/cas-shared-default-http/'
            cas_port= 5570
            user='frajoz'
            mdp='y42tCohuVh4D'

        if host=='RACE':

            cas_host =ip
            cas_port= 5570
            user='sasdemo'
            mdp='Orion123'

        s = swat.CAS(cas_host, cas_port, user, mdp)
        return s
    s = connection_CAS(host_connection,"10.255.154.254")   
    
    s.datastep.runcode('data '+table+';set '+LIB_IN+'.'+table+';docid=uuidgen();run;')
    table_full='C_'+table
    table_full_fact='F_'+table

    #if s.table.tableExists(caslib=LIB_OUT,table='FIN'+table_full)==1:
    s.dropTable(name='FIN'+table_full,caslib=LIB_OUT)


    if flag_concept=='Y':
        if s.table.tableExists(caslib=LIB_OUT,table=table_full)==1:
            s.dropTable(name=table_full,caslib=LIB_OUT)



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
                                     table={"name":table},
                                     language='ENGLISH',
                                     text=var_text
                                    )




        s.datastep.runcode('data '+table_full+';merge '+table+' out_rule_match;by docid;drop _NAME_ _LABEL_;if index(_concept_,"nlp")=0;run;')
        s.builtins.loadActionSet("fedSql")                           #2  
        COUNT_C=s.fedSql.execDirect(                                    #3 
          query='''select distinct _concept_,count(*) from '''+table_full+''' group by _concept_ order by COUNT desc'''
        )
        display(COUNT_C)

        if flag_fact=='Y':

            s.table.dropTable(name=table_full_fact)

            s.datastep.runcode('data '+table_full_fact+'(promote=yes);set outfact;run; ')
            s.fedSql.execDirect(query='''create table FILTER_F_VACCIN{options replace=true} as select t1.*,t2._match_text_ as full_text from F_VACCIN_COVID19_SENTIMENT t1 left join F_VACCIN_COVID19_SENTIMENT t2 on t1.docid=t2.docid and t1._result_id_=t2._result_id_ where t1._fact_argument_ is not null and t2._fact_argument_ is null'''
        )
            s.builtins.loadActionSet(actionSet="transpose")   
            s.transpose.transpose (                                                       
           table={"name":"FILTER_F_VACCIN","groupBy":["docid","_result_id_","FULL_TEXT"]},                 #1
           attributes=["docid","_result_id_","FULL_TEXT"],                   #2
           transpose={"_match_text_"},                                          #3
           id={"_fact_argument_"},                                                 #4
           casOut={"name":"FILTER_F_VACCIN_TRSPOUT", "replace":True}                           #5
                )
            s.fedSql.execDirect(                                    #3 
          query='''create table '''+table_full+'''{options replace=true} as select t1.*,t2.duration,t2.FULL_TEXT as FACT_TEXT
          from '''+table_full+''' t1 left join FILTER_F_VACCIN_TRSPOUT t2 on t1.docid=t2.docid and t1._match_text_=t2.side'''
        )
            s.table.save(table=table_full_fact, name=table_full_fact+'.sashdat', replace=True)
            #s.table.loadTable(path=table_full_fact+'.sashdat',casout={"name":table_full_fact,"caslib":LIB_OUT},promote=True)

    if flag_categ=='Y':



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
                                  table={"name":table},
                                  text=var_text
                                 )

        s.datastep.runcode('data '+table_full+';merge '+table_full+' CategoryOut;by docid; run;')
        if flag_active_learning !='Y' or host_connection.upper() != 'VIYAWAVES':
            s.table.save(table=table_full, name=table_full+'.sashdat', replace=True)
            s.table.loadTable(path=table_full+'.sashdat',casout={"name":'FIN'+table_full,"caslib":LIB_OUT},promote=True)
    
    if flag_active_learning=='Y' and host_connection.upper()=="VIYAWAVES":
        
        s.builtins.loadActionSet(actionSet="textRuleDevelop")
        s.builtins.loadActionSet(actionSet="textRuleScore")


        s.textRuleScore.applyCategory(casOut={"name":"categoryOutAL",                   # 4
                                          "replace":True
                                         },
                                  docId="docId",
                                  matchOut={"name":"termOut", 
                                            "replace":True
                                           },
                                  modelOut={"name":"modelout",'replace':True},
                                  model={"name":table_active_model,"caslib":caslib_active_model},
                                  nThreads=4,
                                  table={"name":table},
                                  text=var_text
                                 )        
        s.datastep.runcode('data '+table_full+';merge '+table_full+' CategoryOutAL(rename=(_category_=Sentiment_AL));by docid; run;')
        s.table.save(table=table_full, name=table_full+'.sashdat', replace=True)
        s.table.loadTable(path=table_full+'.sashdat',casout={"name":'FIN'+table_full,"caslib":LIB_OUT},promote=True)