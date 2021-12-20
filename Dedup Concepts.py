#CONCEPTS DEDUPLICATION 
conncas.datastep.runcode('data CONCEPTS_CALCUL;set '+ outlib + "." + nm_out_concept_ds + ' ;by '+ docidvar +' _concept_ _start_;prevstart=lag(_start_);prevend=lag(_end_);L_MATCH=length(_match_text_);retain result_id 1;if (_start_>prevend+3 or _start_<prevstart-3) then do;result_id+1;end;run;')

s.fedsql.execDirect(query='''create table CONCEPTS_MAX{options replace=true} as 
select distinct '''+docidvar+''',_path_,_concept_,result_id,max(L_MATCH) as L_MAX
from CONCEPTS_CALCUL group by '''+docidvar+''',_path_,_concept_,result_id;''')

conncas..fedsql.execDirect(query='''create table CONCEPTS_DEDUP{options replace=true} as 
select t1.*,t2._start_,t2._end_,t2._match_text_,t2._canonical_form_
from CONCEPTS_MAX t1 left join CONCEPTS_CALCUL t2 on t1.'''+docidvar+'''=t2.'''+docidvar+''' and t1.result_id=t2.result_id 
and t1.L_MAX=t2.L_MATCH; ''')



#FACTS DEDUPLICATION


conncas.fedsql.execDirect(query='''create table OUTFACT_COUNT{options replace=true} as 
select '''+docidvar+''',_result_id_,_fact_,count(distinct _match_text_)-1 as cpt_arg
from '''+outlib+'''.'''+nm_out_fact_ds+''' group by '''+docidvar+''',_result_id_,_fact_ ''')

conncas.fedsql.execDirect(query='''create table FACTS_ARGUMENTS{options replace=true} as 
select t1.* from '''+outlib+'''.'''+nm_out_fact_ds+''' t1 inner join CONCEPTS_DEDUP t2 on t1.'''+docidvar+'''=t2.'''+docidvar+''' and t1._match_text_=t2._match_text_; ''')

conncas.fedsql.execDirect(query='''create table fact_count{options replace=true} as 
select distinct '''+docidvar+''',_fact_ ,_result_id_,count(distinct _match_text_) as CT_R
from FACTS_ARGUMENTS group by '''+docidvar+''',_fact_,_result_id_''')

conncas.fedsql.execDirect(query='''create table factjoin{options replace=true} as 
select distinct t1.* from fact_count t1 right join OUTFACT_COUNT t2 on t1.'''+docidvar+'''=t2.'''+docidvar+''' and 
t1._result_id_=t2._result_id_ and t1.CT_R=t2.cpt_arg''')

conncas.fedsql.execDirect(query='''create table factjoin_full{options replace=true} as 
select distinct t1.'''+docidvar+''',t1._fact_,t1._result_id_,t2._fact_argument_,t2._start_,t2._end_,t2._match_text_,t2._path_ 
from FACTJOIN t1 left join '''+outlib+'''.'''+nm_out_fact_ds+''' t2 on t1.'''+docidvar+'''=t2.'''+docidvar+''' and t1._result_id_=t2._result_id_ where t1._fact_ is not null''')

conncas.datastep.runcode('data CASUSER.FACTS_CALCUL;set CASUSER.factjoin_full(where=(_fact_argument_="")) ;by '+docidvar+' _fact_ _start_;prevstart=lag(_start_);prevend=lag(_end_);L_MATCH=length(_match_text_);retain result_id 1;if (_start_>prevend+3 or _start_<prevstart-3) then do;result_id+1;end;run;')


conncas.fedsql.execDirect(query='''create table fact_nearest1{options replace=true} as 
select distinct '''+docidvar+''',_fact_,result_id,min(L_match) as min_L from FACTS_CALCUL group by '''+docidvar+''',_fact_,result_id''')


conncas.fedsql.execDirect(query='''create table fact_nearest{options replace=true} as 
select distinct t1.* from FACTS_CALCUL t1 right join FACT_NEAREST1 t2 on t1.'''+docidvar+'''=t2.'''+docidvar+''' and t1._fact_=t2._fact_ and 
t1.result_id=t2.result_id and t1.L_match=t2.min_L''')


conncas.fedsql.execDirect(query='''create table '''+outlib+'''.'''+nm_out_fact_ds+'''{options replace=true} as 
select distinct t1.'''+docidvar+''',t1._fact_,t1._result_id_,t1._fact_argument_,t1._start_,t1._end_,t1._match_text_,t1._path_ from '''+outlib+'''.'''+nm_out_fact_ds+''' t1 right join fact_nearest t2 
on t1.'''+docidvar+'''=t2.'''+docidvar+''' and t1._fact_=t2._fact_ and t1._result_id_=t2._result_id_''')