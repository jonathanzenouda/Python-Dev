import swat 
def connection_CAS(host,ip='',user='frajoz',mdp='y42tCohuVh4D'):
    if host.upper()=='VIYAWAVES':
    
        cas_host ='https://viyawaves.sas.com/cas-shared-default-http/'
        cas_port= 5570
        user='frajoz'
        mdp='y42tCohuVh4D'

    if host.upper()=='RACE':
    
        cas_host =ip
        cas_port= 5570
        user='viyademo'
        mdp='Orion123'
    
    s = swat.CAS(cas_host, cas_port, user, mdp)
    return s



