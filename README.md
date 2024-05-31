# airflow-dags

dagantonpod.py   =>   deze werkt

TODO:   Logging  verbeteren van de pods.
TODO:   het maken van een script dat de Dag's gaat genereren opbasis van, de metadata uit de metadata database. 
    >  Dit is dus een SYSTEEM DAG,  Deze hoeft maar één keer te worden gemaakt  en alleen bij wijzigingen moet die opnieuw worden gegenereert,  op basis van een API call  (eventueel) 


We moeten nog verder uitzoeken en documenteren hoe de SYNC werkt
We moeten nog verder uitzoeken om de sync te doen op basis van een "local  GIT repository"


om te kijken of de sync correct is gegaan moeten we nog opzoek naar de 
pod van airflow .  

is het de scheduler?  
helm-airflow-scheduler-8578b455c-mnqr4

kubectl run -it helm-airflow-scheduler-8578b455c-mnqr4  

kubectl exec -it helm-airflow-scheduler-8578b455c-mnqr4 -- /bin/sh