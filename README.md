# airflow-dags


DAP

TODO:   Logging  verbeteren van de pods.
TODO:  aanmaken van DAG voor het initialiseren van Applicatie,  run mdpcreate.py  uit de MDP  folder.


We moeten nog verder uitzoeken en documenteren hoe de SYNC werkt
We moeten nog verder uitzoeken om de sync te doen op basis van een "local  GIT repository"
We kunnen ook in plaats van github  repo voor  de dags  gebruik maken van een s3 bucker sync.  


om te kijken of de sync correct is gegaan moeten we nog opzoek naar de 
pod van airflow .  

is het de scheduler?  
helm-airflow-scheduler-8578b455c-mnqr4

kubectl run -it helm-airflow-scheduler-8578b455c-mnqr4  

kubectl exec -it helm-airflow-scheduler-8578b455c-mnqr4 -- /bin/sh