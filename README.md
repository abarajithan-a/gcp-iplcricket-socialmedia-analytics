# gcp-iplcricket-socialmedia-analytics
Social media data analytics related to Indian Premier League(IPL) - Cricket League 

**Pre-requisites:**  
1. An account in Google Cloud Platform(GCP) should exist.
2. Latest version of Google Cloud SDK needs to be installed in the local machine.
3. Google Cloud SDK needs to be initalized and a default project,region,zone attributes should have been set.
4. Python version 3.8.x
5. All the required Google Component APIs should be enabled from the console under *Google Cloud Platform -> APIs & Services*

**Cloud Infrastructure Deployment:**  
Navigate to the folder ./infrastructure_deployment/  

To deploy the cloud components infrastructure for the first time, run the following from command line  
`gcloud deployment-manager deployments create deploy-ipl-analytics-infrastructure --config infrastructure_deployment.yaml`

If you to want update the already existing deployment, run the following from command line  
`gcloud deployment-manager deployments update deploy-ipl-analytics-infrastructure --config infrastructure_deployment.yaml`

If you want to delete the deployment, run the following from command line  
`gcloud deployment-manager deployments delete deploy-ipl-analytics-infrastructure`  

**Cloud Functions Deployment:**  
Navigate to the folder ./infrastructure_deployment/  

Run the following from command line  
`sh cloud_functions_deployment.sh`    
