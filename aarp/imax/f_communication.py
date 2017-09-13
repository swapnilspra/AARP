from aarp.common.utils import createCluster, monitorJob, destroyCluster,loadEnvVariables
import requests

def startFcomLakeJob():
	airflow_environment=loadEnvVariables()
    clusterMetaData = createCluster(name=airflow_environment['cluster_name'], num_workers=6)
    print clusterMetaData
	
	 postdata = {
      "run_name": "fcom_lake_job",
      "existing_cluster_id":clusterMetaData['clusterid'],
      "timeout_seconds": 3600,
      "notebook_task": {
        "notebook_path": airflow_environment['imax']['fcom']['notebook_path'],
        "base_parameters":{"pathDataLakeImax":airflow_environment['imax']['fcom']['pathDataLakeImax'],"pathLandingImax":airflow_environment['imax']['fcom']['pathLandingImax']}
      }
    }

    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/submit"
    res = requests.post(url, auth=('production@aarp.com','C@serta!23'), json=postdata)
    
    json_data = json.loads(res.text)
    runid = json_data["run_id"]

    print(runid)
	
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/get?run_id="+runid
    res = requests.get(url, auth=('production@aarp.com','C@serta!23'))

	if res.status_code == 200:
        print 'job launched successfully, will start monitoring'
        print res.json()
        monitorJob(str(res.json()['run_id']))
    else:
        raise ReferenceError(
            'The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')
			
	