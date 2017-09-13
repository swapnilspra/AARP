import requests
from time import sleep
import yaml
import os

def checkForProdCluster(name):
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/list"

    res = requests.get(url=url, auth=('production@aarp.com', 'C@serta!23'))

    clusterMetaData = {}

    for cluster in res.json()['clusters']:
        if cluster['cluster_name'] == name and cluster['state'] == 'RUNNING':
            print 'found active prod cluster'
            clusterMetaData['cluster_name'] = cluster['cluster_name']
            clusterMetaData['cluster_id'] = cluster['cluster_id']
    return clusterMetaData

def createCluster(name="prod_cluster", num_workers=9):
    postdata = {
        "cluster_name": name,
        "spark_version": "2.0.x-scala2.10",
        "node_type_id": "r3.2xlarge",
        "spark_conf": {
            "spark.speculation": True
        },
        "aws_attributes": {
            "availability": "SPOT",
            "zone_id": "us-east-1c"
        },
        "libraries": [
            {
                "egg": "dbfs:/Users/production@aarp.com/Libraries/5308cbe8_469a_470e_ad6f_2b29d3f23ee6-aarpcommon_0_3_py2_7-67d2a.egg"},
        ],
        "num_workers": num_workers
    }

    clusterMetaData = checkForProdCluster(name)
    if not clusterMetaData.has_key('cluster_name'):
        createClusterURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/create"
        clusterResponse = requests.post(url=createClusterURL, auth=('production@aarp.com','C@serta!23'), json=postdata)
        print clusterResponse.content
        clusterStatusURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/get?cluster_id="+str(clusterResponse.json()['cluster_id'])
        #TODO add code to loop until the cluster is up and running
        status = ''
        while(status!='RUNNING'):
            r = requests.get(url=clusterStatusURL, auth=('production@aarp.com','C@serta!23'))
            status = r.json()['state']
            print status
            if status not in ['RUNNING', 'PENDING']:
                raise EnvironmentError('could not create the cluster')
            sleep(30)
        clusterMetaData = {'cluster_name':'prod_cluster', 'cluster_id':clusterResponse.json()['cluster_id']}
        return clusterMetaData
    else:
        return clusterMetaData

def destroyCluster(name='prod_cluster'):
    clusterMetaData = checkForProdCluster(name)
    if clusterMetaData.has_key('cluster_name'):
        deleteClusterURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/clusters/delete"
        deleteData = {"cluster_id": clusterMetaData['cluster_id']}
        res = requests.post(url=deleteClusterURL, auth=('production@aarp.com','C@serta!23'), json=deleteData)
        print res.status_code
        print res.content

def monitorJob(run_id):
    monitorURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/get?run_id="+run_id
    monitorRes = requests.get(url=monitorURL, auth=('production@aarp.com','C@serta!23'))
    print monitorRes
    print monitorRes.content
    monitorData = monitorRes.json()
    if monitorRes.status_code == 200:
        state = monitorData["state"]["life_cycle_state"]
        print 'job state'
        print state
        states =  ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED']
        while(state not in states):
            print state
            print 'Job still running, going to sleep'
            sleep(10)
            monitorURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/get?run_id=" + run_id
            monitorRes = requests.get(url=monitorURL, auth=('production@aarp.com', 'C@serta!23'))
            monitorData = monitorRes.json()
            state = monitorData["state"]["life_cycle_state"]
        if state in 'TERMINATED':
            if monitorData['state']['result_state'] == 'SUCCESS':
                #TODO add batch id code\
                return
            else:
                raise StandardError("Job Failed")
        else:
            raise StandardError("Job Failed")
    else:
        raise LookupError("Could not find a job with run_id "+str(run_id)+"monitoring the job failed")

def loadEnvVariables():
    with open('config.properties') as json_data:
    data=json.load(json_data)
	#This will be replaced by env variable
    airflow_environment=data['DEV']
    #print airflow_environment
	return airflow_environment

def loadYAMLEnvVariables():
    with open('dagconfig.yaml') as yaml_data:
    datayaml=yaml.load(yaml_data)
	airflow_zone= os.environ['airflow_zone']
    airflow_yaml_environment=datayaml[airflow_zone]
    print airflow_environment
	return airflow_yaml_environment


