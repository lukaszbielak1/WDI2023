from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from azure.storage.filedatalake import DataLakeServiceClient
import json

class job_creator:
    def __init__(self,db_workspace,db_token):
        api_client = ApiClient(host = db_workspace,token = db_token,jobs_api_version="2.1",api_version="2.1")
        self.job_client = JobsApi(api_client)
    
    def create_job(self,job_name, cluster_id, notebook_path, notebook_parameters,schedule):
        json_command = {
                            "name": job_name,
                            "tags": {
                                "job_creation_mode": "job_creator"
                            },
                            "tasks": [
                                {
                                "task_key": job_name,
                                "existing_cluster_id": cluster_id,
                                "notebook_task": {
                                    "notebook_path": notebook_path,
                                    "source": "WORKSPACE",
                                    "base_parameters": notebook_parameters
                                },
                                "timeout_seconds": 86400,
                                "email_notifications": {}       
                                }
                            ],
                            "timeout_seconds": 86400,
                            "webhook_notifications": {},
                            "email_notifications": {"no_alert_for_skipped_runs": "false"},
                            "format": "MULTI_TASK",
                            "schedule":{
                                "quartz_cron_expression": schedule,
                                "timezone_id": "Europe/Warsaw",
                                "pause_status": "PAUSED"        
                            }
                        }     
        self.delete_job_if_exists(job_name)
        self.job_client.create_job(json_command)

    def delete_job_if_exists(self,job_name):
        job_details = self.job_client._list_jobs_by_name(job_name)
        if(len(job_details) > 0):
            job_id = job_details[0].get("job_id")
            self.job_client.delete_job(job_id) 
            print(f"Job {job_name} deleted")
        
    def read_config(self,storage_account_name, storage_account_key, file_system, directory, file_name):
        try:
            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)

            file_system_client = service_client.get_file_system_client(file_system=file_system)
            directory_client = file_system_client.get_directory_client(directory)

            file_client = directory_client.get_file_client(file_name)
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            return downloaded_bytes.decode("utf-8") 
        except Exception as e:
            print(e)

    def add_new_jobs(self,config_storage_name,
                    config_storage_key, config_file_system, 
                    config_directory, config_file_name, 
                    cluster_id, 
                    notebook_path,
                    schedule):
        jobs_in_config_file = json.loads(
            self.read_config(config_storage_name, 
                            config_storage_key, 
                            config_file_system, 
                            config_directory, 
                            config_file_name
                            )
            )
        
        jobs_in_databricks = self.job_client.list_jobs()['jobs']
        job_names_in_databricks = [x['settings']['name'] for x in jobs_in_databricks 
                                        if 'tags' in x['settings'] and x['settings']['tags']['job_creation_mode'] == 'job_creator']

        for cfg in jobs_in_config_file:
            job_name = f"{cfg['domain']}_{cfg['table_name']}"
            notebook_parameters = cfg
            if job_name not in job_names_in_databricks:
                print(f"created new job {job_name}")
                self.create_job(job_name, cluster_id, notebook_path, notebook_parameters, schedule)
