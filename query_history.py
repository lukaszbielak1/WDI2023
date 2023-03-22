import requests
import json
from azure.storage.filedatalake import DataLakeServiceClient


class query_history:
    def __init__(self, db_token, db_workspace):
        self.url = f"https://{db_workspace}/api/2.0/sql/history/queries"
        self.headers = {"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"}
    
    def get_history_from_api(self,fromTs, toTs, col_list):
        export_data = {}
        rows = []
        data = f"""     {{
                "max_results": 1000,
                "filter_by": {{
                 "query_start_time_range": 
                 {{
                    "end_time_ms": {toTs},
                    "start_time_ms": {fromTs}
                 }}       
                }}
            }}"""
        request = requests.get(self.url,data = data, headers=self.headers)
        
        if 'res' in request.json():
            while request:
                for row in request.json()["res"]:
                        for attribute, value in row.items():
                            if attribute in col_list:
                                export_data[attribute] = value
                                rows.append(export_data)

                if request.json()["has_next_page"]:
                    ntoken = request.json()["next_page_token"]
                    request = request.get(self.url+"?next_page_token="+ntoken, headers=self.headers,data=data)
                else:
                    request = None
            return json.dumps(rows)
        else:
             return None
    
    def save_to_adls(self, data, storage_account_name, storage_account_key, file_system, file_directory, file_name):
        try:
            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)

            file_system_client = service_client.get_file_system_client(file_system=file_system)
            directory_client = file_system_client.get_directory_client(file_directory)
            file_client = directory_client.create_file(file_name)
            file_client.append_data(data=data, offset=0, length=len(data))
            file_client.flush_data(len(data))
        except Exception as e:
             print(e)
