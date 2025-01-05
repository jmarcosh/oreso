import functools
import os
import yaml
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import io
import pandas as pd


@functools.lru_cache()
def load_config():
    with open(os.path.join(os.path.dirname(__file__), "../config_files/mycredentials.yaml")) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


class SharePointContext:
    def __init__(self):
        self.username = load_config()['username']
        self.password = load_config()['password']
        self.site_url = load_config()['site_url']
        self.save_url = load_config()['save_url']
        # SharePoint context
        ctx_auth = AuthenticationContext(self.site_url)
        if ctx_auth.acquire_token_for_user(self.username, self.password):
            self.ctx = ClientContext(self.site_url, ctx_auth)
            self.web = self.ctx.web
            self.ctx.load(self.web)
        else:
            print()

    def read_excel_file(self, file_id, sheet_name="Sheet1", dtype=None):
        # Retrieve the Excel file by its ID
        file_info = self.web.get_file_by_id(file_id)
        self.ctx.load(file_info)
        self.ctx.execute_query()

        # Read the Excel file contents into a pandas dataframe
        with io.BytesIO(file_info.read()) as stream:
            df = pd.read_excel(stream, sheet_name=sheet_name, engine='openpyxl', dtype=dtype)
        return df

    def write_df_to_excel(self, df, folder, filename, sheet_name="Sheet1"):
        # Upload the Excel file to SharePoint
        sp_folder = self.web.get_folder_by_server_relative_url(self.save_url + folder)
        file_info = io.BytesIO()
        with pd.ExcelWriter(file_info, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        file_content = file_info.getvalue()
        file_upload = sp_folder.upload_file(filename, file_content)
        self.ctx.load(file_upload)
        self.ctx.execute_query()
