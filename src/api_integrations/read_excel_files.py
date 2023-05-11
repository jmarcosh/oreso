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

    def read_excel_file(self, file_id, sheet_name="Sheet1"):
        # Retrieve the Excel file by its ID
        file_info = self.web.get_file_by_id(file_id)
        self.ctx.load(file_info)
        self.ctx.execute_query()

        # Read the Excel file contents into a pandas dataframe
        with io.BytesIO(file_info.read()) as stream:
            df = pd.read_excel(stream, sheet_name=sheet_name, engine='openpyxl')
        return df

    def write_df_to_excel(self, df, folder, filename, sheet_name="Sheet1"):
        # Upload the Excel file to SharePoint
        folder = self.web.get_folder_by_server_relative_url(self.save_url + folder)
        file_info = io.BytesIO()
        with pd.ExcelWriter(file_info, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        file_content = file_info.getvalue()
        file_upload = folder.upload_file(filename, file_content)
        self.ctx.load(file_upload)
        self.ctx.execute_query()


# s = SharePointContext()
# data = s.read_excel_file("712C6CA3-0ACE-4F7A-B7E2-7A3B7AA5FCEA", 'Delta')
# s.write_df_to_excel(data, '', "mydata.xlsx")
# 1

# # SharePoint site URL and credentials
#
# username = "joseph.m@oreso.com.mx"
# password = "JiO1919192"
#
# # SharePoint folder URL and filename
# folder_url = "/sites/SERVORESO/Shared Documents"
# filename = "mydata.xlsx"
#
# # DataFrame to be saved as Excel file
# data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 35]}
# df = pd.DataFrame(data)
#
#
#     # Upload the Excel file to SharePoint
#     folder = web.get_folder_by_server_relative_url(folder_url)
#     file_info = io.BytesIO()
#     with pd.ExcelWriter(file_info, engine='xlsxwriter') as writer:
#         df.to_excel(writer, sheet_name='Sheet1', index=False)
#     file_content = file_info.getvalue()
#     file_upload = folder.upload_file(filename, file_content)
#     ctx.load(file_upload)
#     ctx.execute_query()
#
#     print("File uploaded to SharePoint:", file_upload.serverRelativeUrl)
#
# else:
#     print("Failed to authenticate")
#
#
#
#
#
# from office365.runtime.auth.authentication_context import AuthenticationContext
# from office365.sharepoint.client_context import ClientContext
# import io
# import pandas as pd
#
# # SharePoint site URL and credentials
# site_url = "https://oreso.sharepoint.com/sites/SERVORESO"
# username = "joseph.m@oreso.com.mx"
# password = "JiO1919192"
#
# # # File ID and SharePoint context
# # ctx_auth = AuthenticationContext(site_url)
# # if ctx_auth.acquire_token_for_user(username, password):
# #     ctx = ClientContext(site_url, ctx_auth)
# #     web = ctx.web
# #     ctx.load(web)
#
#     # Retrieve the Excel file by its ID
#     file_id = "BF5D945C-7B3C-4427-86A8-40B65DADB075"
#     file_info = web.get_file_by_id(file_id)
#     ctx.load(file_info)
#     ctx.execute_query()
#
#     # Read the Excel file contents into a pandas dataframe
#     with io.BytesIO(file_info.read()) as stream:
#         df = pd.read_excel(stream, sheet_name="Sheet2", engine='openpyxl')
#
# else:
#     print("Failed to authenticate")
#
# SharePoint folder URL and filename
# folder_url = "/sites/SERVORESO/Shared Documents"
# filename = "mydata.xlsx"
#
# # # SharePoint context
# # ctx_auth = AuthenticationContext(site_url)
# # if ctx_auth.acquire_token_for_user(username, password):
# #     ctx = ClientContext(site_url, ctx_auth)
# #     web = ctx.web
# #     ctx.load(web)
#

#
#     print("File uploaded to SharePoint:", file_upload.serverRelativeUrl)
#
# else:
#     print("Failed to authenticate")
#
#
#
# import requests
# import pandas as pd
#
# # access_token = 'eyJ0eXAiOiJKV1QiLCJub25jZSI6IllBWkZKSEY2dkJ5aTVtUDdlam40dUZWV3Q3T04wdmxKeWFMSHRKay13QVEiLCJhbGciOiJSUzI1NiIsIng1dCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyIsImtpZCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyJ9.eyJhdWQiOiJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83YTQ0NmM4Zi04MGUzLTQwNDctOGI1OS01ZmI2MWI4N2Y5ZTUvIiwiaWF0IjoxNjc2ODMxNDg0LCJuYmYiOjE2NzY4MzE0ODQsImV4cCI6MTY3NjgzNTM4NCwiYWlvIjoiRTJaZ1lLZ0lhNWl5Y2YzYUo1UHVQODV1clhYNER3QT0iLCJhcHBfZGlzcGxheW5hbWUiOiJqb3NlcGgubSIsImFwcGlkIjoiM2EyNmM1NzYtYzg0Yi00MjY2LWEwNTctYjIyMTQ4YzE5NGRhIiwiYXBwaWRhY3IiOiIxIiwiaWRwIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvN2E0NDZjOGYtODBlMy00MDQ3LThiNTktNWZiNjFiODdmOWU1LyIsImlkdHlwIjoiYXBwIiwib2lkIjoiZGQ0MWM4NjEtMTI3ZS00YzhiLWFiYzYtNWYxZTI2OTkwOWFhIiwicmgiOiIwLkFTd0FqMnhFZXVPQVIwQ0xXVi0yRzRmNTVRTUFBQUFBQUFBQXdBQUFBQUFBQUFBc0FBQS4iLCJzdWIiOiJkZDQxYzg2MS0xMjdlLTRjOGItYWJjNi01ZjFlMjY5OTA5YWEiLCJ0ZW5hbnRfcmVnaW9uX3Njb3BlIjoiTkEiLCJ0aWQiOiI3YTQ0NmM4Zi04MGUzLTQwNDctOGI1OS01ZmI2MWI4N2Y5ZTUiLCJ1dGkiOiJXMmdCeXM0UTJFT01CeXpHYVkwN0FBIiwidmVyIjoiMS4wIiwid2lkcyI6WyIwOTk3YTFkMC0wZDFkLTRhY2ItYjQwOC1kNWNhNzMxMjFlOTAiXSwieG1zX3RjZHQiOjE1MDY2MTYzMDB9.IAutWuVgwfWBGDweiLiMrSyK1N7V5v7RfBRLgFaYEcs0Dd1IRtjOzdqwsr0OwpkRRc1Ew1XhorU-2Cpc0G2LhMcv13NSSdUF2AgqxBWFkVf9AO6Jl5e-p4iGIIrODzx5oTT5sHvKP7GQw2Qox7GyhPyoMrDPfANA8HyVYuf4RpvRYED68zMuJxJMbxugDbdqOBbec53NRMRtFCi9Xx5XILgKyyD6FP-1KzdBjJ2XQnlTdVUpzP_QQB_aokYx71F7NdAk4xv-tq3Zyf26Gu3WStV1IJX6F2u89GcqxgWZLwoWKN4LGkC6B694VJ9icIiSeIaX5vhdyk0ndNG7UpFEUg'
#
# import msal
#
# def acquire_token_func():
#     """
#     Acquire token via MSAL
#     """
#     authority_url = 'https://login.microsoftonline.com/7a446c8f-80e3-4047-8b59-5fb61b87f9e5'
#     app = msal.ConfidentialClientApplication(
#         authority=authority_url,
#         client_id='3a26c576-c84b-4266-a057-b22148c194da',
#         client_credential='dS58Q~BdVRt~cU4FeHsx_BwPkYWm4eY3JLDnZaPs'
#     )
#     token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
#     return token
#
# from office365.graph_client import GraphClient
#
# tenant_name = "oreso.onmicrosoft.com"
# access_token = acquire_token_func
# client = GraphClient(access_token)
# drives = client.drives.get().execute_query()
# for drive in drives:
#     print("Drive url: {0}".format(drive.web_url))
#
#
# headers = {
#     "Authorization": f"Bearer {access_token}",
#     "Accept": "application/json"
# }
#
# site_name = "adel_h_oreso_com_mx"
# filename = "230122 DICIEMBRE 2022.xlsx"
#
# url = f"https://graph.microsoft.com/v1.0/sites/{site_name}/drive/root/search(q='{filename}')"
#
# response = requests.get(url, headers=headers)
#
# if response.status_code == 200:
#     data = response.json()
#     file_id = data['value'][0]['id']
#     drive_id = data['value'][0]['parentReference']['driveId']
# else:
#     print(f"Error getting file info: {response.status_code} - {response.text}")
#
#
#
# # Set the endpoint and headers
# url = 'https://graph.microsoft.com/v1.0/sites/root:/sites/{site_name}:/drive/items/{item_id}/workbook/worksheets/{sheet_name}/usedRange/values'
# headers = {
#     'Authorization': f'Bearer {access_token}',
#     'Content-Type': 'application/json'
# }
#
# # Set the site name, item ID, and sheet name
# site_name = 'adel_h_oreso_com_mx'
# item_id = 'D1B099EE-BFDE-4B65-86DC-A0ADE9F76F7E'
# sheet_name = 'RESUMEN'
#
# # Build the URL
# url = url.format(site_name=site_name, item_id=item_id, sheet_name=sheet_name)
#
# # Send the request to download the file
# response = requests.get(url, headers=headers)
#
# # Check if the request was successful
# if response.status_code == 200:
#     # Read the Excel data into a Pandas DataFrame
#     df = pd.DataFrame(response.json())
#     # Print the DataFrame
#     print(df)
# else:
#     print('Failed to download file.')
