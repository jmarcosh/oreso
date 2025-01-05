from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import requests

client_id = '3a26c576-c84b-4266-a057-b22148c194da'
client_secret = 'dS58Q~BdVRt~cU4FeHsx_BwPkYWm4eY3JLDnZaPs'
tenant = '7a446c8f-80e3-4047-8b59-5fb61b87f9e5'

# Set the endpoint and parameters
url = f'https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token'
payload = {
    'grant_type': 'client_credentials',
    'client_id': f'{client_id}',
    'client_secret': f'{client_secret}',
    'scope': 'https://graph.microsoft.com/.default'
}

# Send the request
response = requests.post(url, data=payload)

# Check if the request was successful
if response.status_code == 200:
    # Get the access token from the response
    access_token = response.json()['access_token']
    print(access_token)
else:
    print('Failed to retrieve the access token.')



# client_credentials = ClientCredential(f'{client_id}', f'{client_secret}')
# ctx = ClientContext(f'{url}').with_credentials(client_credentials)
# target_web = ctx.web.get().execute_query()
# print(target_web.url)
1