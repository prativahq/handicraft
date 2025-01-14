import os
import requests
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def get_access_token():
    try:
        # Get credentials from environment
        consumer_key = os.getenv("CONSUMER_KEY")
        consumer_secret = os.getenv("CONSUMER_SECRET")
        username = os.getenv("SALESFORCE_USERNAME")
        password = os.getenv("PASSWORD")
        instance_url = os.getenv("INSTANCE_URL")
        
        logger.info("Retrieved credentials from environment", consumer_key, consumer_secret, username, password, instance_url)

        # Validate credentials
        if not all([consumer_key, consumer_secret, username, password, instance_url]):
            raise ValueError("Missing required environment variables")

        # Prepare request
        auth_url = f"{instance_url}/services/oauth2/token"
        payload = {
            'grant_type': 'password',
            'client_id': consumer_key,
            'client_secret': consumer_secret,
            'username': username,
            'password': password
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        # Make request
        response = requests.post(auth_url, data=payload,headers=headers)
        # Log response details
        logger.info(f"Status Code: {response.status_code}")
        logger.info(f"Response Content: {response.text}")
        response.raise_for_status()
        
        # Get token
        token_data = response.json()
        access_token = token_data['access_token']
        logger.info("Successfully retrieved access token")
        return access_token

    except Exception as e:
        logger.error(f"Error getting access token: {str(e)}")
        return None

if __name__ == "__main__":
    token = get_access_token()
    if token:
        print(f"Access Token: {token} ✅")
    else:
        print("Failed to get access token ❌")