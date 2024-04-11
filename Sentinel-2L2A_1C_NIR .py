import os
import re
from datetime import date, timedelta
import pandas as pd
import requests

# Copernicus User
copernicus_user = "martin2kelko@gmail.com"
# Copernicus Password
copernicus_password = "Nepijemrum22_22"

# Area of Interest (AOI) as WKT polygon
ft = "POLYGON ((-72.079582 -39.533174, -72.079582 -39.331907, -71.760635 -39.331907, -71.760635 -39.533174, -72.079582 -39.533174))"
# Date range
today = date.today()
yesterday = today - timedelta(days=2)
yesterday_string = yesterday.strftime("%Y-%m-%d")
today_string = today.strftime("%Y-%m-%d")

# Function to get Copernicus API access token
def get_keycloak_token(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        response = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        raise Exception(f"Keycloak token retrieval failed. Error: {e}")

# Query products function
def query_products(collection_name: str, ft: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        token = get_keycloak_token(copernicus_user, copernicus_password)
        headers = {"Authorization": f"Bearer {token}"}

        url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        params = {
            "$filter": f"Collection/Name eq '{collection_name}' "
                       f"and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') "
                       f"and ContentDate/Start gt {start_date}T00:00:00.000Z "
                       f"and ContentDate/Start lt {end_date}T23:59:59.999Z",
            "$count": "true",
            "$top": "1000"
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
        return pd.DataFrame(json_data["value"])
    except Exception as e:
        raise Exception(f"Error querying products. Error: {e}")

# Main execution
try:
    # Query Sentinel-2 Level-2A products
    products_l2a = query_products("SENTINEL-2-L2A", ft, yesterday_string, today_string)

    if not products_l2a.empty:
        # Filter out L1C datasets (if any)
        products_l2a = products_l2a[~products_l2a["Name"].str.contains("L1C")]

        print(f"Total L2A tiles found: {len(products_l2a)}")

        for idx, product in products_l2a.iterrows():
            try:
                product_id = product["Id"]
                product_name = product["Name"]

                # Download directory
                download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages"
                os.makedirs(download_directory, exist_ok=True)

                # Construct download URL
                download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"

                # Extract identifier from product name
                identifier = re.sub(r'[^a-zA-Z0-9-_]', '', product_name.split("_")[0])[:50]

                # File path for saving
                file_path = os.path.join(download_directory, f"{identifier}.zip")

                # Download product
                with requests.get(download_url, stream=True) as r:
                    r.raise_for_status()
                    with open(file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                print(f"Downloaded: {product_name}")

            except Exception as e:
                print(f"Error downloading {product_name}: {e}")

    else:
        print("No L2A tiles found for today")

except Exception as e:
    print(f"Error: {e}")
