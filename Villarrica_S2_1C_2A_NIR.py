import os
import re
from datetime import date, timedelta
import pandas as pd
import requests
import schedule
import time

# Copernicus User
copernicus_user = "martin2kelko@gmail.com"
# Copernicus Password
copernicus_password = "Nepijemrum22_22"


# Copernicus Browser API Token
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


# Function to download Level-1C product
def download_level1c(product_name: str, response: requests.Response,
                     download_directory: str):
    # Extract the identifier from the product name
    identifier = product_name.split("_")[0]
    # Truncate or modify the identifier if needed to fit within file name limits
    identifier = re.sub(r'[^a-zA-Z0-9-_]', '', identifier)[:50]

    # Save the downloaded Level-1C product
    file_path = os.path.join(download_directory, f"{identifier}_L1C.zip")
    with open(file_path, "wb") as file:
        file.write(response.content)


# Function to download Level-2A product
def download_level2a(product_name: str, response: requests.Response,
                     download_directory: str):
    # Extract the identifier from the product name
    identifier = product_name.split("_")[0]
    # Truncate or modify the identifier if needed to fit within file name limits
    identifier = re.sub(r'[^a-zA-Z0-9-_]', '', identifier)[:50]

    # Save the downloaded Level-2A product
    file_path = os.path.join(download_directory, f"{identifier}_L2A.zip")
    with open(file_path, "wb") as file:
        file.write(response.content)


# Function to download NIR composite image
def download_nir_composite(product_name: str, response: requests.Response,
                           download_directory: str):
    # Extract the identifier from the product name
    identifier = product_name.split("_")[0]
    # Truncate or modify the identifier if needed to fit within file name limits
    identifier = re.sub(r'[^a-zA-Z0-9-_]', '', identifier)[:50]

    # Determine NIR composite bands based on product type
    if "L1C" in product_name:
        bands = ["B08", "B04", "B03"]  # NIR composite bands for L1C
    elif "L2A" in product_name:
        bands = ["B12", "B11", "B04"]  # NIR composite bands for L2A
    else:
        return  # Skip if neither L1C nor L2A

    # Save the downloaded NIR composite file
    file_path = os.path.join(download_directory, f"{identifier}_NIR.zip")
    with open(file_path, "wb") as file:
        file.write(response.content)


# Copernicus Browser catalogue and download products
def query_and_download_products():
    try:
        # Villarrica coordinates = get coordinates by drawing polygon in
        # Copernicus Browser, copy+paste
        # coordinates in geojson.io, download as .wkt file, open the .wkt file and copy+paste text here
        ft = "POLYGON ((-72.079582 -39.533174, -72.079582 -39.331907, -71.760635 -39.331907, -71.760635 -39.533174, -72.079582 -39.533174))"

        # Date range
        today = date.today()
        today_string = today.strftime("%Y-%m-%d")
        yesterday = today - timedelta(days=2)
        yesterday_string = yesterday.strftime("%Y-%m-%d")

        # Query the Copernicus catalogue for matching products
        response = requests.get(
            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
            f"$filter=Collection/Name eq 'SENTINEL-2' and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{ft}') and "
            f"ContentDate/Start gt {yesterday_string}T00:00:00.000Z and "
            f"ContentDate/Start lt {today_string}T00:00:00.000Z&$count=True&$top=1000"
        )
        response.raise_for_status()

        json_data = response.json()
        products = pd.DataFrame.from_dict(json_data["value"])

        if not products.empty:
            print(f"Total products found: {len(products)}")

            # Specify the download directory for NIR composite
            nir_download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-NIR_downloads"
            os.makedirs(nir_download_directory, exist_ok=True)

            for idx, product in products.iterrows():
                try:
                    session = requests.Session()
                    keycloak_token = get_keycloak_token(copernicus_user,
                                                        copernicus_password)
                    session.headers.update(
                        {"Authorization": f"Bearer {keycloak_token}"})

                    product_id = product["Id"]
                    product_name = product["Name"]

                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
                    response = session.get(url, allow_redirects=False)

                    while response.status_code in (301, 302, 303, 307):
                        url = response.headers["Location"]
                        response = session.get(url, allow_redirects=False)

                    print(f"Downloading: {product_name}")

                    # Determine download directory based on product type
                    if "L1C" in product_name:
                        download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-2L1C_downloads"
                        os.makedirs(download_directory, exist_ok=True)
                        download_level1c(product_name, response,
                                         download_directory)
                    elif "L2A" in product_name:
                        download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-2L2A_downloads"
                        os.makedirs(download_directory, exist_ok=True)
                        download_level2a(product_name, response,
                                         download_directory)

                    # Download the NIR composite using the specified directory
                    download_nir_composite(product_name, response,
                                           nir_download_directory)

                except Exception as e:
                    print(f"Error downloading {product_name}: {e}")

        else:
            print("No products found for the required date range")

    except Exception as e:
        print(f"Error in downloading products: {e}")


# Automate test the function outside of scheduling
print("Automatically starting the script...")
query_and_download_products()
print("Automate test complete.")

# Scheduling the script every day at 4:30 AM
schedule.every().day.at("04:30").do(query_and_download_products)

# Infinite loop to run the scheduler
print("Scheduled job started. Waiting for execution...")
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid high CPU usage
