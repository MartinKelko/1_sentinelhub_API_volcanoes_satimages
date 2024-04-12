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
        yesterday = today - timedelta(days=3)
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

                    # Extract the identifier from the product name
                    identifier = product_name.split("_")[0]
                    # Truncate or modify the identifier if needed to fit within file name limits
                    identifier = re.sub(r'[^a-zA-Z0-9-_]', '', identifier)[:50]

                    # Determine if Level-1C or Level-2A
                    if "L1C" in product_name:
                        download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-2L1C_downloads"
                    elif "L2A" in product_name:
                        download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-2L2A_downloads"
                    else:
                        continue  # Skip if neither L1C nor L2A

                    os.makedirs(download_directory, exist_ok=True)
                    file_path = os.path.join(download_directory, f"{identifier}.zip")

                    # Save the downloaded file
                    with open(file_path, "wb") as file:
                        file.write(response.content)

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
# Open Win11 Task Scheduler -> click on "Create Basic Task" -> set the
# trigger to be "Daily" and specify the time -> choose to start a program and
# provide the path to your Python executable (python.exe) and the script (
# Villarrica_S2_1C_2A.py)
schedule.every().day.at("04:30").do(query_and_download_products)

# Infinite loop to run the scheduler
print("Scheduled job started. Waiting for execution...")
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid high CPU usage
