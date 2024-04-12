import os
import re
from datetime import date, timedelta
import pandas as pd
import requests
import schedule
import time
from PIL import Image
import zipfile

# Copernicus User
copernicus_user = "martin2kelko@gmail.com"
# Copernicus Password
copernicus_password = "Nepijemrum22_22"

# Function to retrieve Keycloak token for Copernicus API access
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

# Function to generate false color composite image using specific bands
def generate_false_color_composite(product_name: str, download_directory: str, output_directory: str, band_paths: list):
    try:
        identifier = product_name.split("_")[0]
        identifier = re.sub(r'[^a-zA-Z0-9-_]', '', identifier)[:50]

        with zipfile.ZipFile(os.path.join(download_directory, f"{identifier}.zip"), 'r') as zip_ref:
            bands = ["B12", "B11", "B04"]  # Bands to use for false color composite
            composite_image = [None] * len(bands)

            for band_path in band_paths:
                for i, band in enumerate(bands):
                    filename = f"{identifier}_{band}_10m.jp2"
                    path_in_zip = os.path.join(*band_path[:-1], filename)

                    if path_in_zip in zip_ref.namelist():
                        with zip_ref.open(path_in_zip) as file:
                            image = Image.open(file)
                            composite_image[i] = image.split()[0]
                    else:
                        print(f"File {filename} not found in the archive for product {product_name}")

            if all(composite_image):
                # Merge bands into RGB channels (B12 -> R, B11 -> G, B04 -> B)
                composite_image = Image.merge("RGB", (composite_image[0], composite_image[1], composite_image[2]))

                # Save the false color composite image
                file_path = os.path.join(output_directory, f"{identifier}_FalseColor.jpg")
                composite_image.save(file_path)
                print(f"False color composite generated for {product_name}")
            else:
                print(f"Unable to generate false color composite for {product_name}: Insufficient band data")

    except Exception as e:
        print(f"Error generating false color composite for {product_name}: {e}")


# Function to query Copernicus catalogue and generate false color composites
def query_and_generate_false_color_composites():
    try:
        # Villarrica coordinates - specify the polygon of interest
        ft = "POLYGON ((-72.079582 -39.533174, -72.079582 -39.331907, -71.760635 -39.331907, -71.760635 -39.533174, -72.079582 -39.533174))"

        # Date range for querying products
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

            download_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-NIR_downloads"
            output_directory = r"C:\Users\marti\PycharmProjects\sentinelhub_API_volcanoes_satimages\Sentinel-FalseColor_images"

            os.makedirs(download_directory, exist_ok=True)
            os.makedirs(output_directory, exist_ok=True)

            for idx, product in products.iterrows():
                try:
                    session = requests.Session()
                    keycloak_token = get_keycloak_token(copernicus_user, copernicus_password)
                    session.headers.update({"Authorization": f"Bearer {keycloak_token}"})

                    product_id = product["Id"]
                    product_name = product["Name"]

                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
                    response = session.get(url, allow_redirects=False)

                    while response.status_code in (301, 302, 303, 307):
                        url = response.headers["Location"]
                        response = session.get(url, allow_redirects=False)

                    print(f"Downloading: {product_name}")

                    identifier = product_name.split("_")[0]
                    identifier = re.sub(r'[^a-zA-Z0-9-_]', '', identifier)[:50]
                    file_path = os.path.join(download_directory, f"{identifier}.zip")

                    with open(file_path, "wb") as file:
                        file.write(response.content)

                    # Extract band paths from product name
                    band_paths = [re.split(r'\\|/', path) for path in product_name.split(",")]

                    print(f"Generating false color composite for: {product_name}")
                    generate_false_color_composite(product_name, download_directory, output_directory, band_paths)

                except Exception as e:
                    print(f"Error processing {product_name}: {e}")

        else:
            print("No products found for the required date range")

    except Exception as e:
        print(f"Error in querying and processing products: {e}")

# Automate test the function outside of scheduling
print("Automatically starting the script...")
query_and_generate_false_color_composites()
print("Automate test complete.")

# Scheduling the script every day at 4:30 AM
schedule.every().day.at("04:30").do(query_and_generate_false_color_composites)

# Infinite loop to run the scheduler
print("Scheduled job started. Waiting for execution...")
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid high CPU usage
