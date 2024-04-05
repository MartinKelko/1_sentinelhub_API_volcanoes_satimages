# import os
from datetime import date, timedelta
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

copernicus_user = "martin2kelko@gmail.com" # copernicus User
copernicus_password = "Nepijemrum22_22" # copernicus Password
ft = "LINESTRING (18.378160470578962 48.93531570012354, 19.25354062765919 48.95389112187124, 19.269879679349344 48.74267295558218, 18.36424446604272 48.72542958357761, 18.37033165553393 48.93531004676146)"  # AOI = get coordinates by drawing polygon in geojson.io, download as .wkt file, open the file and copy+paste here
data_collection = "SENTINEL-2" # Sentinel type

# date range
today =  date.today()
today_string = today.strftime("%Y-%m-%d")
yesterday = today - timedelta(days=10)
yesterday_string = yesterday.strftime("%Y-%m-%d")

def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Keycloak token creation failed. Response from the server was: {r.json()}"
        )
    return r.json()["access_token"]

try:
    json_ = requests.get(
        f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') and ContentDate/Start gt {yesterday_string}T00:00:00.000Z and ContentDate/Start lt {today_string}T00:00:00.000Z&$count=True&$top=1000"
    ).json()
    p = pd.DataFrame.from_dict(json_["value"]) # Fetch available dataset
    if p.shape[0] > 0 :
        p["geometry"] = p["GeoFootprint"].apply(shape)
        productDF = gpd.GeoDataFrame(p).set_geometry("geometry") # Convert PD to GPD
        productDF = productDF[~productDF["Name"].str.contains("L1C")] # Remove L1C dataset
        print(f" total L2A tiles found {len(productDF)}")
        productDF["identifier"] = productDF["Name"].str.split(".").str[0]
        allfeat = len(productDF)

        if allfeat == 0:
            print("No tiles found for today")
        else:
            ## download all tiles from server
            for index,feat in enumerate(productDF.iterfeatures()):
                try:
                    session = requests.Session()
                    keycloak_token = get_keycloak(copernicus_user,copernicus_password)
                    session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({feat['properties']['Id']})/$value"
                    response = session.get(url, allow_redirects=False)
                    while response.status_code in (301, 302, 303, 307):
                        url = response.headers["Location"]
                        response = session.get(url, allow_redirects=False)
                    print(feat["properties"]["Id"])
                    file = session.get(url, verify=False, allow_redirects=True)

                    # download directory here
                    with open(
                            f"C:/Users/marti/PycharmProjects/sentinelhub_API_volcanoes_satimages/{feat['properties']['identifier']}.zip",
                            "wb",
                    ) as p:
                        print(feat["properties"]["Name"])
                        p.write(file.content)

                except Exception as e:
                    print("Error:", e)
    else :
        print('no data found')
except Exception as e:
    print("Error:", e)
