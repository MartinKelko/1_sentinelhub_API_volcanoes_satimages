from sentinelsat import SentinelAPI
from datetime import datetime

user = 'Martin Kelko'
password = 'Nepijemrum22_22'

# Connect to the Copernicus API
api = SentinelAPI(user, password, 'https://apihub.copernicus.eu/apihub')

# Extract coordinates and time range from the JSON data
data = [
    {
        "_id": "b88dfbcc-779f-4796-a004-abc93f769d0b-pin",
        "title": "Sentinel-2 L1C: Custom (Default)",
        "lat": -8.064583520533905,
        "lng": 114.26158905029297,
        "fromTime": "2024-03-30T00:00:00.000Z",
        "toTime": "2024-03-30T23:59:59.999Z",
        "cloudCoverage": 30
    }
]

for item in data:
    lat = item['lat']
    lon = item['lng']
    from_time = datetime.strptime(item['fromTime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
    to_time = datetime.strptime(item['toTime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
    cloud_coverage = item['cloudCoverage']

    # Search for Sentinel-2 images
    products = api.query(area=(lat, lon),
                         date=(from_time, to_time),
                         platformname='Sentinel-2',
                         cloudcoverpercentage=(0, cloud_coverage))

    # Iterate over the search results and print the product IDs
    for product_id in products:
        print("Product ID:", product_id)

    # Download all results from the search
    api.download_all(products)

    # Convert to Pandas DataFrame
    products_df = api.to_dataframe(products)

    # GeoJSON FeatureCollection containing footprints and metadata of the scenes
    api.to_geojson(products)

    # GeoPandas GeoDataFrame with the metadata of the scenes and the footprints as geometries
    api.to_geodataframe(products)

    # Get basic information about the product: its title, file size, MD5 sum, date, footprint and
    # its download url (replace 'your_product_id' with the actual ID)
    for product_id in products:
        api.get_product_odata(product_id)

    # Get the product's full metadata available on the server (replace 'your_product_id' with the actual ID)
    for product_id in products:
        api.get_product_odata(product_id, full=True)
