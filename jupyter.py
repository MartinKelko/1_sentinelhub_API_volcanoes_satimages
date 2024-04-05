import matplotlib.pyplot as plt
import pandas as pd
import getpass
from sentinelhub import (
    SHConfig, DataCollection, SentinelHubCatalog, SentinelHubRequest,
    BBox, bbox_to_dimensions, CRS, MimeType
)

config = SHConfig()
config.sh_client_id = getpass.getpass("sh-dd900236-c196-4971-ac17-2f5909e6111d")
config.sh_client_secret = getpass.getpass("Zo0CLOiTnMVSa7QfG8bdQKKqnFfskrL1")
config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
config.sh_base_url = "https://sh.dataspace.copernicus.eu"
config.save("cdse")
config = SHConfig("Martin_Kelko")

# AOI
aoi_coords_wgs84 = [15.461282, 46.757161, 15.574922, 46.851514]
resolution = 10
aoi_bbox = BBox(bbox=aoi_coords_wgs84, crs=CRS.WGS84)
aoi_size = bbox_to_dimensions(aoi_bbox, resolution=resolution)

print(f"Image shape at {resolution} m resolution: {aoi_size} pixels")

# Catalog API
catalog = SentinelHubCatalog(config=config)

time_interval = "2022-07-01", "2022-07-20"

search_iterator = catalog.search(
    DataCollection.SENTINEL2_L2A,
    bbox=aoi_bbox,
    time=time_interval,
    fields={"include": ["id", "properties.datetime"], "exclude": []},
)

results = list(search_iterator)
print("Total number of results:", len(results))

# True Color
evalscript_true_color = """
    //VERSION=3

    function setup() {
        return {
            input: [{
                bands: ["B02", "B03", "B04"]
            }],
            output: {
                bands: 3
            }
        };
    }

    function evaluatePixel(sample) {
        return [sample.B04, sample.B03, sample.B02];
    }
"""

request_true_color = SentinelHubRequest(
    evalscript=evalscript_true_color,
    input_data=[
        SentinelHubRequest.input_data(
            data_collection=DataCollection.SENTINEL2_L2A,
            time_interval=time_interval,
            other_args={"dataFilter": {"mosaickingOrder": "leastCC"}},
        )
    ],
    responses=[SentinelHubRequest.output_response("default", MimeType.PNG)],
    bbox=aoi_bbox,
    size=aoi_size,
    config=config,
)

true_color_imgs = request_true_color.get_data()
print(
    f"Returned data is of type = {type(true_color_imgs)} and length {len(true_color_imgs)}."
)
print(
    f"Single element in the list is of type {type(true_color_imgs[-1])} and has shape {true_color_imgs[-1].shape}"
)

# Plot the image
if true_color_imgs:
    image = true_color_imgs[0]
    plt.imshow(image)
    plt.axis('off')
    plt.show()
else:
    print("No image data returned.")
