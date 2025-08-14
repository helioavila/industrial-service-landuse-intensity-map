
# export_landuse_buildings.py
# Downloads land use and building footprints for
# - City of Vancouver
# - City of North Vancouver

# using OSMnx, and saves them as CSV and GeoPackage (.gpkg) for further analysis.

# Requirements:
# pip install osmnx geopandas pandas

import os
import pandas as pd
import geopandas as gpd
import osmnx as ox

CITIES = [
    "Vancouver, British Columbia, Canada",
    "City of North Vancouver, British Columbia, Canada",
]

OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)


def fetch_boundary(place):
    print(f"[boundary] {place}")
    gdf = ox.geocode_to_gdf(place).to_crs(4326)
    return gdf.geometry.values[0]


def export_layer(gdf, base_name, layer_name):
    gpkg = os.path.join(OUT_DIR, f"{base_name}.gpkg")
    csv = os.path.join(OUT_DIR, f"{base_name}.csv")
    gdf.to_file(gpkg, layer=layer_name, driver="GPKG")
    gdf.drop(columns="geometry").to_csv(csv, index=False)
    print(f"  -> {gpkg} | {csv}")


def main():
    all_landuse = []
    all_buildings = []

    for city in CITIES:
        city_slug = city.lower().replace(" ", "_").replace(",", "").replace("__", "_")
        geom = fetch_boundary(city)

        # --- Landuse
        print(f"[landuse] {city}")
        lu = ox.features_from_polygon(geom, tags={"landuse": True})
        lu = lu[["landuse", "name", "geometry"]].copy()
        lu["city"] = city
        export_layer(lu, f"landuse__{city_slug}", "landuse")
        all_landuse.append(lu)

        # --- Buildings
        print(f"[buildings] {city}")
        bld = ox.features_from_polygon(geom, tags={"building": True})
        keep = [c for c in ["building", "name", "addr:housenumber",
                            "addr:street", "height", "levels", "geometry"] if c in bld.columns]
        bld = bld[keep].copy()
        bld["city"] = city
        export_layer(bld, f"buildings__{city_slug}", "buildings")
        all_buildings.append(bld)

    # --- Merge (both cities)
    if all_landuse:
        lu_all = gpd.GeoDataFrame(
            pd.concat(all_landuse, ignore_index=True), crs=4326)
        export_layer(lu_all, "landuse__vancouver__north_vancouver", "landuse")

    if all_buildings:
        bld_all = gpd.GeoDataFrame(
            pd.concat(all_buildings, ignore_index=True), crs=4326)
        export_layer(
            bld_all, "buildings__vancouver__north_vancouver", "buildings")

    print("✅ Done.")


if __name__ == "__main__":
    main()
