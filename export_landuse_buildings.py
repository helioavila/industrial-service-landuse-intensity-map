# export_landuse_buildings_csv.py
# Baixa usos do solo e edificações para:
# - City of Vancouver
# - City of North Vancouver
# usando OSMnx e salva SOMENTE como CSV (com geometria em WKT).

# Requisitos:
# pip install osmnx geopandas pandas shapely

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


def slugify(text: str) -> str:
    return (
        text.lower()
        .replace(" ", "_")
        .replace(",", "")
        .replace("__", "_")
    )


def fetch_boundary(place):
    """Geocodifica e retorna a geometria (Polygon/MultiPolygon) em EPSG:4326."""
    print(f"[boundary] {place}")
    gdf = ox.geocode_to_gdf(place).to_crs(4326)
    return gdf.geometry.values[0]


def to_csv_with_wkt(gdf: gpd.GeoDataFrame, csv_path: str):
    """Converte geometria para WKT e salva CSV."""
    # Garante CRS WGS84 (lon/lat) para WKT legível
    if gdf.crs is None or int(gdf.crs.to_epsg() or 4326) != 4326:
        gdf = gdf.to_crs(4326)

    gdf = gdf.copy()
    # Converte geometria para WKT
    gdf["geometry"] = gdf.geometry.to_wkt()
    # Salva
    gdf.to_csv(csv_path, index=False)
    print(f"  -> {csv_path}")


def export_layer(gdf: gpd.GeoDataFrame, base_name: str):
    """Prepara e exporta um GeoDataFrame para CSV com WKT."""
    csv = os.path.join(OUT_DIR, f"{base_name}.csv")
    to_csv_with_wkt(gdf, csv)


def main():
    all_landuse = []
    all_buildings = []

    for city in CITIES:
        city_slug = slugify(city)
        geom = fetch_boundary(city)

        # --- Landuse
        print(f"[landuse] {city}")
        lu = ox.features_from_polygon(geom, tags={"landuse": True})
        # garante colunas presentes conforme disponibilidade
        keep_lu = [c for c in ["landuse", "name", "geometry"] if c in lu.columns]
        lu = lu[keep_lu].copy()
        lu["city"] = city
        export_layer(lu, f"landuse__{city_slug}")
        all_landuse.append(lu)

        # --- Buildings
        print(f"[buildings] {city}")
        bld = ox.features_from_polygon(geom, tags={"building": True})
        keep_bld = [
            c for c in [
                "building", "name", "addr:housenumber",
                "addr:street", "height", "levels", "geometry"
            ] if c in bld.columns
        ]
        bld = bld[keep_bld].copy()
        bld["city"] = city
        export_layer(bld, f"buildings__{city_slug}")
        all_buildings.append(bld)

    # --- Merge (ambas cidades)
    if all_landuse:
        lu_all = gpd.GeoDataFrame(pd.concat(all_landuse, ignore_index=True), crs=4326)
        export_layer(lu_all, "landuse__vancouver__north_vancouver")

    if all_buildings:
        bld_all = gpd.GeoDataFrame(pd.concat(all_buildings, ignore_index=True), crs=4326)
        export_layer(bld_all, "buildings__vancouver__north_vancouver")

    print("✅ Done (CSV com WKT gerados em ./data).")


if __name__ == "__main__":
    main()
