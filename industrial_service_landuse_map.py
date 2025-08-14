"""
industrial_service_landuse_map_from_github.py

Pipeline (sem OSMnx):
1) Carrega landuse direto de URLs do GitHub (GeoPackage .gpkg)
2) Classifica setor (service / industrial) e intensidade (1-4)
3) Exporta camadas enriquecidas (GPKG/CSV)
4) Gera mapas estáticos (PNG) com matplotlib

Requisitos:
    pip install geopandas pandas shapely matplotlib fiona

Observação:
- Use URLs "raw" do GitHub, por ex.:
  https://github.com/<user>/<repo>/raw/main/data/landuse__vancouver.gpkg
"""

import os
import re
import json
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------
# CONFIG: edite aqui as URLs para seus arquivos no GitHub
# ---------------------------------------------------------------------
LANDUSE_URLS = {
    "Vancouver, British Columbia, Canada":
        "https://github.com/helioavila/industrial-service-landuse-intensity-map/raw/main/data/landuse__vancouver.gpkg",
    "City of North Vancouver, British Columbia, Canada":
        "https://github.com/helioavila/industrial-service-landuse-intensity-map/raw/main/data/landuse__city_of_north_vancouver.gpkg"
    # se seu arquivo for "landuse__north_vancouver.gpkg", ajuste a URL acima
}

GPKG_LAYER_NAME = "landuse"  # layer salvo no export anterior

OUT_DATA = "data"
OUT_MAPS = "maps"
os.makedirs(OUT_DATA, exist_ok=True)
os.makedirs(OUT_MAPS, exist_ok=True)

# Paletas (claro -> escuro)
BLUE   = {1: "#E8F1FA", 2: "#A9C8EA", 3: "#5B8FCB", 4: "#1F4E94"}   # Services
ORANGE = {1: "#FCECDD", 2: "#F7B87A", 3: "#E97A1C", 4: "#8C3D06"}  # Industrial
NEUTRAL = "#DDDDDD"

# Campos comuns a verificar (se existirem)
ATTR_FIELDS = [
    "landuse","building","name","amenity","shop","office","industrial","craft",
    "man_made","operator","brand","description","notes","addr:housenumber","addr:street"
]

# Regras de classificação (ajuste conforme necessário)
RULES = {
    "industrial": {
        4: ["refinery","heavy","smelter","processing","plant","steel"],
        3: ["logistics","distribution","warehouse","utility","substation"],
        2: ["industrial","light","workshop","manufactur","depot"],
        1: ["storage","craft"],
    },
    "service": {
        4: ["corporate hq","headquarters","campus"],
        3: ["it","data center","telecom","tech park","technology park"],
        2: ["office","shop","school","college","university"],
        1: ["service","clinic","health","hairdresser","cafe","restaurant"],
    },
}

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+","_",s.lower()).strip("_")

def text_from_row(row: pd.Series) -> str:
    """
    Concatena valores textuais da linha (exceto geometria) de forma robusta,
    ignorando NaN/None e tolerando listas/Series.
    """
    parts = []
    for key, val in row.items():
        if key == "geometry":
            continue
        if val is None:
            continue
        try:
            is_na = pd.isna(val)
            if hasattr(is_na, "__iter__"):
                # se vier array/Series de bools, considera NaN somente se todos forem True
                if bool(pd.Series(is_na).all()):
                    continue
            else:
                if is_na:
                    continue
        except Exception:
            pass

        if isinstance(val, (list, tuple, set)):
            for x in val:
                if x is not None:
                    try:
                        if pd.isna(x): 
                            continue
                    except Exception:
                        pass
                    parts.append(str(x))
        elif isinstance(val, pd.Series):
            parts.append(" ".join(map(str, val.dropna().tolist())))
        else:
            parts.append(str(val))
    return " ".join(parts).lower()

def classify(row: pd.Series):
    txt = text_from_row(row)

    # Industrial primeiro (evita confundir fábrica com serviço)
    for intensity, keys in RULES["industrial"].items():
        if any(k in txt for k in keys):
            return "industrial", intensity

    # Depois serviços
    for intensity, keys in RULES["service"].items():
        if any(k in txt for k in keys):
            return "service", intensity

    # fallback leve por landuse
    lu = (row.get("landuse") or "").lower()
    if lu == "industrial":
        return "industrial", 2
    if lu == "commercial":
        return "service", 2

    return None, None

def color_for(sector, intensity):
    if sector == "service":
        return BLUE.get(intensity, BLUE[1])
    if sector == "industrial":
        return ORANGE.get(intensity, ORANGE[1])
    return NEUTRAL

def load_landuse_from_github(url: str, layer: str) -> gpd.GeoDataFrame:
    """
    Lê um GeoPackage hospedado no GitHub (URL com /raw/) usando fiona/GDAL.
    """
    print(f"[load] {url}")
    gdf = gpd.read_file(url, layer=layer)
    # garantir somente polígonos
    gdf = gdf[gdf.geometry.type.isin(["Polygon","MultiPolygon"])].copy()
    # manter apenas campos úteis (se existirem)
    keep = [c for c in ["landuse","name"] + ATTR_FIELDS if c in gdf.columns] + ["geometry"]
    return gdf[keep].copy()

def enrich(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.empty:
        gdf["sector"]=[]; gdf["intensity"]=[]; gdf["fill"]=[]; return gdf
    pairs = gdf.apply(classify, axis=1)
    gdf["sector"]    = [p[0] for p in pairs]
    gdf["intensity"] = [p[1] for p in pairs]
    gdf["fill"]      = gdf.apply(lambda r: color_for(r["sector"], r["intensity"]), axis=1)
    return gdf

def save_outputs(gdf: gpd.GeoDataFrame, base: str):
    gpkg = os.path.join(OUT_DATA, f"{base}.gpkg")
    csv  = os.path.join(OUT_DATA, f"{base}.csv")
    if not gdf.empty:
        gdf.to_file(gpkg, layer="landuse", driver="GPKG")
        gdf.drop(columns="geometry").to_csv(csv, index=False)
    print(f"  -> {gpkg} | {csv}")

def plot_matplotlib(gdf: gpd.GeoDataFrame, title: str, out_png: str):
    if gdf.empty:
        print(f"[plot] {title}: empty, skip")
        return
    fig, ax = plt.subplots(figsize=(10,10))
    # contorno fininho branco ajuda a leitura em áreas densas
    gdf.plot(ax=ax, color=gdf["fill"], edgecolor="white", linewidth=0.15)
    ax.set_title(title, fontsize=12)
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig(out_png, dpi=220)
    plt.close(fig)
    print(f"  -> {out_png}")

# ---------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------
def process_city(city: str, url: str) -> gpd.GeoDataFrame:
    gdf = load_landuse_from_github(url, GPKG_LAYER_NAME)
    gdf["city"] = city
    gdf = enrich(gdf)
    base = f"landuse__{slug(city)}"
    save_outputs(gdf, base)
    plot_matplotlib(gdf, f"Industrial & Service Land Use Intensity – {city}",
                    os.path.join(OUT_MAPS, f"{base}.png"))
    return gdf

def main():
    all_gdfs = []
    for city, url in LANDUSE_URLS.items():
        g = process_city(city, url)
        if not g.empty:
            all_gdfs.append(g)

    # Merge (Vancouver + North Vancouver)
    if all_gdfs:
        all_ = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True), crs=4326)
        base = "landuse__vancouver__north_vancouver"
        save_outputs(all_, base)
        plot_matplotlib(all_, "Industrial & Service Land Use Intensity – Vancouver + North Vancouver",
                        os.path.join(OUT_MAPS, f"{base}.png"))

if __name__ == "__main__":
    main()
