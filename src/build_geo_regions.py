# src/build_geo_regions.py
# Usage:
#   python src/build_geo_regions.py
#
# Output:
#   data/processed/lan_regions.geojson
#   data/processed/lan_regions_simplified.geojson  (optional, smaller)

import sys
from pathlib import Path
import geopandas as gpd

# ---- CONFIG -----------------------------------------------------------------
# Folder where you unzipped SCB "LanSweref99TM" (the .shp/.dbf/.shx/.prj live here)
INPUT_DIR = Path("data/raw/LanSweref99TM")
# Output folder
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Simplification tolerance in degrees (0 = skip). ~0.002 is a good start for Sweden
SIMPLIFY_TOLERANCE = 0.002

# ---- HELPERS ----------------------------------------------------------------
def detect_column(candidates, columns):
    cols_low = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in cols_low:
            return cols_low[cand.lower()]
    # fallback: try contains
    for c in columns:
        if any(k.lower() in c.lower() for k in candidates):
            return c
    return None

def find_shp_file(folder: Path):
    shp_files = list(folder.glob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp found in {folder}")
    if len(shp_files) > 1:
        # Prefer something with "lan" in the name if multiple exist
        for p in shp_files:
            if "lan" in p.name.lower():
                return p
        return shp_files[0]
    return shp_files[0]

# ---- MAIN -------------------------------------------------------------------
def main():
    shp_path = find_shp_file(INPUT_DIR)
    print(f"Reading shapefile: {shp_path}")

    gdf = gpd.read_file(shp_path)
    print("Original CRS:", gdf.crs)

    # Reproject to WGS84 for web maps
    gdf = gdf.to_crs(epsg=4326)

    # Try to detect code & name cols from common SCB schemas
    code_col = detect_column(
        ["LANSKOD", "LAN_KOD", "KOD", "SCB_LANSKOD", "LAN_ID"],
        gdf.columns,
    )
    name_col = detect_column(
        ["LAN_NAMN", "LAN", "LAN_NAMN2", "NAMN", "LANNAMN"],
        gdf.columns,
    )

    if code_col is None:
        raise RuntimeError(
            f"Could not detect län code column. Columns available: {list(gdf.columns)}"
        )
    if name_col is None:
        raise RuntimeError(
            f"Could not detect län name column. Columns available: {list(gdf.columns)}"
        )

    print(f"Detected columns -> code: {code_col} | name: {name_col}")

    # Normalize properties we want to ship
    gdf["scb_lan_code_2"] = gdf[code_col].astype(str).str.zfill(2)
    gdf["region_name_official"] = gdf[name_col].astype(str)

    # Keep only what we need in the properties (smaller file)
    keep_cols = ["scb_lan_code_2", "region_name_official", "geometry"]
    gdf_out = gdf[keep_cols].copy()

    # Basic validation: expect 21 regions, codes 01..25 excluding gaps
    n = len(gdf_out)
    uniq = gdf_out["scb_lan_code_2"].nunique()
    print(f"Features: {n} | unique codes: {uniq}")
    if uniq < 21:
        print("⚠️  Warning: fewer than 21 unique län codes detected. Check the input data.")

    # Save full-resolution GeoJSON
    out_full = OUTPUT_DIR / "lan_regions.geojson"
    gdf_out.to_file(out_full, driver="GeoJSON")
    print(f"✅ Saved GeoJSON → {out_full}")

    # Optional: simplified version for dashboards (much smaller)
    if SIMPLIFY_TOLERANCE and SIMPLIFY_TOLERANCE > 0:
        # preserve topology: simplify on a projected CRS to avoid artifacts, then reproject back
        gdf_proj = gdf_out.to_crs(epsg=3857)
        gdf_proj["geometry"] = gdf_proj.geometry.simplify(
            tolerance=SIMPLIFY_TOLERANCE * 111_000,  # deg → meters approx
            preserve_topology=True
        )
        gdf_simplified = gdf_proj.to_crs(epsg=4326)
        out_simpl = OUTPUT_DIR / "lan_regions_simplified.geojson"
        gdf_simplified.to_file(out_simpl, driver="GeoJSON")
        print(f"✅ Saved simplified GeoJSON → {out_simpl} (tolerance={SIMPLIFY_TOLERANCE})")

    print("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Error:", e)
        sys.exit(1)
