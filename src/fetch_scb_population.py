# fetch_scb_population.py  (defensive version)
import json, requests, sys
from pathlib import Path
from datetime import datetime

# Candidate endpoints for the same table (en vs sv)
CANDIDATE_URLS = [
    "https://api.scb.se/OV0104/v1/doris/en/ssd/BE/BE0101/BE0101A/FolkmangdKon",
    "https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101/BE0101A/FolkmangdKon",
    # if needed, try the “BefolkningNy” family too:
    "https://api.scb.se/OV0104/v1/doris/en/ssd/BE/BE0101/BE0101A/BefolkningNy",
    "https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101/BE0101A/BefolkningNy",
]

REGIONS = ["01","03","04","05","06","07","08","09","10","12","13","14",
           "17","18","19","20","21","22","23","24","25"]
YEARS   = ["2020","2021","2022","2023","2024","2025"]

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (SCB test script)"
}

def get_json(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    print(f"GET {url} -> {r.status_code} {r.headers.get('Content-Type')}")
    if not r.ok:
        print(r.text[:400])
        return None
    # Ensure JSON
    if "application/json" not in (r.headers.get("Content-Type") or ""):
        print("Not JSON, first 400 chars:\n", r.text[:400])
        return None
    try:
        return r.json()
    except Exception as e:
        print("Failed to parse JSON:", e, "\nFirst 400 chars:\n", r.text[:400])
        return None

def first_working_metadata():
    for url in CANDIDATE_URLS:
        meta = get_json(url)
        if meta:
            return url, meta
    print("No working SCB endpoint returned JSON. Check network/proxy and URLs.")
    sys.exit(1)

def find_var(meta, code_name):
    for v in meta["variables"]:
        if v["code"].lower() == code_name.lower():
            return v
    return None

def main():
    url, meta = first_working_metadata()

    v_region   = find_var(meta, "Region")
    v_time     = find_var(meta, "Tid") or find_var(meta, "time")
    v_contents = find_var(meta, "ContentsCode") or find_var(meta, "Contents")

    if not v_region or not v_time:
        print("Could not find Region/Tid variables")
        sys.exit(1)

    # filter to existing values
    regions_ok = [r for r in REGIONS if r in set(v_region["values"])] or v_region["values"]
    years_ok   = [y for y in YEARS if y in set(v_time["values"])]     or v_time["values"][-6:]

    query = [
        {"code": v_region["code"], "selection": {"filter": "item", "values": regions_ok}},
        {"code": v_time["code"],   "selection": {"filter": "item", "values": years_ok}},
    ]
    if v_contents:  # some tables require it
        query.insert(0, {"code": v_contents["code"],
                         "selection": {"filter": "item", "values": [v_contents["values"][0]]}})

    payload = {"query": query, "response": {"format": "JSON"}}

    print("POST", url, "with", json.dumps(payload)[:200], "...")
    rp = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    print("POST status:", rp.status_code, rp.headers.get("Content-Type"))
    if not rp.ok:
        print(rp.text[:800])
        sys.exit(1)
    if "application/json" not in (rp.headers.get("Content-Type") or ""):
        print("POST did not return JSON. Body:\n", rp.text[:800])
        sys.exit(1)

    data = rp.json()
    rows = len(data.get("data", []))
    print(f"✅ Rows received: {rows}")

    out_dir = Path("data/raw"); out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "scb_population.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    meta_out = out_dir / "scb_population.meta.json"
    with open(meta_out, "w", encoding="utf-8") as f:
        json.dump({
            "endpoint_used": url,
            "regions_used": regions_ok,
            "years_used": years_ok,
            "retrieved_at": datetime.utcnow().isoformat()+"Z"
        }, f, indent=2)

    print("Saved →", out_json)

if __name__ == "__main__":
    main()
