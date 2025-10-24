import json, time, requests
from pathlib import Path
from datetime import datetime

BASE = "https://api.kolada.se/v3"
HDRS = {"Accept":"application/json","User-Agent":"HealthcareAccessProject/1.0"}

REGIONS = [
  "0001","0003","0004","0005","0006","0007","0008","0009","0010","0012","0013",
  "0014","0017","0018","0019","0020","0021","0022","0023","0024","0025"
]

OUT_DIR = Path("data/raw"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_JSON = OUT_DIR/"kolada_vardgaranti.json"
OUT_META = OUT_DIR/"kolada_vardgaranti.meta.json"

S = requests.Session(); S.headers.update(HDRS)

def paged(url, params=None):
    out=[]; page=1
    while True:
        q=dict(params or {}); q["page"]=page
        r=S.get(url, params=q, timeout=60)
        r.raise_for_status()
        j=r.json()
        vals=j.get("values",[])
        out.extend(vals)
        if len(vals)<100: break
        page += 1
    return out

def pick_kpi(family_terms):
    # find a single KPI id whose title best matches the family
    cand=[]
    for term in family_terms:
        cand += paged(f"{BASE}/kpi", {"title": term})
    # keep percent-like
    cand = [k for k in cand if "%" in (k.get("unit","")).lower() or "andel" in (k.get("title","")).lower()]
    if not cand: return None
    # simple scoring by keywords
    def score(k, keys): 
        tl=(k.get("title") or "").lower()
        return sum(1 for kw in keys if kw in tl), -len(tl)
    fam_keys = set(" ".join(family_terms).lower().split())
    return sorted(cand, key=lambda k: score(k, fam_keys), reverse=True)[0]["id"]

def discover_kpis():
    return {
      "3d_assessment":   pick_kpi(["vårdgaranti 3","bedömning 3","3 dagar"]),
      "90d_first_visit": pick_kpi(["första besök 90","besök 90"]),
      "90d_treatment":   pick_kpi(["åtgärd 90","operation 90"])
    }

def fetch_series(ou_id, kpi_id):
    r = S.get(f"{BASE}/oudata", params={"kpi": kpi_id, "ou": ou_id}, timeout=90)
    if r.ok and "application/json" in r.headers.get("Content-Type",""):
        j=r.json()
        if "values" in j: return j["values"]
    # fallback path
    r = S.get(f"{BASE}/oudata/kpi/{kpi_id}/ou/{ou_id}", timeout=90)
    if r.ok:
        j=r.json()
        if "values" in j: return j["values"]
    return []

def main():
    kpis = discover_kpis()
    print("KPI IDs:", kpis)
    kpi_ids = [v for v in kpis.values() if v]

    combined = {
        "source":"Kolada v3",
        "fetched_at": datetime.now().astimezone().isoformat(),
        "regions": [{"id": r} for r in REGIONS],
        "kpis": kpis,
        "data": []
    }

    for kid in kpi_ids:
        for ou in REGIONS:
            vals = fetch_series(ou, kid)
            if vals:
                combined["data"].append({"kpi_id": kid, "region_id": ou, "values": vals})
            time.sleep(0.12)

    OUT_JSON.write_text(json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_META.write_text(json.dumps({
        "region_count": len(REGIONS),
        "kpi_count": len(kpi_ids),
        "series_count": len(combined["data"]),
        "fetched_at": combined["fetched_at"]
    }, indent=2), encoding="utf-8")
    print(f"✅ Saved {OUT_JSON} (series: {len(combined['data'])})")

if __name__ == "__main__":
    main()
