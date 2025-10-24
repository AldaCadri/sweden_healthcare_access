import pandas as pd
import os
import re

IN_PATH = "Statistikdatabasen_2025-10-24 14_45_40.xlsx"   
SHEET = 0                                                 

MONTHS = {
    "jan":"01","januari":"01","feb":"02","februari":"02","mar":"03","mars":"03",
    "apr":"04","april":"04","maj":"05","jun":"06","juni":"06","jul":"07","juli":"07",
    "aug":"08","augusti":"08","sep":"09","sept":"09","september":"09",
    "okt":"10","oktober":"10","nov":"11","november":"11","dec":"12","december":"12"
}

def safe_pad_cols(mi, target_levels=3):
    tuples=[]
    for c in mi:
        if isinstance(c, tuple):
            t = tuple("" if (x is None or (isinstance(x,float) and pd.isna(x))) else str(x) for x in c)
        else:
            t = (str(c),)
        if len(t)<target_levels:
            t = t + tuple([""]*(target_levels-len(t)))
        tuples.append(t[:target_levels])
    return pd.MultiIndex.from_tuples(tuples)

def tidy_file(path, sheet=0):
    # read with multirow headers (year/month/metric)
    df = pd.read_excel(path, sheet_name=sheet, header=[0,1,2])
    # drop empty rows/cols
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # detect leading ID columns (they usually have "Unnamed" in top header level)
    top = df.columns.get_level_values(0)
    id_end=0
    for i,t in enumerate(top):
        if isinstance(t, str) and t.startswith("Unnamed"):
            id_end = i+1
        else:
            break
    if id_end == 0:
        # fallback: assume Region, Kön, Ålder, Diagnosgrupp are first 4 cols
        id_end = min(4, df.shape[1])

    id_cols = df.columns[:id_end]
    val_cols = df.columns[id_end:]

    # rename ID columns using last non-empty label across levels
    id_df = df.iloc[:, :id_end].copy()
    new_id_names=[]
    for col in id_cols:
        if isinstance(col, tuple):
            cand=[x for x in col if isinstance(x,str) and x and not x.startswith("Unnamed")]
            new_id_names.append(cand[-1] if cand else "ID")
        else:
            new_id_names.append(str(col))
    id_df.columns = new_id_names

    # pad value columns to exactly 3 levels: Year, Month, Metric
    val_cols_padded = safe_pad_cols(val_cols, 3)
    val_df = pd.DataFrame(df.iloc[:, id_end:].to_numpy(), columns=val_cols_padded, index=df.index)

    # stack to long
    long = (
        val_df
        .stack(level=[0,1,2])
        .rename_axis(index=["row","Year","Month","Metric"])
        .reset_index(name="Value")
    )

    # join IDs back
    id_df = id_df.reset_index().rename(columns={"index":"row"})
    merged = long.merge(id_df, on="row", how="left").drop(columns=["row"])

    # build Period YYYY-MM
    merged["Year"] = merged["Year"].astype(str).str.extract(r"(\d{4})", expand=False)
    mm = merged["Month"].astype(str).str.strip().str.lower().str.replace(".","",regex=False)
    mm = mm.map(MONTHS).fillna(merged["Month"].astype(str).str.extract(r"(\d{2})", expand=False))
    merged["Period"] = pd.to_datetime(merged["Year"] + "-" + mm, errors="coerce", format="%Y-%m")

    # standardize common Swedish ID names if present
    rename_map = {
        "Region/landsting/akutmottagning": "Region",
        "Region/landsting": "Region",
        "Kön": "Kon",
        "Ålder": "Alder",
        "Åldersgrupp": "Aldersgrupp",
        "Diagnosgrupp": "Diagnosgrupp"
    }
    for k,v in rename_map.items():
        if k in merged.columns:
            merged.rename(columns={k:v}, inplace=True)

    merged = merged.dropna(subset=["Period"]).dropna(how="all", subset=["Value"])

    # build WIDE per period with metrics as columns
    id_cols_final = [c for c in ["Region","Kon","Alder","Aldersgrupp","Diagnosgrupp"] if c in merged.columns]
    if not id_cols_final:
        id_cols_final = new_id_names

    wide = merged.pivot_table(index=[*id_cols_final,"Period"], columns="Metric", values="Value", aggfunc="first").reset_index()
    wide.columns = [c if isinstance(c,str) else " ".join([str(x) for x in c if x]) for c in wide.columns]

    base = os.path.splitext(os.path.basename(path))[0]
    out_long = f"tidy_long_{base}.csv"
    out_wide = f"tidy_wide_{base}.csv"
    merged.to_csv(out_long, index=False)
    wide.to_csv(out_wide, index=False)
    print(f"Saved: {out_long} ({merged.shape[0]} rows) and {out_wide} ({wide.shape[0]} rows)")

if __name__ == "__main__":
    tidy_file(IN_PATH, SHEET)
