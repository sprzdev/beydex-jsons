#!/usr/bin/env python3
"""
insert_data.py
Lee los JSONs de piezas y los inserta en las tablas de Supabase.
Las URLs de imagen se reescriben automáticamente a las del bucket de Supabase.

Uso:
  pip install supabase
  python insert_data.py
"""

import json
from pathlib import Path
from supabase import create_client, Client

# ─────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────
SUPABASE_URL = "https://mdvauewlafpdmmqccqjg.supabase.co"
SUPABASE_KEY = "sb_publishable_OLrLWV2mYYOpRK7tmyLHiw_wrSBESF3"
BUCKET       = "beyblade-assets"
DATA_DIR     = Path(__file__).parent / "data"

# ─────────────────────────────────────────
# HELPER: construir URL de Supabase Storage
# a partir de la URL original de bey-library
# ─────────────────────────────────────────
def supabase_image_url(old_url: str, folder: str, item_id: str, variant_id: str) -> str | None:
    if not old_url:
        return None
    from pathlib import PurePosixPath
    ext = PurePosixPath(old_url.split("?")[0]).suffix or ".webp"
    filename     = f"{variant_id}{ext}"
    storage_path = f"{folder}/{item_id}/{filename}"
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{storage_path}"


# ─────────────────────────────────────────
# INSERCIÓN: BLADES
# ─────────────────────────────────────────
def insert_blades(supabase: Client):
    print("\n📂 Insertando blades...")
    with open(DATA_DIR / "blades.json", encoding="utf-8") as f:
        blades = json.load(f)

    blade_rows   = []
    variant_rows = []

    for b in blades:
        blade_rows.append({
            "id":                   b["id"],
            "name":                 b["name"],
            "hasbro_name":          b.get("hasbro_name") or None,
            "line":                 b["line"],
            "weight":               b.get("weight"),
            "spin_direction":       b.get("spin_direction"),
            "bey_type":             b.get("bey_type"),
            "is_collaboration":     b.get("is_collaboration", False),
            "collaboration_series": b.get("collaboration_series"),
            "gimmick":              b.get("gimmick"),
            "stock_combo":          b.get("stock_combo"),
            "description":          b.get("description", ""),
            "is_infinity":          b.get("is_infinity", False),
            "has_integrated_ratchet": b.get("has_integrated_ratchet", False),
        })
        for v in b.get("variants", []):
            variant_rows.append({
                "variant_id":   v["variant_id"],
                "blade_id":     b["id"],
                "release_code": v.get("release_code", ""),
                "name":         v["name"],
                "image":        supabase_image_url(v.get("image",""), "blades", b["id"], v["variant_id"]),
                "color":        v.get("color"),
            })

    _upsert(supabase, "blades",         blade_rows,   "blades")
    _upsert(supabase, "blade_variants", variant_rows, "blade_variants")


# ─────────────────────────────────────────
# INSERCIÓN: RATCHETS
# ─────────────────────────────────────────
def insert_ratchets(supabase: Client):
    print("\n📂 Insertando ratchets...")
    with open(DATA_DIR / "ratchets.json", encoding="utf-8") as f:
        ratchets = json.load(f)

    ratchet_rows = []
    variant_rows = []

    for r in ratchets:
        ratchet_rows.append({
            "id":          r["id"],
            "name":        r["name"],
            "sides":       r.get("sides"),
            "height":      r.get("height"),
            "weight":      r.get("weight"),
            "description": r.get("description", ""),
        })
        for v in r.get("variants", []):
            variant_rows.append({
                "variant_id":   v["variant_id"],
                "ratchet_id":   r["id"],
                "release_code": v.get("release_code", ""),
                "name":         v["name"],
                "image":        supabase_image_url(v.get("image",""), "ratchets", r["id"], v["variant_id"]),
                "color":        v.get("color"),
            })

    _upsert(supabase, "ratchets",         ratchet_rows, "ratchets")
    _upsert(supabase, "ratchet_variants", variant_rows, "ratchet_variants")


# ─────────────────────────────────────────
# INSERCIÓN: BITS
# ─────────────────────────────────────────
def insert_bits(supabase: Client):
    print("\n📂 Insertando bits...")
    with open(DATA_DIR / "bits.json", encoding="utf-8") as f:
        bits = json.load(f)

    bit_rows     = []
    variant_rows = []

    for b in bits:
        bit_rows.append({
            "id":                   b["id"],
            "name":                 b["name"],
            "full_name":            b.get("full_name"),
            "abbreviation":         b.get("abbreviation"),
            "bey_type":             b.get("bey_type"),
            "weight":               b.get("weight"),
            "burst_resistance_type": b.get("burst_resistance_type"),
            "gimmick":              b.get("gimmick") or None,
            "description":          b.get("description", ""),
        })
        for v in b.get("variants", []):
            variant_rows.append({
                "variant_id":   v["variant_id"],
                "bit_id":       b["id"],
                "release_code": v.get("release_code", ""),
                "name":         v["name"],
                "image":        supabase_image_url(v.get("image",""), "bits", b["id"], v["variant_id"]),
                "color":        v.get("color"),
            })

    _upsert(supabase, "bits",         bit_rows,     "bits")
    _upsert(supabase, "bit_variants", variant_rows, "bit_variants")


# ─────────────────────────────────────────
# INSERCIÓN: ASSIST BLADES
# ─────────────────────────────────────────
def insert_assist_blades(supabase: Client):
    print("\n📂 Insertando assist_blades...")
    with open(DATA_DIR / "assistBlades.json", encoding="utf-8") as f:
        items = json.load(f)

    rows         = []
    variant_rows = []

    for item in items:
        rows.append({
            "id":            item["id"],
            "name":          item["name"],
            "weight":        item.get("weight"),
            "high_level":    item.get("high_level"),
            "bey_type":      item.get("bey_type"),
            "spin_direction": item.get("spin_direction"),
            "description":   item.get("description", ""),
        })
        for v in item.get("variants", []):
            variant_rows.append({
                "variant_id":      v["variant_id"],
                "assist_blade_id": item["id"],
                "release_code":    v.get("release_code", ""),
                "name":            v["name"],
                "image":           supabase_image_url(v.get("image",""), "assist-blades", item["id"], v["variant_id"]),
                "color":           v.get("color"),
            })

    _upsert(supabase, "assist_blades",         rows,         "assist_blades")
    _upsert(supabase, "assist_blade_variants", variant_rows, "assist_blade_variants")


# ─────────────────────────────────────────
# INSERCIÓN: OVER BLADES
# ─────────────────────────────────────────
def insert_over_blades(supabase: Client):
    print("\n📂 Insertando over_blades...")
    with open(DATA_DIR / "overBlades.json", encoding="utf-8") as f:
        items = json.load(f)

    rows         = []
    variant_rows = []

    for item in items:
        rows.append({
            "id":            item["id"],
            "name":          item["name"],
            "weight":        item.get("weight"),
            "bey_type":      item.get("bey_type"),
            "spin_direction": item.get("spin_direction"),
            "description":   item.get("description", ""),
        })
        for v in item.get("variants", []):
            variant_rows.append({
                "variant_id":    v["variant_id"],
                "over_blade_id": item["id"],
                "release_code":  v.get("release_code", ""),
                "name":          v["name"],
                "image":         supabase_image_url(v.get("image",""), "over-blades", item["id"], v["variant_id"]),
                "color":         v.get("color"),
            })

    _upsert(supabase, "over_blades",         rows,         "over_blades")
    _upsert(supabase, "over_blade_variants", variant_rows, "over_blade_variants")


# ─────────────────────────────────────────
# HELPER: upsert en lotes de 500 filas
# ─────────────────────────────────────────
def _upsert(supabase: Client, table: str, rows: list, label: str):
    if not rows:
        print(f"  ⚠️  {label}: sin datos")
        return

    batch_size = 500
    total = len(rows)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table(table).upsert(batch).execute()
            inserted += len(batch)
            print(f"  ✅ {label}: {inserted}/{total} filas")
        except Exception as e:
            print(f"  ❌ {label} lote {i}-{i+batch_size}: {e}")

    print(f"  ✔  {label} completado: {inserted} filas totales")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    print("🚀 BeyDex Data Inserter")
    print(f"   Proyecto: {SUPABASE_URL}")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    insert_blades(supabase)
    insert_ratchets(supabase)
    insert_bits(supabase)
    insert_assist_blades(supabase)
    insert_over_blades(supabase)

    print(f"\n{'═'*60}")
    print("✅ Todos los datos insertados en Supabase")
    print(f"{'═'*60}")


if __name__ == "__main__":
    main()
