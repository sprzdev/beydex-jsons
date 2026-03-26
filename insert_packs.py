#!/usr/bin/env python3
"""
insert_packs.py
Lee scraped_packs.json y lo sube a Supabase.
Usa la SERVICE KEY para saltarse RLS en escritura.

Uso:
  python3 insert_packs.py

IMPORTANTE: Necesitas la service_role key (NO la publishable key).
Encuéntrala en: Supabase Dashboard → Settings → API → service_role key
"""

import json
import os
from pathlib import Path
from supabase import create_client, Client

# ─────────────────────────────────────────
# CONFIGURACIÓN
# La service key permite escritura aunque RLS esté activo.
# NUNCA la pongas en el código de la app iOS — solo en scripts de admin.
# ─────────────────────────────────────────
SUPABASE_URL     = "https://mdvauewlafpdmmqccqjg.supabase.co"
# Pon aquí tu service_role key cuando la tengas
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

INPUT = Path(__file__).parent / "scraped_packs.json"


def _upsert(supabase: Client, table: str, rows: list, label: str):
    if not rows:
        print(f"  ⚠️  {label}: sin datos")
        return
    batch_size = 200
    total = len(rows)
    inserted = 0
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table(table).upsert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            print(f"  ❌ {label} lote {i}: {e}")
            return
    print(f"  ✅ {label}: {inserted} filas")


def main():
    if not SUPABASE_SERVICE_KEY:
        print("❌ Falta SUPABASE_SERVICE_KEY.")
        print("   Ejecútalo así:")
        print("   SUPABASE_SERVICE_KEY='tu_key' python3 insert_packs.py")
        return

    print("🚀 BeyDeck Pack Inserter")
    print(f"   Proyecto: {SUPABASE_URL}")

    with open(INPUT, encoding="utf-8") as f:
        data = json.load(f)

    meta = data["meta"]
    print(f"\n📦 Datos a insertar:")
    print(f"   Packs:      {meta['total_packs']}")
    print(f"   Pack parts: {meta['total_pack_parts']}")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    print("\n📤 Insertando...")
    _upsert(supabase, "packs",               data["packs"],               "packs")
    _upsert(supabase, "pack_parts",          data["pack_parts"],          "pack_parts")
    _upsert(supabase, "pack_beys",           data["pack_beys"],           "pack_beys")
    _upsert(supabase, "pack_affiliate_links",data["pack_affiliate_links"],"pack_affiliate_links")

    print(f"\n{'='*50}")
    print("✅ Packs insertados en Supabase")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
