#!/usr/bin/env python3
"""
upload_images.py
Lee los JSONs de piezas, descarga cada imagen de bey-library.vercel.app
y la sube al bucket beyblade-assets de Supabase.

Uso:
  pip install supabase requests
  python upload_images.py
"""

import json
import time
import os
from urllib.parse import quote
from pathlib import Path
import requests
from supabase import create_client, Client

# ─────────────────────────────────────────
# CONFIGURACIÓN — rellena estas dos líneas
# ─────────────────────────────────────────
SUPABASE_URL = "https://mdvauewlafpdmmqccqjg.supabase.co"
SUPABASE_KEY = "sb_publishable_OLrLWV2mYYOpRK7tmyLHiw_wrSBESF3"
BUCKET       = "beyblade-assets"
DATA_DIR     = Path(__file__).parent / "data"

# ─────────────────────────────────────────
# Archivos JSON y su carpeta destino en Storage
# ─────────────────────────────────────────
JSON_MAP = {
    "blades.json":       "blades",
    "ratchets.json":     "ratchets",
    "bits.json":         "bits",
    "assistBlades.json": "assist-blades",
    "overBlades.json":   "over-blades",
}

# ─────────────────────────────────────────────────────────────────────────────

def download_image(url: str) -> bytes | None:
    """Descarga una imagen dada su URL. Devuelve bytes o None si falla."""
    if not url:
        return None
    try:
        # Las URLs tienen espacios — hay que codificarlos
        encoded = quote(url, safe=":/?=&#%")
        response = requests.get(encoded, timeout=15)
        if response.status_code == 200:
            return response.content
        else:
            print(f"    ⚠️  HTTP {response.status_code} — {url}")
            return None
    except Exception as e:
        print(f"    ❌ Error descargando {url}: {e}")
        return None


def upload_to_supabase(supabase: Client, storage_path: str, image_bytes: bytes) -> str | None:
    """Sube imagen a Supabase Storage. Devuelve la URL pública o None si falla."""
    try:
        supabase.storage.from_(BUCKET).upload(
            path=storage_path,
            file=image_bytes,
            file_options={
                "content-type": "image/webp",
                "upsert": "true"   # sobrescribe si ya existe
            }
        )
        public_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{storage_path}"
        return public_url
    except Exception as e:
        print(f"    ❌ Error subiendo {storage_path}: {e}")
        return None


def process_file(supabase: Client, json_filename: str, folder: str):
    """Procesa un archivo JSON completo: descarga y sube cada imagen de variante."""
    json_path = DATA_DIR / json_filename
    if not json_path.exists():
        print(f"\n⚠️  No encontrado: {json_path}")
        return

    with open(json_path, encoding="utf-8") as f:
        items = json.load(f)

    print(f"\n{'─'*60}")
    print(f"📂 {json_filename} — {len(items)} piezas")
    print(f"{'─'*60}")

    ok = 0
    skip = 0
    errors = 0

    for item in items:
        item_id  = item.get("id", "unknown")
        variants = item.get("variants", [])

        for variant in variants:
            variant_id = variant.get("variant_id", "unknown")
            old_url    = variant.get("image", "")

            if not old_url:
                skip += 1
                continue

            # Nombre del archivo: variant_id + extensión original
            ext = Path(old_url.split("?")[0]).suffix or ".webp"
            filename     = f"{variant_id}{ext}"
            storage_path = f"{folder}/{item_id}/{filename}"

            print(f"  ↓ {variant_id}", end="  ", flush=True)

            # Descargar
            image_bytes = download_image(old_url)
            if image_bytes is None:
                errors += 1
                continue

            # Subir
            new_url = upload_to_supabase(supabase, storage_path, image_bytes)
            if new_url:
                print(f"✅ ({len(image_bytes)//1024}KB)")
                ok += 1
            else:
                errors += 1

            # Pausa breve para no saturar la API
            time.sleep(0.1)

    print(f"\n  Resultado: {ok} subidas · {skip} sin imagen · {errors} errores")


def main():
    print("🚀 BeyDex Image Uploader")
    print(f"   Bucket: {BUCKET}")
    print(f"   Proyecto: {SUPABASE_URL}")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    for json_filename, folder in JSON_MAP.items():
        process_file(supabase, json_filename, folder)

    print(f"\n{'═'*60}")
    print("✅ Proceso completado")
    print(f"{'═'*60}")


if __name__ == "__main__":
    main()
