#!/usr/bin/env python3
"""
scrape_pack_images.py  v2
─────────────────────────────────────────────────────────────────────────────
Fuentes de imágenes:
  1. phstudy.org/data/products_multilang.json
     → images/products/detail_{product_id}.png   (alta res, box art oficial)
     → cobertura: 84 packs Takara Tomy

  2. Hasbro (Amazon.com placeholder por ahora): 95 packs sin fuente automática.
     → Se asigna box_placeholder.webp como fallback

REGLA DE LA CAJA:
  Si un pack no tiene imagen disponible o la descarga falla,
  el campo `image` en Supabase apunta a:
  beyblade-assets/packs/box_placeholder.webp
  Ningún pack puede quedarse sin imagen.

Salida:
  images_output/packs/pack-bx01.webp   ← imagen real convertida
  images_output/packs/box_placeholder.webp  ← caja genérica (se crea una vez)
  pack_images_map.json  ← mapeo para upload_pack_images.py

Uso:
  pip install supabase requests Pillow
  python3 scrape_pack_images.py
"""

import json
import os
import re
import time
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont
from supabase import create_client

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://mdvauewlafpdmmqccqjg.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_OLrLWV2mYYOpRK7tmyLHiw_wrSBESF3")
BUCKET       = "beyblade-assets"

PHSTUDY_BASE     = "https://beyblade.phstudy.org"
PHSTUDY_PRODUCTS = f"{PHSTUDY_BASE}/data/products_multilang.json"

OUTPUT_DIR   = Path(__file__).parent / "images_output" / "packs"
PLACEHOLDER  = OUTPUT_DIR / "box_placeholder.webp"
TARGET_SIZE  = (600, 600)
WEBP_QUALITY = 85

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": PHSTUDY_BASE,
}

# ─────────────────────────────────────────────────────────────────────────────
# PASO 0: Crear imagen placeholder (caja genérica)
# Se genera una sola vez con Pillow — fondo gris con texto BEYBLADE X
# ─────────────────────────────────────────────────────────────────────────────

def create_placeholder():
    if PLACEHOLDER.exists():
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", TARGET_SIZE, color=(40, 40, 50))
    draw = ImageDraw.Draw(img)
    # Rectángulo decorativo
    draw.rounded_rectangle([40, 40, 560, 560], radius=30,
                           outline=(80, 130, 200), width=4)
    draw.rounded_rectangle([60, 60, 540, 540], radius=20,
                           outline=(60, 100, 160), width=2)
    # Texto central
    cx, cy = TARGET_SIZE[0] // 2, TARGET_SIZE[1] // 2
    draw.text((cx, cy - 40), "BEYBLADE X", fill=(200, 220, 255),
              anchor="mm", font=None)
    draw.text((cx, cy + 10), "No image available", fill=(120, 140, 160),
              anchor="mm", font=None)
    img.save(PLACEHOLDER, "WEBP", quality=WEBP_QUALITY)
    print(f"  ✅ Placeholder creado: {PLACEHOLDER.name}")


# ─────────────────────────────────────────────────────────────────────────────
# PASO 1: Cargar packs desde Supabase
# ─────────────────────────────────────────────────────────────────────────────

def fetch_packs() -> list[dict]:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    result = (supabase.table("packs")
              .select("id, release_code, name, manufacturer")
              .execute())
    packs = result.data or []
    print(f"✅ {len(packs)} packs leídos de Supabase")
    return packs


# ─────────────────────────────────────────────────────────────────────────────
# PASO 2: Cargar catálogo de imágenes de phstudy
# Devuelve dict: product_id (ej. "BX01") → lista de URLs de imagen
# ─────────────────────────────────────────────────────────────────────────────

def fetch_phstudy_catalog() -> dict[str, list[str]]:
    """
    Descarga products_multilang.json de phstudy y construye un índice
    { "BX01": ["https://...detail_bx01.png", ...], "BX02": [...], ... }
    """
    print(f"\n📡 Descargando catálogo de phstudy.org...")
    resp = requests.get(PHSTUDY_PRODUCTS, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    raw = resp.json()   # es un objeto {0: {...}, 1: {...}, ...}

    catalog: dict[str, list[str]] = {}
    for entry in raw.values():
        pid = entry.get("product_id", "")   # "BX01", "BX02", "UX01", etc.
        images = entry.get("images", [])
        if pid and images:
            # Priorizar: detail_ > @1 (alta res) > _list (thumbnail)
            full_urls = [f"{PHSTUDY_BASE}/{img}" for img in images]
            # Ordenar: detail primero
            detail = [u for u in full_urls if "detail_" in u]
            hires  = [u for u in full_urls if "@1" in u and "detail_" not in u]
            rest   = [u for u in full_urls if u not in detail and u not in hires]
            catalog[pid] = detail + hires + rest

    print(f"✅ {len(catalog)} productos en catálogo phstudy")
    return catalog


# ─────────────────────────────────────────────────────────────────────────────
# PASO 3: Mapear release_code → product_id de phstudy
# BX-01 → "BX01", UX-01 → "UX01", CX-01 → "CX01"
# ─────────────────────────────────────────────────────────────────────────────

def release_code_to_product_id(release_code: str) -> str | None:
    """
    Convierte 'BX-01' → 'BX01', 'UX-12' → 'UX12', etc.
    Hasbro codes (F9580, G0184) no tienen equivalente en phstudy → None
    """
    m = re.match(r'^(BX|UX|CX)-(\d+)$', release_code.strip().upper())
    if m:
        prefix = m.group(1)
        num    = int(m.group(2))         # elimina ceros iniciales
        return f"{prefix}{num:02d}"      # BX01, BX14, UX01...
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PASO 4: Descargar imagen y convertir a WebP
# ─────────────────────────────────────────────────────────────────────────────

def download_and_convert(image_url: str, output_path: Path) -> bool:
    try:
        resp = requests.get(image_url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return False

        img = Image.open(BytesIO(resp.content)).convert("RGBA")

        # Redimensionar manteniendo ratio si la imagen es muy grande
        if max(img.size) > 1200:
            img.thumbnail((1200, 1200), Image.LANCZOS)

        # Rellenar hasta TARGET_SIZE con fondo blanco (para imágenes con transparencia)
        if img.width < TARGET_SIZE[0] or img.height < TARGET_SIZE[1]:
            canvas = Image.new("RGBA", TARGET_SIZE, (255, 255, 255, 0))
            offset = ((TARGET_SIZE[0] - img.width) // 2,
                      (TARGET_SIZE[1] - img.height) // 2)
            canvas.paste(img, offset)
            img = canvas

        # Convertir RGBA → RGB para WebP
        rgb = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "RGBA":
            rgb.paste(img, mask=img.split()[3])
        else:
            rgb = img.convert("RGB")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        rgb.save(output_path, "WEBP", quality=WEBP_QUALITY, method=6)
        size_kb = output_path.stat().st_size // 1024
        print(f"    ✅ {output_path.name} ({size_kb}KB)")
        return True

    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# PASO 5: Regla de la Caja — asignar placeholder si no hay imagen
# ─────────────────────────────────────────────────────────────────────────────

PLACEHOLDER_STORAGE_URL = (
    f"https://mdvauewlafpdmmqccqjg.supabase.co"
    f"/storage/v1/object/public/{BUCKET}/packs/box_placeholder.webp"
)

def fallback_entry(pack: dict, reason: str) -> dict:
    """
    Devuelve un entry con status 'placeholder' apuntando a la caja genérica.
    Garantiza que ningún pack se quede sin imagen en Supabase.
    """
    return {
        "pack_id":        pack["id"],
        "release_code":   pack["release_code"],
        "manufacturer":   pack["manufacturer"],
        "local_path":     str(PLACEHOLDER),
        "storage_path":   "packs/box_placeholder.webp",
        "supabase_url":   PLACEHOLDER_STORAGE_URL,
        "status":         "placeholder",
        "reason":         reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("🚀 BeyDeck Pack Image Scraper v2")
    print("=" * 60)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 0. Crear placeholder
    create_placeholder()

    # 1. Leer packs de Supabase
    packs = fetch_packs()

    # 2. Cargar catálogo de phstudy
    catalog = fetch_phstudy_catalog()

    results  = []
    ok       = 0
    skipped  = 0
    placeholder_count = 0

    for i, pack in enumerate(packs):
        pack_id      = pack["id"]           # pack-bx01
        release_code = pack["release_code"] # BX-01
        manufacturer = pack["manufacturer"] # takara_tomy | hasbro

        filename    = f"{pack_id}.webp"
        output_path = OUTPUT_DIR / filename

        print(f"\n[{i+1:>3}/{len(packs)}] {release_code} — {pack['name'][:45]}")

        # ── Ya existe localmente ────────────────────────────────────────
        if output_path.exists():
            print(f"    ⏭  Ya existe, skipping")
            skipped += 1
            results.append({
                "pack_id":      pack_id,
                "release_code": release_code,
                "manufacturer": manufacturer,
                "local_path":   str(output_path),
                "storage_path": f"packs/{filename}",
                "supabase_url": f"https://mdvauewlafpdmmqccqjg.supabase.co/storage/v1/object/public/{BUCKET}/packs/{filename}",
                "status":       "existing",
            })
            continue

        # ── Intentar fuente phstudy (solo Takara Tomy) ─────────────────
        product_id = release_code_to_product_id(release_code)
        image_url  = None

        if product_id and product_id in catalog:
            image_url = catalog[product_id][0]  # mejor URL disponible
            print(f"    📦 phstudy: {product_id} → {image_url.split('/')[-1]}")
        elif manufacturer == "hasbro":
            print(f"    ℹ️  Hasbro pack — sin fuente automática disponible")
        else:
            print(f"    ⚠️  No encontrado en phstudy: {product_id or release_code}")

        # ── Descargar imagen ────────────────────────────────────────────
        if image_url:
            success = download_and_convert(image_url, output_path)
            if success:
                ok += 1
                results.append({
                    "pack_id":      pack_id,
                    "release_code": release_code,
                    "manufacturer": manufacturer,
                    "local_path":   str(output_path),
                    "storage_path": f"packs/{filename}",
                    "supabase_url": f"https://mdvauewlafpdmmqccqjg.supabase.co/storage/v1/object/public/{BUCKET}/packs/{filename}",
                    "status":       "downloaded",
                    "source_url":   image_url,
                })
                time.sleep(0.3)
                continue

        # ── REGLA DE LA CAJA: fallback al placeholder ────────────────────
        # Llega aquí si: no hay fuente, o la descarga falló
        reason = (
            "hasbro_no_source" if manufacturer == "hasbro"
            else f"not_in_phstudy:{product_id or release_code}"
            if not image_url
            else "download_failed"
        )
        results.append(fallback_entry(pack, reason))
        placeholder_count += 1
        print(f"    📦 → box_placeholder.webp ({reason})")

    # ── Guardar mapa de resultados ───────────────────────────────────────
    output_json = Path(__file__).parent / "pack_images_map.json"
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ Descargadas:        {ok}")
    print(f"⏭  Ya existían:       {skipped}")
    print(f"📦 Con placeholder:   {placeholder_count}")
    print(f"   → {ok + skipped} packs tendrán imagen real")
    print(f"   → {placeholder_count} packs tendrán box_placeholder.webp")
    print(f"\n📋 Mapa guardado: {output_json}")
    print(f"\n🔍 Próximos pasos:")
    print(f"   1. Revisa images_output/packs/ — reemplaza manualmente")
    print(f"      las que tengan status='placeholder' si tienes la imagen")
    print(f"   2. SUPABASE_SERVICE_KEY='eyJ...' python3 upload_pack_images.py")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
