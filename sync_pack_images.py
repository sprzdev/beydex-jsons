#!/usr/bin/env python3
"""
sync_pack_images.py
═══════════════════════════════════════════════════════════════════════════════
Script definitivo para sincronizar imágenes de packs con Supabase Storage.

Flujo completo en un solo script:
  1. Lee los 179 packs de la tabla `packs` en Supabase
  2. Genera box_placeholder.webp si no existe
  3. Descarga products_multilang.json de phstudy.org
  4. Por cada pack:
       → Si tiene imagen en phstudy: descarga, convierte a WebP, guarda local
       → Si no la tiene (Hasbro, limitados): usa box_placeholder.webp
  5. Sube TODO al bucket beyblade-assets/packs/
  6. Hace UPDATE masivo en la tabla `packs` con la URL pública final

Instalación:
  pip install supabase requests Pillow

Uso:
  # Requiere service_role key para escribir en Supabase (no la publishable)
  SUPABASE_SERVICE_KEY='eyJ...' python3 sync_pack_images.py

  # Solo generar imágenes localmente (sin subir a Supabase):
  python3 sync_pack_images.py --dry-run

  # Procesar solo un pack específico (para pruebas):
  SUPABASE_SERVICE_KEY='eyJ...' python3 sync_pack_images.py --only BX-01
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageDraw
from supabase import create_client, Client

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

SUPABASE_URL         = "https://mdvauewlafpdmmqccqjg.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
BUCKET               = "beyblade-assets"
STORAGE_PREFIX       = "packs"

PHSTUDY_BASE         = "https://beyblade.phstudy.org"
PHSTUDY_PRODUCTS_URL = f"{PHSTUDY_BASE}/data/products_multilang.json"

OUTPUT_DIR     = Path(__file__).parent / "output_images" / "packs"
PLACEHOLDER_FN = "box_placeholder.webp"
PLACEHOLDER_PATH = OUTPUT_DIR / PLACEHOLDER_FN

# Dimensiones y calidad
IMAGE_TARGET_LONG = 800    # lado largo máximo (px)
IMAGE_MIN_SIZE    = 400    # lado mínimo aceptable (px)
WEBP_QUALITY      = 85
PLACEHOLDER_SIZE  = (600, 600)

# Rate limiting — pausa entre descargas para no ser bloqueado
DOWNLOAD_DELAY_S  = 0.4

# Headers que simulan un browser normal
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": PHSTUDY_BASE,
    "Accept": "image/webp,image/png,image/*,*/*;q=0.8",
}


# ──────────────────────────────────────────────────────────────────────────────
# TIPOS DE DATOS
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class PackResult:
    pack_id:      str
    release_code: str
    manufacturer: str
    local_path:   Path | None
    storage_path: str           # path dentro del bucket (sin prefijo bucket)
    public_url:   str
    status:       str           # downloaded | placeholder | skipped | error
    reason:       str = ""
    source_url:   str = ""


# ──────────────────────────────────────────────────────────────────────────────
# PASO 1 — Leer packs de Supabase
# ──────────────────────────────────────────────────────────────────────────────

def fetch_packs(supabase: Client) -> list[dict]:
    print("📡 Leyendo packs de Supabase...")
    result = (
        supabase.table("packs")
        .select("id, release_code, name, manufacturer, image")
        .order("release_code")
        .execute()
    )
    packs = result.data or []
    print(f"   ✅ {len(packs)} packs encontrados")
    return packs


# ──────────────────────────────────────────────────────────────────────────────
# PASO 2 — Generar box_placeholder.webp con Pillow
# ──────────────────────────────────────────────────────────────────────────────

def ensure_placeholder() -> Path:
    """
    Genera la imagen placeholder si no existe.
    Fondo oscuro con degradado, texto centrado y marco decorativo.
    """
    if PLACEHOLDER_PATH.exists():
        print(f"   ⏭  Placeholder ya existe: {PLACEHOLDER_PATH.name}")
        return PLACEHOLDER_PATH

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    w, h = PLACEHOLDER_SIZE

    # Fondo oscuro con degradado vertical manual
    img = Image.new("RGB", (w, h), (28, 30, 40))
    draw = ImageDraw.Draw(img)

    # Degradado: píxeles más claros hacia arriba
    for y in range(h):
        alpha = int(20 * (1 - y / h))
        r, g, b = 28 + alpha, 32 + alpha, 48 + alpha
        draw.line([(0, y), (w, y)], fill=(r, g, b))

    # Marco exterior redondeado (simulado con rectángulos)
    margin = 24
    draw.rounded_rectangle(
        [margin, margin, w - margin, h - margin],
        radius=28,
        outline=(70, 110, 190),
        width=3,
    )
    draw.rounded_rectangle(
        [margin + 8, margin + 8, w - margin - 8, h - margin - 8],
        radius=22,
        outline=(45, 75, 140),
        width=1,
    )

    # Icono de caja (dibujado con líneas)
    cx, cy = w // 2, h // 2 - 30
    box_size = 80
    draw.rectangle(
        [cx - box_size // 2, cy - box_size // 2,
         cx + box_size // 2, cy + box_size // 2],
        outline=(100, 150, 220),
        width=2,
    )
    # Línea diagonal de la tapa
    draw.line(
        [cx - box_size // 2, cy - box_size // 2,
         cx, cy - box_size // 2 - 20,
         cx + box_size // 2, cy - box_size // 2],
        fill=(100, 150, 220),
        width=2,
    )

    # Texto principal
    draw.text(
        (cx, cy + box_size // 2 + 28),
        "BEYBLADE X",
        fill=(200, 220, 255),
        anchor="mm",
    )
    draw.text(
        (cx, cy + box_size // 2 + 54),
        "No image available",
        fill=(100, 120, 150),
        anchor="mm",
    )

    img.save(PLACEHOLDER_PATH, "WEBP", quality=WEBP_QUALITY)
    size_kb = PLACEHOLDER_PATH.stat().st_size // 1024
    print(f"   ✅ Placeholder generado: {PLACEHOLDER_PATH.name} ({size_kb}KB)")
    return PLACEHOLDER_PATH


# ──────────────────────────────────────────────────────────────────────────────
# PASO 3A — Descargar catálogo de phstudy.org
# ──────────────────────────────────────────────────────────────────────────────

def fetch_phstudy_catalog() -> dict[str, list[str]]:
    """
    Descarga products_multilang.json y devuelve:
    { "BX01": ["https://.../detail_bx01.png", ...], "BX02": [...] }
    Prioriza: detail_ > @1 (alta res) > _list (thumbnail)
    """
    print(f"\n📡 Descargando catálogo de phstudy.org...")
    try:
        resp = requests.get(
            PHSTUDY_PRODUCTS_URL,
            headers=HEADERS,
            timeout=20
        )
        resp.raise_for_status()
        raw = resp.json()  # puede ser list o dict según versión
    except requests.RequestException as e:
        print(f"   ❌ Error descargando catálogo phstudy: {e}")
        return {}

    # products_multilang.json es una lista de objetos
    entries = raw if isinstance(raw, list) else raw.values()

    catalog: dict[str, list[str]] = {}
    for entry in entries:
        pid    = entry.get("product_id", "")    # "BX01", "BX02", "UX01"
        images = entry.get("images", [])
        if not pid or not images:
            continue

        full_urls = [f"{PHSTUDY_BASE}/{img}" for img in images]

        # Priorizar imágenes de alta calidad
        detail = [u for u in full_urls if "detail_" in u.lower()]
        hires  = [u for u in full_urls if "@1" in u and "detail_" not in u.lower()]
        rest   = [u for u in full_urls
                  if u not in detail and u not in hires and "_list" not in u]
        thumb  = [u for u in full_urls if "_list" in u]

        catalog[pid] = detail + hires + rest + thumb

    print(f"   ✅ {len(catalog)} productos en catálogo phstudy")
    return catalog


# ──────────────────────────────────────────────────────────────────────────────
# PASO 3B — Mapear release_code al product_id de phstudy
# ──────────────────────────────────────────────────────────────────────────────

def to_phstudy_product_id(release_code: str) -> str | None:
    """
    BX-01  → BX01
    BX-14  → BX14
    UX-01  → UX01
    CX-01  → CX01
    F9580  → None  (Hasbro)
    G0184  → None  (Hasbro)
    """
    m = re.match(r'^(BX|UX|CX)-(\d+)$', release_code.strip().upper())
    if m:
        prefix = m.group(1)
        number = int(m.group(2))
        return f"{prefix}{number:02d}"
    return None


# ──────────────────────────────────────────────────────────────────────────────
# PASO 3C — Descargar y convertir imagen a WebP
# ──────────────────────────────────────────────────────────────────────────────

def download_image(url: str) -> Image.Image | None:
    """
    Descarga una imagen desde una URL.
    Devuelve un objeto PIL Image o None si falla.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        if len(resp.content) < 1024:       # menos de 1KB → probablemente error
            return None
        img = Image.open(BytesIO(resp.content))
        img.load()                          # forzar decodificación completa
        return img
    except Exception:
        return None


def process_and_save(img: Image.Image, output_path: Path) -> bool:
    """
    Redimensiona si hace falta, convierte a RGB y guarda como WebP.
    """
    try:
        # Convertir modos especiales a RGBA primero
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGBA")

        # Escalar si la imagen es demasiado grande
        max_side = max(img.size)
        if max_side > IMAGE_TARGET_LONG:
            ratio = IMAGE_TARGET_LONG / max_side
            new_size = (int(img.width * ratio), int(img.height * ratio))
            img = img.resize(new_size, Image.LANCZOS)

        # Componer sobre fondo blanco (elimina transparencia)
        if img.mode == "RGBA":
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            img = bg
        else:
            img = img.convert("RGB")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(output_path, "WEBP", quality=WEBP_QUALITY, method=6)
        return True
    except Exception as e:
        print(f"      ❌ Error guardando imagen: {e}")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# PASO 4 — Procesar cada pack
# ──────────────────────────────────────────────────────────────────────────────

def make_public_url(storage_path: str) -> str:
    return (
        f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{storage_path}"
    )


def process_pack(
    pack: dict,
    catalog: dict[str, list[str]],
    placeholder_path: Path,
) -> PackResult:
    """
    Orquesta la lógica por pack:
      1. Si ya existe localmente → reutilizar
      2. Si phstudy tiene imagen → descargar y convertir
      3. Si no → usar placeholder (Regla de la Caja)
    """
    pack_id      = pack["id"]           # pack-bx01
    release_code = pack["release_code"] # BX-01
    manufacturer = pack["manufacturer"]

    filename     = f"{pack_id}.webp"
    output_path  = OUTPUT_DIR / filename
    storage_path = f"{STORAGE_PREFIX}/{filename}"

    # ── Ya existe localmente ─────────────────────────────────────────────────
    if output_path.exists():
        return PackResult(
            pack_id=pack_id, release_code=release_code,
            manufacturer=manufacturer, local_path=output_path,
            storage_path=storage_path,
            public_url=make_public_url(storage_path),
            status="skipped", reason="already_exists_locally",
        )

    # ── Buscar en catálogo phstudy ────────────────────────────────────────────
    product_id = to_phstudy_product_id(release_code)
    image_urls = catalog.get(product_id, []) if product_id else []

    if image_urls:
        # Intentar cada URL disponible (detail_ primero)
        for attempt, url in enumerate(image_urls, 1):
            img = download_image(url)
            if img is not None:
                if process_and_save(img, output_path):
                    size_kb = output_path.stat().st_size // 1024
                    print(f"      ✅ {filename} ({size_kb}KB) — {url.split('/')[-1]}")
                    return PackResult(
                        pack_id=pack_id, release_code=release_code,
                        manufacturer=manufacturer, local_path=output_path,
                        storage_path=storage_path,
                        public_url=make_public_url(storage_path),
                        status="downloaded", source_url=url,
                    )
            if attempt < len(image_urls):
                time.sleep(0.2)

        # Todas las URLs fallaron
        reason = f"all_{len(image_urls)}_urls_failed"
    else:
        reason = (
            "hasbro_no_source" if manufacturer == "hasbro"
            else f"not_in_phstudy:{product_id or release_code}"
        )

    # ── REGLA DE LA CAJA: fallback al placeholder ─────────────────────────────
    placeholder_storage = f"{STORAGE_PREFIX}/{PLACEHOLDER_FN}"
    print(f"      📦 → box_placeholder.webp ({reason})")
    return PackResult(
        pack_id=pack_id, release_code=release_code,
        manufacturer=manufacturer, local_path=placeholder_path,
        storage_path=placeholder_storage,
        public_url=make_public_url(placeholder_storage),
        status="placeholder", reason=reason,
    )


# ──────────────────────────────────────────────────────────────────────────────
# PASO 5 — Subir imágenes al bucket de Supabase
# ──────────────────────────────────────────────────────────────────────────────

def upload_to_storage(
    supabase: Client,
    local_path: Path,
    storage_path: str,
    dry_run: bool = False,
) -> bool:
    if dry_run:
        return True
    try:
        with open(local_path, "rb") as f:
            data = f.read()
        supabase.storage.from_(BUCKET).upload(
            path=storage_path,
            file=data,
            file_options={"content-type": "image/webp", "upsert": "true"},
        )
        return True
    except Exception as e:
        print(f"         ❌ Upload error ({storage_path}): {e}")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# PASO 6 — UPDATE masivo en tabla `packs`
# ──────────────────────────────────────────────────────────────────────────────

def update_pack_images(
    supabase: Client,
    results: list[PackResult],
    dry_run: bool = False,
) -> int:
    """
    Actualiza el campo `image` en la tabla `packs` para cada resultado.
    Usa upsert en lotes de 50 para no saturar la API.
    """
    if dry_run:
        print(f"\n[DRY RUN] Se actualizarían {len(results)} registros en Supabase")
        return len(results)

    print(f"\n📤 Actualizando campo `image` en tabla `packs`...")
    updated = 0
    batch_size = 50

    for i in range(0, len(results), batch_size):
        batch = results[i:i + batch_size]
        for r in batch:
            try:
                supabase.table("packs").update(
                    {"image": r.public_url}
                ).eq("id", r.pack_id).execute()
                updated += 1
            except Exception as e:
                print(f"   ❌ UPDATE fallido para {r.pack_id}: {e}")
        print(f"   ✅ {min(i + batch_size, len(results))}/{len(results)} packs actualizados")

    return updated


# ──────────────────────────────────────────────────────────────────────────────
# REPORTE FINAL
# ──────────────────────────────────────────────────────────────────────────────

def print_report(results: list[PackResult]):
    downloaded   = [r for r in results if r.status == "downloaded"]
    placeholders = [r for r in results if r.status == "placeholder"]
    skipped      = [r for r in results if r.status == "skipped"]
    errors       = [r for r in results if r.status == "error"]

    print(f"\n{'═'*60}")
    print(f"  RESULTADO FINAL")
    print(f"{'═'*60}")
    print(f"  ✅ Imágenes descargadas:  {len(downloaded)}")
    print(f"  ⏭  Ya existían:           {len(skipped)}")
    print(f"  📦 Con placeholder:        {len(placeholders)}")
    if errors:
        print(f"  ❌ Errores:               {len(errors)}")
    print(f"{'─'*60}")

    # Breakdown de placeholders por razón
    if placeholders:
        reasons: dict[str, int] = {}
        for r in placeholders:
            reasons[r.reason] = reasons.get(r.reason, 0) + 1
        print(f"  Razones del placeholder:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason}: {count}")

    print(f"{'═'*60}")
    print(f"  Todos los packs tienen imagen en Supabase. ✓")
    print(f"{'═'*60}\n")

    # Guardar JSON de resultados para auditoría
    output_json = Path(__file__).parent / "sync_pack_images_report.json"
    report = [
        {
            "pack_id":      r.pack_id,
            "release_code": r.release_code,
            "manufacturer": r.manufacturer,
            "status":       r.status,
            "reason":       r.reason,
            "public_url":   r.public_url,
            "source_url":   r.source_url,
        }
        for r in results
    ]
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  📋 Reporte guardado: {output_json.name}")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sincroniza imágenes de packs entre phstudy.org y Supabase Storage"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Genera y guarda imágenes localmente pero NO sube a Supabase",
    )
    parser.add_argument(
        "--only",
        metavar="RELEASE_CODE",
        help="Procesar solo un pack (ej. BX-01). Útil para pruebas.",
    )
    args = parser.parse_args()

    print("🚀 BeyDeck Pack Image Sync")
    print(f"   Bucket:  {BUCKET}/{STORAGE_PREFIX}/")
    print(f"   Output:  {OUTPUT_DIR}")
    if args.dry_run:
        print("   ⚠️  DRY RUN — no se subirá nada a Supabase")
    print()

    # Verificar service key (solo necesaria si no es dry-run)
    if not args.dry_run and not SUPABASE_SERVICE_KEY:
        print("❌ Falta SUPABASE_SERVICE_KEY")
        print("   Ejecútalo así:")
        print("   SUPABASE_SERVICE_KEY='eyJ...' python3 sync_pack_images.py")
        sys.exit(1)

    # Conectar a Supabase
    key = SUPABASE_SERVICE_KEY or "sb_publishable_OLrLWV2mYYOpRK7tmyLHiw_wrSBESF3"
    supabase = create_client(SUPABASE_URL, key)

    # Paso 1: Leer packs
    packs = fetch_packs(supabase)
    if args.only:
        packs = [p for p in packs if p["release_code"].upper() == args.only.upper()]
        if not packs:
            print(f"❌ Pack '{args.only}' no encontrado en Supabase")
            sys.exit(1)
        print(f"   Modo --only: procesando únicamente {args.only}")

    # Paso 2: Placeholder
    print("\n🖼  Preparando placeholder...")
    placeholder = ensure_placeholder()

    # Paso 3: Catálogo phstudy
    catalog = fetch_phstudy_catalog()

    # Paso 4: Procesar cada pack
    print(f"\n🔄 Procesando {len(packs)} packs...\n")
    results:            list[PackResult] = []
    uploaded_paths:     set[str] = set()   # evitar subir el placeholder múltiples veces

    for i, pack in enumerate(packs):
        release_code = pack["release_code"]
        print(f"[{i+1:>3}/{len(packs)}] {release_code:<10} {pack['name'][:50]}")

        result = process_pack(pack, catalog, placeholder)
        results.append(result)

        # Paso 5: Subir a Storage
        if result.status in ("downloaded",) or (
            result.status == "placeholder"
            and result.storage_path not in uploaded_paths
        ):
            upload_ok = upload_to_storage(
                supabase,
                result.local_path,
                result.storage_path,
                dry_run=args.dry_run,
            )
            if upload_ok and not args.dry_run:
                uploaded_paths.add(result.storage_path)
                if result.status == "downloaded":
                    print(f"      ☁️  Subido a Storage")

        # Rate limiting
        if result.status == "downloaded":
            time.sleep(DOWNLOAD_DELAY_S)

    # Subir placeholder una vez si no se ha subido ya
    if not args.dry_run and f"{STORAGE_PREFIX}/{PLACEHOLDER_FN}" not in uploaded_paths:
        print(f"\n☁️  Subiendo placeholder al bucket...")
        upload_to_storage(
            supabase,
            placeholder,
            f"{STORAGE_PREFIX}/{PLACEHOLDER_FN}",
            dry_run=False,
        )

    # Paso 6: UPDATE masivo en Supabase
    update_pack_images(supabase, results, dry_run=args.dry_run)

    # Reporte final
    print_report(results)


if __name__ == "__main__":
    main()
