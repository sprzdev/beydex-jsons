#!/usr/bin/env python3
"""
upload_pack_images.py
─────────────────────────────────────────────────────────────────────────────
Lee pack_images_map.json (generado por scrape_pack_images.py),
sube cada imagen al bucket beyblade-assets/packs/ y actualiza
el campo `image` en la tabla `packs` de Supabase.

Requiere la SERVICE KEY (no la publishable key).

Uso:
  SUPABASE_SERVICE_KEY='eyJ...' python3 upload_pack_images.py
"""

import json
import os
from pathlib import Path
from supabase import create_client

SUPABASE_URL         = "https://mdvauewlafpdmmqccqjg.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
BUCKET               = "beyblade-assets"
INPUT_JSON           = Path(__file__).parent / "pack_images_map.json"


def main():
    if not SUPABASE_SERVICE_KEY:
        print("❌ Falta SUPABASE_SERVICE_KEY")
        print("   SUPABASE_SERVICE_KEY='eyJ...' python3 upload_pack_images.py")
        return

    with open(INPUT_JSON, encoding="utf-8") as f:
        entries = json.load(f)

    ready = [e for e in entries if e["status"] in ("downloaded", "existing") and e["local_path"]]
    print(f"🚀 Subiendo {len(ready)}/{len(entries)} imágenes al bucket...")

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    ok = 0
    errors = 0

    for entry in ready:
        local_path   = Path(entry["local_path"])
        storage_path = entry["storage_path"]   # packs/pack-bx01.webp
        pack_id      = entry["pack_id"]

        if not local_path.exists():
            print(f"  ⚠️  No encontrado localmente: {local_path.name}")
            errors += 1
            continue

        try:
            # Subir al bucket
            with open(local_path, "rb") as f:
                supabase.storage.from_(BUCKET).upload(
                    path=storage_path,
                    file=f.read(),
                    file_options={"content-type": "image/webp", "upsert": "true"}
                )

            # Construir URL pública
            public_url = (
                f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{storage_path}"
            )

            # Actualizar campo image en la tabla packs
            supabase.table("packs").update({"image": public_url}).eq("id", pack_id).execute()

            print(f"  ✅ {local_path.name} → {storage_path}")
            ok += 1

        except Exception as e:
            print(f"  ❌ {local_path.name}: {e}")
            errors += 1

    print(f"\n{'='*50}")
    print(f"✅ Subidas:  {ok}")
    print(f"❌ Errores: {errors}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
