import re
from pathlib import Path

_slug_re = re.compile(r"[^a-zA-Z0-9._-]+")

def slugify(text: str, max_len: int = 80) -> str:
    text = (text or "").strip().lower()
    text = _slug_re.sub("-", text).strip("-")
    return text[:max_len] if len(text) > max_len else text

def ensure_dataset_dir(downloads_root: Path, repository: str, dataset_slug: str) -> Path:
    path = downloads_root / repository / dataset_slug
    path.mkdir(parents=True, exist_ok=True)
    return path