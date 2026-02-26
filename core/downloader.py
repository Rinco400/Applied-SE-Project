from pathlib import Path
import requests

def download_file(url: str, out_path: Path, user_agent: str, timeout: int = 60) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": user_agent}

    with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)