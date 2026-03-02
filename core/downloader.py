from pathlib import Path
import time
import requests

def download_file(url: str, out_path: Path, user_agent: str, timeout: int = 60, retries: int = 3) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": user_agent}

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
            return
        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)  # backoff

    raise last_err