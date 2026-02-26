from dataclasses import dataclass
from pathlib import Path

# Keep evolving this list as you discover real formats in the wild.
QDA_EXTENSIONS = {
    ".qdpx",   # REFI / QDPX
    ".nvp",    # NVivo project
    ".nvpx",   # NVivo exchange (sometimes)
    ".atlproj",  # ATLAS.ti project
    ".mx",     # MAXQDA (some variants)
    ".mxa",    # MAXQDA archive (sometimes)
}

@dataclass(frozen=True)
class Settings:
    # Root folder for all downloads (will be created automatically)
    downloads_root: Path = Path("my_downloads")

    # SQLite database path (created automatically)
    db_path: Path = Path("metadata.db")

    # Helpful for polite scraping and debugging
    user_agent: str = "QDArchiveSeedingBot/1.0 (FAU Applied SE Part1)"

SETTINGS = Settings()