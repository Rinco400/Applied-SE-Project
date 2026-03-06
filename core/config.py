from dataclasses import dataclass
from pathlib import Path

# Keep evolving this list as you discover real formats in the wild.
QDA_EXTENSIONS = {
".qdpx",
".qdc",
".nvp",
".nvpx",
".atlasproj",
".hpr7",
".ppj",
".pprj",
".qlt",
".f4p",
".qpd",
".mqda",
".mqbac",
".mqtc",
".mqex",
".mqmtr",
".mx24",
".mx24bac",
".mc24",
".mex24",
".mx22",
".mx20",
".mx18",
".mx12",
".mx11",
".mx5",
".mx4",
".mx3",
".mx2",
".m2k",
".loa",
".sea",
".mtr",
".mod",
".mex22"
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