from __future__ import annotations

import shutil
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

def download_file(url: str, destination: Path):
    """Util. function to download a remote file without exposing partial final file"""
    
    destination.parent.mkdir(parents=True, exist_ok=True)
    
    temporary_path = destination.with_suffix(f"{destination.suffix}.part")
    temporary_path.unlink(missing_ok=True)
    
    request = Request(url, headers={"User-Agent": "spark-dagster-taxi-pipeline/1.0"})
    
    try: 
        with(urlopen(request, timeout=60) as response, temporary_path.open("wb") as output_file):
            shutil.copyfileobj(response, output_file, length=1024*1024)
            
            if temporary_path.stat().st_size == 0:
                raise OSError("Downloaded file was empty")
            
            temporary_path.replace(destination)
    except (OSError, URLError) as exc:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(f"Unable to download file from {url}") from exc
    
    