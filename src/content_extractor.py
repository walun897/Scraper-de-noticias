from __future__ import annotations
from typing import Optional, Dict
import trafilatura, json

def extract_main_text(url: str) -> Dict[str, Optional[str]]:
    downloaded = trafilatura.fetch_url(url, no_ssl=True)
    if not downloaded:
        return {"content": None, "language": None, "title": None, "author": None}
    data = trafilatura.extract(downloaded, include_comments=False, include_tables=False,
                               include_links=False, with_metadata=True, output="json")
    if not data:
        return {"content": None, "language": None, "title": None, "author": None}
    obj = json.loads(data)
    return {
        "content": obj.get("text"),
        "language": obj.get("language"),
        "title": obj.get("title"),
        "author": (",".join(obj.get("authors")) if isinstance(obj.get("authors"), list) else obj.get("author")),
    }
