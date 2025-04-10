from typing import List, Dict, Any
import json



def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    # TODO: implement me mfklsdmfkl
    with open(path, "w+") as f:
        json.dump(json_content, f)
    pass
