from datetime import datetime, date
from decimal import Decimal
from typing import Any
import pandas as pd
import numpy as np

def sanitize_json(obj: Any) -> Any:
    """Recursively convert pandas/NumPy/Decimal/Datetime into JSON-serializable types."""
    # Simple types
    if obj is None or isinstance(obj, (str, int)):
        return obj
    if isinstance(obj, float):
        # Convert NaN/Inf to None per JSON rules
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (pd.Timestamp,)):
        # Treat like datetime
        return obj.to_pydatetime().isoformat()
    if isinstance(obj, Decimal):
        return float(obj)

    # Collections
    if isinstance(obj, dict):
        return {sanitize_json(k): sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [sanitize_json(v) for v in obj]

    # NumPy types
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return None if np.isnan(v) or np.isinf(v) else v
    if isinstance(obj, (np.ndarray,)):
        return [sanitize_json(v) for v in obj.tolist()]

    # Fallback to string
    return str(obj)
