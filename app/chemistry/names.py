import re

NOISE_RX = re.compile(
    r"(\b\d{1,3}\s*%(?:\s*(?:w/v|v/v))?\b)|"
    r"(\b(?:ml|l|kg|g|lb|gal|drum|tote|pail|bottle|case|kit|refill|cartridge)s?\b)|"
    r"(\b(?:acs|tech(?:nical)?|reagent|usp|nf|bp|fcc|food|industrial|anhydrous|solution|aq\.?|conc(?:entrated)?)\b)|"
    r"[®™]", flags=re.IGNORECASE
)

def normalize_name(n: str | None) -> str | None:
    if not n: return None
    s = NOISE_RX.sub(" ", n)
    s = re.sub(r"\s+", " ", s).strip()
    return s
