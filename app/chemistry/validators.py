import re

CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")

def normalize_cas(s: str | None) -> str | None:
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    m = re.match(r"^(\d{2,7})(\d{2})(\d)$", digits)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

def cas_is_valid(s: str | None) -> bool:
    s = normalize_cas(s)
    if not s: return False
    body = re.sub(r"-", "", s)[:-1]
    check = int(s[-1])
    # compute check digit
    total = 0
    for i, ch in enumerate(reversed(body), start=1):
        total += int(ch) * i
    return total % 10 == check
