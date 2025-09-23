import asyncio, httpx, random
from app.core.config import settings

class BaseScraper:
    def __init__(self, source: str):
        self.source = source
        self.client = httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT)

    async def fetch(self, url: str, headers: dict | None = None) -> dict | None:
        for attempt in range(settings.MAX_RETRIES):
            try:
                resp = await self.client.get(url, headers=headers)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code in [429, 500, 502, 503]:
                    backoff = settings.RETRY_BACKOFF_BASE * (2 ** attempt)
                    await asyncio.sleep(backoff + random.random())
            except Exception as e:
                await asyncio.sleep(1.0)
        return None
