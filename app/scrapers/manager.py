import re
import asyncio
from typing import Any, Dict

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from app.scrapers.pubchem import PubChemScraper
from app.db.session import AsyncSessionLocal
from app.db.models import ScrapedCompound, ScrapingLog

CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")

def _normalize_pubchem_properties(payload: dict | None) -> dict | None:
    """
    PubChem PUG 'property' response -> first Properties entry flattened
    """
    if not payload:
        return None
    try:
        props = payload["PropertyTable"]["Properties"][0]
    except Exception:
        return None

    # Keep a small, consistent shape (extend later as needed)
    return {
        "CID": props.get("CID"),
        "MolecularFormula": props.get("MolecularFormula"),
        "MolecularWeight": props.get("MolecularWeight"),
        "IUPACName": props.get("IUPACName"),
        "_raw": payload,  # keep full raw in case you need extra fields later
    }

class ScrapingManager:
    def __init__(self):
        self.pubchem = PubChemScraper("pubchem")

    async def scrape_compound(self, query: str) -> Dict[str, Any]:
        """
        Query PubChem by name and by CAS (if query looks like a CAS).
        Normalize, upsert into DB, and log the job.
        """
        # Decide which calls to make
        tasks = [self.pubchem.fetch_by_name(query)]
        if CAS_RE.match(query):
            tasks.append(self.pubchem.fetch_by_cas(query))
        else:
            tasks.append(asyncio.sleep(0, result=None))  # keep results index stable

        by_name, by_cas = await asyncio.gather(*tasks, return_exceptions=True)

        norm_name = _normalize_pubchem_properties(None if isinstance(by_name, Exception) else by_name)
        norm_cas  = _normalize_pubchem_properties(None if isinstance(by_cas,  Exception) else by_cas)

        # Choose “best” result (prefer CAS if present & valid)
        chosen = norm_cas or norm_name
        await self._persist(query=query, chosen=chosen, raw_by_name=by_name, raw_by_cas=by_cas)

        return {
            "pubchem_by_name": by_name if not isinstance(by_name, Exception) else None,
            "pubchem_by_cas": by_cas if not isinstance(by_cas, Exception) else None,
            "normalized": chosen,
        }

    async def _persist(self, query: str, chosen: dict | None, raw_by_name: dict | None, raw_by_cas: dict | None):
        async with AsyncSessionLocal() as session:
            try:
                if chosen and chosen.get("CID"):
                    cid = str(chosen["CID"])

                    # Upsert (on source+external_id)
                    stmt = insert(ScrapedCompound).values(
                        query=query,
                        source="pubchem",
                        external_id=cid,
                        properties=chosen,
                    ).on_conflict_do_update(
                        index_elements=["source", "external_id"],
                        set_={
                            "query": query,
                            "properties": chosen,
                        }
                    )
                    await session.execute(stmt)
                    await session.flush()

                    # Log success
                    session.add(ScrapingLog(
                        job_type="compound_lookup",
                        query=query,
                        source="pubchem",
                        status="success",
                        message=f"CID={cid}",
                        attempt=1,
                    ))
                else:
                    # Log failure
                    session.add(ScrapingLog(
                        job_type="compound_lookup",
                        query=query,
                        source="pubchem",
                        status="error",
                        message="No result from PubChem",
                        attempt=1,
                    ))

                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                # Best-effort error log
                try:
                    session.add(ScrapingLog(
                        job_type="compound_lookup",
                        query=query,
                        source="pubchem",
                        status="error",
                        message=f"DB error: {e}",
                        attempt=1,
                    ))
                    await session.commit()
                except Exception:
                    pass
