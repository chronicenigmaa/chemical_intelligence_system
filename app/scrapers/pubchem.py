from .base import BaseScraper

class PubChemScraper(BaseScraper):
    BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

    async def fetch_by_name(self, name: str) -> dict | None:
        url = f"{self.BASE_URL}/compound/name/{name}/property/MolecularFormula,MolecularWeight,IUPACName/JSON"
        return await self.fetch(url)

    async def fetch_by_cas(self, cas: str) -> dict | None:
        url = f"{self.BASE_URL}/compound/xref/RN/{cas}/property/MolecularFormula,MolecularWeight,IUPACName/JSON"
        return await self.fetch(url)