import asyncio
import hashlib
import json
import logging
import httpx
import requests
from requests.auth import AuthBase
from redis.asyncio import Redis


from document_setup.src.modules.knowledge_prompt_bank import (
    QUERY_REFINER_PROMPT_V2,
    QUERY_REFINER_PROMPT_V3,
)
from document_setup.src.configs.app import settings
from document_setup.src.modules.search_journals import SearchJournal
from document_setup.src.schemas.search_papers import SearchQueries, PaperTitle

from get_llm_response import get_llm, get_answer_with_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class OpenAlexAuth(AuthBase):

    def __init__(self, config: dict):
        self.config = config

    def __call__(self, r):
        if self.config.get("api_key"):
            r.headers["Authorization"] = f"Bearer {self.config['api_key']}"

        if self.config.get("email"):
            r.headers["From"] = self.config["email"]

        if self.config.get("user_agent"):
            r.headers["User-Agent"] = self.config["user_agent"]

        return r


class SearchPapers(SearchJournal):
    """
    Handles searching, downloading, and processing academic papers.

    Uses a language model client (initialized externally, typically Gemini)
    for refining queries and extracting key points. Interacts with Google CSE
    for searching papers and attempts to download PDFs.

    Attributes:
        client: An instance of the language model client (e.g., genai.Client).
        model_id (str): Identifier for the language model used by the client.
    """
    def __init__(
        self, 
        model_id: str, 
        document_id: str, 
        user_id: str, 
        use_web_search: bool, 
        redis_client: Redis, 
        language: str, 
        max_tokens: int, 
        llm_key: str = ""
    ):
        """
        Initializes the SearchPapers class.

        Args:
            model_id (str): The identifier of the language model to use.
            client: An initialized language model client instance.
        """
        self.model_id = model_id
        self.redis_client = redis_client
        super().__init__(self.redis_client)
        self.llm = get_llm(self.model_id, llm_key, max_tokens, 1.0)
        if "gemini" in self.model_id:
            self.llm_with_search = self.llm
        elif "gpt" in self.model_id:
            tool = {"type": "web_search_preview"}
            self.llm_with_search = self.llm.bind_tools([tool])
        self.input_tokens = 0
        self.output_tokens = 0
        self.web_search_call = 0
        self.use_web_search = use_web_search
        self.language = language
        self.document_id = document_id if document_id else user_id

    @property
    def id(self):
        return self.document_id[:8]

    def __str__(self):
        return self.id

    async def refine_query_v2(self, field: str, domain: str, subdomains: list[str], proposal: dict, query: str) -> SearchQueries:
        if proposal:
            user_proposal = f"User's proposal:\n{proposal}\n"
            prompt = QUERY_REFINER_PROMPT_V2
        else:
            user_proposal = f"User's query:\n{query}" if query else ""
            prompt = QUERY_REFINER_PROMPT_V3
        content = f"""
        User's field:
        {field}
        User's domain:
        {domain}
        User's subdomains:
        {subdomains}
        {user_proposal}"""
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema(
            self.id, 
            self.llm,
            prompt,
            content,
            SearchQueries
        )
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        if not success:
            raise error
        return result
    
    async def _search_papers(
        self,
        query: str,
        list_papers_title: set,
        list_papers: list[dict],
        country_filter: str,
        oa_filter: bool,
        is_oa: bool,
        cut_off_year_low: int,
        cut_off_year_high: int,
        search_range: str,
        search_num: int = 50,
    ) -> tuple[list[str], list[dict]]:
        config = {
            "max_retries": 10,
            "email": settings.OPEN_ALEX_EMAIL,
            "user_agent": settings.OPEN_ALEX_USER,
            "api_key": settings.OPEN_ALEX_KEY,
            "backoff_factor": 0.1,
            "status_forcelist": [429, 500, 503],
        }
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        transport = httpx.AsyncHTTPTransport(retries=2)

        # Prepare search query (supports Vietnamese and all languages directly)
        search_query = query
        if len(search_range):
            if search_range != "all":
                search_query += " " + search_range

        logger.info(f"[{self}] Search query: {search_query}")

        # Build country filters
        filter_parts = []

        if country_filter == "both":
            filter_parts.append("institutions.is_global_south:true")
        elif country_filter == "out":
            filter_parts.append("institutions.is_global_south:true")
            filter_parts.append("authorships.countries:!VN")
        elif country_filter == "in":
            filter_parts.append("authorships.countries:VN")

        # Build open access filter
        if oa_filter:
            if is_oa:
                filter_parts.append("open_access.is_oa:true")
            else:
                filter_parts.append("open_access.is_oa:false")

        # Build year filters
        if cut_off_year_low:
            filter_parts.append(f"from_publication_date:{cut_off_year_low}-01-01")
        if cut_off_year_high:
            filter_parts.append(f"to_publication_date:{cut_off_year_high}-12-31")

        # Combine filters
        filter_string = ",".join(filter_parts) if filter_parts else None

        # Build URL with search parameter (supports Vietnamese directly)
        # Use search= parameter for multilingual support including Vietnamese
        url_parts = ["https://api.openalex.org/works"]
        params = []

        # Add search query
        params.append(f"search={search_query.replace(' ', '+')}")

        # Add filters if any
        if filter_string:
            params.append(f"filter={filter_string}")

        # Add pagination and sorting
        params.append(f"per_page={search_num}")
        params.append("sort=relevance_score:desc")

        url = url_parts[0] + "?" + "&".join(params)

        logger.info(f"[{self}] OpenAlex URL: {url}")

        cache_key = f"openalex:search:{hashlib.sha256(url.encode()).hexdigest()}"
        res = None
        if self.redis_client:
            try:
                cached_bytes = await self.redis_client.get(cache_key)
                if cached_bytes:
                    res = json.loads(cached_bytes)
                    logger.info(f"[{self}] OpenAlex cache HIT for {search_query[:30]}")
            except Exception as ce:
                logger.warning(f"[{self}] Redis cache read error: {ce}")

        if res is None:
            http_timeout = httpx.Timeout(connect=5.0, read=12.0, write=5.0, pool=5.0)
            try:
                async with httpx.AsyncClient(limits=limits, transport=transport, auth=OpenAlexAuth(config), timeout=http_timeout) as client:
                    result = await client.get(url)
                    result.raise_for_status()
                    res = result.json()
                if self.redis_client and res.get("results"):
                    try:
                        await self.redis_client.set(cache_key, json.dumps(res), ex=86400)
                    except Exception as ce:
                        logger.warning(f"[{self}] Redis cache write error: {ce}")
            except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError) as e:
                logger.warning(f"[{self}] OpenAlex HTTP error for query '{search_query[:30]}': {e}")
                res = {"results": []}
            except Exception as e:
                logger.error(f"[{self}] Unexpected error during OpenAlex search: {e}")
                res = {"results": []}

        for work in (res.get("results") or []):
            if work.get("title") not in list_papers_title:
                paper = {}
                if not work.get("title") or work["title"] in ("Unknown", "None"):
                    continue
                paper["title"] = work["title"]
                paper["source"] = "open_alex"
                try:
                    paper["abs"] = " ".join(list(work["abstract_inverted_index"].keys())) if len(list(work["abstract_inverted_index"].keys())) > 100 else ""
                except Exception:
                    paper["abs"] = ""
                if cut_off_year_low:
                    if int(work["publication_year"]) < cut_off_year_low:
                        continue
                if cut_off_year_high:
                    if int(work["publication_year"]) > cut_off_year_high:
                        continue
                if work.get("open_access", {}).get("is_oa"):
                    paper["open_access"] = True
                    paper["url"] = work["open_access"].get("oa_url", "")
                else:
                    paper["open_access"] = False
                    paper["url"] = work.get("primary_location", {}).get("landing_page_url", "")
                paper["isDownloaded"] = False
                paper["authors"] = []
                for author in work.get("authorships", []):
                    paper["authors"].append(author.get("author", {}).get("display_name", ""))
                if not len(paper["authors"]):
                    paper["authors"] = ["Author"]
                paper["year"] = work.get("publication_year", "")
                prim_loc = work.get("primary_location") or {}
                if prim_loc.get("source"):
                    paper["journal"] = prim_loc["source"].get("display_name", "")
                    paper["publisher"] = prim_loc["source"].get("host_organization_name", "")
                    paper["issn"] = prim_loc["source"].get("issn_l", "")
                else:
                    paper["journal"] = ""
                    paper["publisher"] = ""
                    paper["issn"] = ""
                biblio = work.get("biblio") or {}
                paper["volume"] = biblio.get("volume") or ""
                paper["issue"] = biblio.get("issue") or ""
                paper["pages"] = f'{biblio["first_page"]}-{biblio["last_page"]}' if biblio.get("first_page") and biblio.get("last_page") and int(biblio["first_page"]) < int(biblio["last_page"]) else ""
                if paper["journal"] != "":
                    q, issn = await self.get_q_issn(
                        paper["journal"].replace(" ", "_").replace(":", "_").lower(),
                        paper["year"]
                    )
                    paper["q"] = q
                    if paper["issn"] != "":
                        paper["issn"] = issn
                else:
                    paper["q"] = 'N/A'
                paper["citation"] = work.get("cited_by_count") or 0
                paper["language"] = work.get("language") or "Unknown"
                paper["type"] = work.get("type") or "Unknown"
                if paper["type"] == "preprint":
                    paper["status"] = "Preprint"
                else:
                    paper["status"] = "Peer-reviewed"
                list_papers.append(paper)
                list_papers_title.add(work["title"])
        return list_papers_title, list_papers

    async def search_papers(
        self, 
        field: str,
        domain: str,
        subdomains: list[str],
        proposal: dict,
        downloaded_papers: list[str],
        country_filter: str,
        oa_filter: bool,
        is_oa: bool,
        cut_off_year_low: int, 
        cut_off_year_high: int, 
        search_range: str,
        query: str = ""
    ) -> tuple[list[dict], bool]:
        """
        Searches for academic papers based on refined queries, filters by year,
        and enriches them with publication details and journal rankings.

        The process involves:
        1. Refining the initial domain(s) into specific search queries using an LLM.
        2. Searching Google Scholar for each query, applying year cut-offs if provided.
        3. Extracting paper details (title, URL, open access status).
        4. Enriching the paper list with author, year, journal name, SJR Quartile, and ISSN.

        Args:
            domains (list[str]): A list of research domains to search within.
            cut_off_year_low (int | None): The earliest publication year to include.
            cut_off_year_high (int | None): The latest publication year to include.
            start (int, optional): The starting index for search results (for pagination).
                                   Defaults to 0.

        Returns:
            list[dict]: A list of dictionaries, each representing a found and enriched
                        academic paper, sorted by journal ranking. Returns a maximum
                        of 50 papers.
        """
        list_papers: list[dict] = []
        list_papers_title = set(downloaded_papers)
        if query:
            list_papers_title, list_papers = await self._search_papers(
                query,
                list_papers_title,
                list_papers,
                country_filter,
                oa_filter,
                is_oa,
                cut_off_year_low,
                cut_off_year_high,
                search_range,
                50,
            )
        search_num = 50 if len(subdomains) == 1 else 20
        for subdomain in subdomains:
            list_papers_title, list_papers = await self._search_papers(
                subdomain,
                list_papers_title,
                list_papers,
                country_filter,
                oa_filter,
                is_oa,
                cut_off_year_low,
                cut_off_year_high,
                search_range,
                search_num,
            )
        if len(list_papers) < 100:
            logger.info(f"[{self}] Not enough papers: {len(list_papers)}/100")       
            queries = await self.refine_query_v2(field, domain, subdomains, proposal, query)
            logger.info(f"[{self}] {queries.search_queries}")
            for search_query in queries.search_queries:
                list_papers_title, list_papers = await self._search_papers(
                    search_query,
                    list_papers_title,
                    list_papers,
                    country_filter,
                    oa_filter,
                    is_oa,
                    cut_off_year_low,
                    cut_off_year_high,
                    search_range,
                )
            if len(list_papers) < 100:
                if field:
                    logger.info(f"[{self}] Not enough papers: {len(list_papers)}/100")
                    list_papers_title, list_papers = await self._search_papers(
                        field,
                        list_papers_title,
                        list_papers,
                        country_filter,
                        oa_filter,
                        is_oa,
                        cut_off_year_low,
                        cut_off_year_high,
                        search_range,
                    )
                    list_papers_title, list_papers = await self._search_papers(
                        domain,
                        list_papers_title,
                        list_papers,
                        country_filter,
                        oa_filter,
                        is_oa,
                        cut_off_year_low,
                        cut_off_year_high,
                        search_range,
                    )     
        # sorted_papers = sorted(list_papers, key=lambda x: x['open_access'], reverse=True)
        semaphore = asyncio.Semaphore(10)

        async def _translate_with_semaphore(title: str):
            async with semaphore:
                return await self.add_user_language_title(title)

        tasks = [_translate_with_semaphore(paper["title"]) for paper in list_papers]
        usage_results = await asyncio.gather(*tasks, return_exceptions=True)
        for paper, res in zip(list_papers, usage_results):
            if isinstance(res, tuple) and len(res) == 3:
                user_language_title, in_tokens, out_tokens = res
                paper["user_language_title"] = user_language_title
                self.input_tokens += in_tokens
                self.output_tokens += out_tokens
            else:
                paper["user_language_title"] = paper.get("title", "Unknown")
        logger.info(f"[{self}] Total papers: {len(list_papers)}/100")
        return list_papers

    async def add_user_language_title(self, title: str) -> tuple[PaperTitle, int, int]:
        title_prompt = f"Translate the following research paper's title into the {self.language}"
        _, success, title, input_tokens, output_tokens = await get_answer_with_schema(self.id, self.llm, title_prompt, title, PaperTitle)
        if not success:
            return "Unknown", input_tokens, output_tokens
        else:
            return title.title, input_tokens, output_tokens