import asyncio
import logging
import time
import rapidfuzz

from document_setup.src.modules.search_web import SearchWeb
from document_setup.src.schemas.proposal import (
    Domains,
    Ideas,
    FinalProposal,
    ResearchType,
    Title,
    UserInfo,
    Phrases,
    KeywordType,
    CRITERIA_MAP,
    SubDomains,
)
from document_setup.src.modules.knowledge_prompt_bank import (
    DOMAINS_PROMPT,
    EVAL_PROMPT,
    FINAL_PROPOSAL_PROMPT,
    GENERATE_IDEAS_SOCIAL_SCIENCE_PROMPT_V2,
    RESEARCH_TYPE_PROMPT,
    TRANSLATE_PROMPT,
    SUBDOMAINS_PROMPT,
    CRITERIA,
    GENERATE_TITLE,
    GET_USER_INFO_PROMPT,
    SPLIT_TITLE,
    KEYWORD_PROMPT_V3,
)

from exception_type import AIERROR
from get_llm_response import get_llm, get_answer_with_schema
from utils import get_minio_client
from translate import LANGUAGE_KIT

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

# Level hợp lệ (khoá của CRITERIA / CRITERIA_MAP). BE phải gửi đúng một trong các giá trị này.
DEFAULT_LEVEL = 'LUAN_VAN_THAC_SI'


def _criteria_text(level: str) -> str:
    """Trả mô tả tiêu chí theo level; fallback level mặc định nếu level lạ."""
    text = CRITERIA.get(level)
    if text is None:
        logger.warning(f"[criteria] level='{level}' không có trong CRITERIA → dùng '{DEFAULT_LEVEL}'")
        text = CRITERIA.get(DEFAULT_LEVEL, "")
    return text


def _criteria_schema(level: str):
    """Trả schema tiêu chí theo level; fallback mặc định thay vì None.

    Trước đây ``CRITERIA_MAP.get(level)`` trả None với level lạ ⇒ truyền None vào
    ``llm.with_structured_output(None)`` ⇒ Gemini báo 'Unsupported tool type None'
    (error 615/624 khó hiểu).
    """
    schema = CRITERIA_MAP.get(level)
    if schema is None:
        logger.warning(f"[criteria] level='{level}' không có schema → dùng '{DEFAULT_LEVEL}'")
        schema = CRITERIA_MAP[DEFAULT_LEVEL]
    return schema


# BE có thể gửi "Vietnamese"/"vi"/"en"; LANGUAGE_KIT dùng khoá dạng "tiếng việt"/"english".
_LANG_ALIASES = {
    "vi": "tiếng việt", "vietnamese": "tiếng việt", "tiếng việt": "tiếng việt",
    "en": "english", "english": "english",
}
_DEFAULT_LANG_KEY = "tiếng việt"


def _lang_key(language: str) -> str:
    key = _LANG_ALIASES.get((language or "").strip().lower())
    if key not in LANGUAGE_KIT:
        logger.warning(f"[lang] language='{language}' không có trong LANGUAGE_KIT → dùng '{_DEFAULT_LANG_KEY}'")
        key = _DEFAULT_LANG_KEY
    return key


class IdeaGeneration(SearchWeb):

    def __init__(
        self, 
        document_id: str, 
        model_id: str, 
        language: str, 
        llm_key: str, 
        max_tokens: int, 
        search_web_key: str = "", 
        use_web_search: bool = False
    ):
        self.document_id = document_id
        self.model_id = model_id
        self.llm = get_llm(self.model_id, llm_key, max_tokens, 1.0)
        super().__init__(self.document_id, model_id, self.llm, search_web_key, use_web_search)
        self.minioClient = get_minio_client()
        self.language = language
    
    @property
    def id(self):
        return self.document_id[:8]

    def __str__(self):
        return self.id
    
    async def generate_keywords_v3(
        self,
        proposal: dict,
    ) -> tuple[dict, int, int]:
        content = f"Title: {proposal["title"]}"
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm,
            SPLIT_TITLE,
            content,
            Phrases
        )
        if not success:
            raise error
        else:
            logger.info(f"[{self}] {result}")
            unique_phrases = list(set(result.phrases))
            non_overlap_pharse = [
                s for s in unique_phrases
                if not any((s != other and s in other) for other in unique_phrases)
            ]
            tasks = [self.match_phrase(phrase, proposal["title"]) for phrase in non_overlap_pharse]
            results = await asyncio.gather(*tasks)
            final_dict = {
                "main_keywords": [],
                "supplementary_keywords": [],
            }
            for result in results:
                if result[0] != "":
                    final_dict[result[0]].append(result[1])
                input_tokens += result[2]
                output_tokens += result[3]
            return final_dict, input_tokens, output_tokens

    async def match_phrase(self, phrase: str, title: str) -> tuple[str, str, int, int]:
        # title_words = title.split() 
        # n = len(phrase.split())
        # title_substrings = [" ".join(title_words[i:i + n]) for i in range(len(title_words) - n + 1)]
        # titles_set = set(title_substrings)
        # if phrase in titles_set:
        #     final_phrase = phrase
        # else:
        #     best_match = rapidfuzz.process.extractOne(phrase, titles_set, scorer=rapidfuzz.fuzz.WRatio)
        #     final_phrase = best_match[0]

        content = f"Title: {title}\nKeyword: {phrase}"
        _, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm,
            KEYWORD_PROMPT_V3,
            content,
            KeywordType
        )
        if not success:
            return "", "", 0, 0
        else:
            keyword_type = ["main_keywords", "supplementary_keywords", "gibberish"]
            if result.keyword_type in keyword_type:
                final_type = result.keyword_type
            else:
                best_match = rapidfuzz.process.extractOne(result.keyword_type, keyword_type, scorer=rapidfuzz.fuzz.WRatio)
                final_type = best_match[0]
            if final_type == "gibberish":
                final_type = ""
            return final_type, phrase, input_tokens, output_tokens

    async def generate_domains(
        self, 
        field: str, 
        domains_num: int,
    ) -> dict:
        content = f"""
            User's field:
            {field}
            User's language:
            {self.language}
            Generate {domains_num} domains
        """
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm,
            DOMAINS_PROMPT,
            content,
            Domains,
        )
        if not success:
            raise error
        else:
            result_dict = result.model_dump()
            result_dict["domains"] = [domain for domain in result_dict["domains"] if len(domain) >= 4]
            return result_dict, input_tokens, output_tokens
    
    async def generate_subdomains(
        self, 
        field: str,
        domain: str,
        subdomains_num: int,
    ) -> dict:
        content = f"""
            User's field:
            {field}
            User's domain:
            {domain}
            User's language:
            {self.language}
            Generate {subdomains_num} subdomains
        """
        error, success, result, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm,
            SUBDOMAINS_PROMPT,
            content,
            SubDomains,
        )
        if not success:
            raise error
        else:
            result_dict = result.model_dump()
            result_dict["subdomains"] = [domain for domain in result_dict["subdomains"] if len(domain) >= 4]
            return result_dict, input_tokens, output_tokens
    
    async def generate_titles_v2(
        self, 
        field: str,
        domain: str,
        subdomains: list[str],
        level: str,
    ) -> tuple[dict, dict]:
        start = time.time()
        if len(subdomains):
            tasks = [self._generate_for_subdomain(field, domain, subdomain) for subdomain in subdomains]
            results = await asyncio.gather(*tasks)
            web_search_results = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            search_web = [result[3] for result in results]
            self.input_tokens += sum(input_tokens)
            self.output_tokens += sum(output_tokens)
            self.web_search_call += sum(search_web)
            logger.info(f"[{self}] Search web time: {time.time() - start}")
            knowledge_base = [
                {"subdomain": subdomain, "web_search_result": web_search_result} 
                for subdomain, web_search_result in zip(subdomains, web_search_results)
            ]
        else:
            knowledge_base = []
        proposals = await self.generate_proposal_v2(field, domain, knowledge_base, level, 9)            
        logger.info(f"[{self}] Total time: {time.time() - start}")
        return proposals, knowledge_base
    
    async def _generate_for_subdomain(
        self,
        field: str,
        domain: str,
        subdomain: str,
    ) -> str:
        start = time.time()
        plan, plan_input_tokens, plan_output_tokens = await self.plan_knowledge_base_v2(field, domain, subdomain)
        plan_time = time.time()
        logger.info(f"[{self}] Plan time: {plan_time - start}")
        web_search_result, web_input_tokens, web_output_tokens, search_web = await self.construct_knowledge_base(plan)
        return web_search_result, plan_input_tokens + web_input_tokens, plan_output_tokens + web_output_tokens, search_web

    async def update_title(
        self, 
        field: str,
        domain: str,
        knowledge_base: list[dict],
        title: str,
        level: str,
    ) -> dict:
        start = time.time()
        proposal, input_tokens, output_tokens = await self._generate_proposal_v2(
            title, 
            field, 
            domain, 
            knowledge_base, 
            level,
        )
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        logger.info(f"[{self}] Update proposal time: {time.time() - start}")
        proposal["title"] = title
        return proposal

    async def generate_proposal_v2(
        self, 
        field: str,
        domain: str,
        knowledge_base: list[dict],
        level: str,
        num_ideas: int,
    ) -> list[FinalProposal]:
        start = time.time()
        content = f"""
        Important criteria must satisfy:
        {_criteria_text(level)}
        Target field:
        {field}
        Target domain:
        {domain}
        Knowledge base:
        {knowledge_base}
        Generate {num_ideas} ideas
        """
        error, success, ideas, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            GENERATE_IDEAS_SOCIAL_SCIENCE_PROMPT_V2, 
            content, 
            Ideas
        )
        if not success:
            raise error
        else:
            idea_time = time.time()
            logger.info(f"[{self}] Idea time: {idea_time - start}")
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens
            tasks = [
                self._generate_proposal_v2(
                    idea.idea, 
                    field, 
                    domain, 
                    idea.web_search_result,
                    level, 
                ) for idea in ideas.ideas
            ]
            results = await asyncio.gather(*tasks)
            proposals = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            self.input_tokens += sum(input_tokens)
            self.output_tokens += sum(output_tokens)
            final_proposals: list[dict] = []
            seen_titles: set = set()
            for proposal in proposals:
                if proposal:
                    if proposal["title"].lower() not in seen_titles and proposal["title"] != "FinalProposal":
                        seen_titles.add(proposal["title"].lower())
                        final_proposals.append(proposal)
            return final_proposals

    async def _generate_proposal_v2(
        self,
        idea: str,
        field: str,
        domain: str,
        web_search_result: str,
        level: str,
    ) -> dict:
        total_input_tokens = 0
        total_output_tokens = 0
        try:
            init_time = time.time()
            content = f"""
            Important criteria must satisfy:
            {_criteria_text(level)}
            Target field:
            {field}
            Target domain:
            {domain}
            Web search result:
            {web_search_result}
            Idea:
            {idea}
            User's language:
            {self.language}
            """
            error, success, final_proposal, input_tokens, output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                FINAL_PROPOSAL_PROMPT, 
                content, 
                FinalProposal
            )
            if not success:
                raise error
            original_title = final_proposal.title
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            content = f"""
            Proposal:
            {final_proposal}
            User's language:
            {self.language}
            """
            error, success, final_proposal, input_tokens, output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                TRANSLATE_PROMPT, 
                content, 
                FinalProposal
            )
            if not success:
                raise error
            translate_title = final_proposal.title
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            content = f"""
            Proposal's criteria:
            {_criteria_text(level)}
            Target field:
            {field}
            Target domain:
            {domain}
            Web search result:
            {web_search_result}
            Proposal:
            {final_proposal}
            User's language:
            {self.language}
            """
            error, success, final_proposal_eval, input_tokens, output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                EVAL_PROMPT,
                content,
                _criteria_schema(level)
            )
            if not success:
                raise error
            final_time = time.time()
            logger.info(f"[{self}] Final time: {final_time - init_time}")
            eval_dict = final_proposal_eval.model_dump()
            eval_str = ""
            for count, (key, value) in enumerate(eval_dict.items()):
                eval_str += f"<b>{count + 1}. {LANGUAGE_KIT[_lang_key(self.language)].get(key, key)}:</b> {value}</br>"
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            result_dict = final_proposal.model_dump()
            result_dict["web_search"] = web_search_result
            result_dict["research_gap"] = result_dict["problem_statement"]
            result_dict["problem_statement"] = eval_str
            if result_dict["title"] == "FinalProposal":
                if translate_title == "FinalProposal":
                    if original_title == "FinalProposal":
                        result_dict["title"] == original_title
                    else:
                        del result_dict["title"]
                        for retry in range(3):
                            error, success, title, input_tokens, output_tokens = await get_answer_with_schema(
                                self.id, 
                                self.llm, 
                                GENERATE_TITLE, 
                                str(result_dict), 
                                Title
                            )
                            if not success:
                                if retry == 2:
                                    raise error
                                continue
                            else:
                                if title.title != "FinalProposal":
                                    result_dict["title"] = title.title
                                    break
                else:
                    result_dict["title"] == translate_title
            return result_dict, total_input_tokens, total_output_tokens
        except AIERROR:
            logger.error(f"Fail to generate proposal for {idea}")
            return {}, total_input_tokens, total_output_tokens
    
    async def identify_user_information(self, query: str) -> UserInfo:
        error, success, user_info, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm,
            GET_USER_INFO_PROMPT,
            f"User's query: {query}",
            UserInfo,
        )
        if not success:
            raise error
        else:
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens
            return user_info
    
    async def update_title_without_info(self, query: str, level: str) -> tuple[dict, dict]:
        start = time.time()
        user_info = await self.identify_user_information(query)
        if query not in user_info.subdomains:
            user_info.subdomains.insert(0, query)
        user_info.subdomains = user_info.subdomains[:5]
        tasks = [self._generate_for_subdomain(user_info.field, user_info.domain, subdomain) for subdomain in user_info.subdomains]
        results = await asyncio.gather(*tasks)
        web_search_results = [result[0] for result in results]
        input_tokens = [result[1] for result in results]
        output_tokens = [result[2] for result in results] 
        search_web = [result[3] for result in results]
        self.input_tokens += sum(input_tokens)
        self.output_tokens += sum(output_tokens)
        self.web_search_call += sum(search_web)
        logger.info(f"[{self}] Search web time: {time.time() - start}")
        knowledge_base = [
            {"subdomain": subdomain, "web_search_result": web_search_result} 
            for subdomain, web_search_result in zip(user_info.subdomains, web_search_results)
        ]
        proposal, input_tokens, output_tokens = await self._generate_proposal_v2(
            query, 
            user_info.field, 
            user_info.domain, 
            knowledge_base, 
            level,
        )
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        logger.info(f"[{self}] Update proposal without user info time: {time.time() - start}")
        proposal["title"] = query
        return user_info.model_dump(), proposal
    
    async def mix_titles_v2(
        self, 
        field: str,
        domain: str,
        level: str,
        used_proposals: list[dict],
        num_ideas: int = 5,
    ) -> list[FinalProposal]:
        start = time.time()
        content = f"""
        Important criteria must satisfy:
        {_criteria_text(level)}
        Target field:
        {field}
        Target domain:
        {domain}
        Based on these given proposals:
        {used_proposals}
        Generate {num_ideas} new ideas
        """
        error, success, ideas, input_tokens, output_tokens = await get_answer_with_schema( 
            self.id, 
            self.llm, 
            GENERATE_IDEAS_SOCIAL_SCIENCE_PROMPT_V2, 
            content, 
            Ideas
        )
        if not success:
            raise error
        else:
            idea_time = time.time()
            logger.info(f"[{self}] Idea time for mix titles: {idea_time - start}")
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens
            tasks = [
                self._generate_proposal_v2(
                    idea.idea, 
                    field, 
                    domain, 
                    idea.web_search_result,
                    level, 
                ) for idea in ideas.ideas
            ]
            results = await asyncio.gather(*tasks)
            proposals = [result[0] for result in results]
            input_tokens = [result[1] for result in results]
            output_tokens = [result[2] for result in results]
            self.input_tokens += sum(input_tokens)
            self.output_tokens += sum(output_tokens)
            final_proposals: list[dict] = []
            seen_titles: set = set()
            for proposal in proposals:
                if proposal:
                    if proposal["title"].lower() not in seen_titles and proposal["title"] != "FinalProposal":
                        seen_titles.add(proposal["title"].lower())
                        final_proposals.append(proposal)
            return final_proposals, self.input_tokens, self.output_tokens

    async def get_research_type(self, proposal: dict, field: str, domain: str):
        start_time = time.time()
        for _ in range(5):
            content = f"""
            User's proposal:
            {proposal}
            User's field:
            {field}
            User's domain:
            {domain}
            User's language:
            {self.language}
            """
            error, success, research_type, input_tokens, output_tokens = await get_answer_with_schema(
                self.id, 
                self.llm, 
                RESEARCH_TYPE_PROMPT, 
                content, 
                ResearchType
            )
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens
            if not success:
                raise error
            else:
                if research_type.type <= 2:
                    logger.info(f"[{self}] Research type: {research_type.type}")
                    stop_time = time.time()
                    logger.info(f"[{self}] Research type time: {stop_time - start_time}s")
                    return research_type.model_dump(), self.input_tokens, self.output_tokens
        raise AIERROR(607, "Maximum retry reached for get_research_type")
