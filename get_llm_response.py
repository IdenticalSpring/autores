import asyncio
import json
import httpx
import openai
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, BaseMessage
import logging
from pydantic import BaseModel, Field
from tavily.errors import InvalidAPIKeyError, UsageLimitExceededError, MissingAPIKeyError, BadRequestError
from tavily.async_tavily import AsyncTavilyClient
from datetime import datetime
from typing import List, Union

from exception_type import AIERROR
from error_handle import handle_llm_exception
from utils import get_mongodb_client
from write_reports.src.configs.app import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("openai").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

MAX_OUTPUT_TOKENS_MODEL_CONFIG = {
    "gemini-2.0-flash": 10000,
    "gemini-2.5-pro": 10000,
    "gpt-4.1-mini": 10000,
    "gpt-4.1-nano": 10000,
    "gpt-4.1": 10000,
    "gpt-4.1-2025-04-14": 10000,
    "gpt-5-mini": 10000,
    "gpt-5-nano": 10000,
    "gpt-5": 10000,
    "localhost": 10000,
}

# Pricing dictionary (prices per 1M tokens)
MODEL_PRICING = {
    "gpt-5.1": {"input": 1.25, "cached_input": 0.125, "output": 10.00},
    "gpt-5": {"input": 1.25, "cached_input": 0.125, "output": 10.00},
    "gpt-5-mini": {"input": 0.25, "cached_input": 0.025, "output": 2.00},
    "gpt-5-nano": {"input": 0.05, "cached_input": 0.005, "output": 0.40},
    "gpt-4.1": {"input": 2.00, "cached_input": 0.50, "output": 8.00},
    "gpt-4.1-mini": {"input": 0.40, "cached_input": 0.10, "output": 1.60},
    "gpt-4.1-nano": {"input": 0.10, "cached_input": 0.025, "output": 0.40},
    "gpt-4.1-2025-04-14": {"input": 2.00, "cached_input": 0.50, "output": 8.00},
    "localhost": {"input": 0.0, "cached_input": 0.0, "output": 0.0},
}


def calculate_token_scaling(
    input_tokens: int,
    output_tokens: int,
    target_model: str,
    base_model: str,
    include_cached: bool = False
) -> tuple[int, int]:
    """
    Rescale input and output tokens based on pricing ratios.
    Rescaling factor = (target_model_price / base_model_price)
    - Input: uses 'input' price (or 'cached_input' if specified and available)
    - Output: uses 'output' price
    Reasoning tokens are treated as output tokens.
    Returns rescaled input_tokens and output_tokens.
    """
    target_prices = MODEL_PRICING.get(target_model, {})
    base_prices = MODEL_PRICING.get(base_model, {})
    
    if not target_prices or not base_prices:
        raise ValueError(f"Missing pricing for models: {target_model} or {base_model}")
    
    # Input scaling
    if include_cached and target_prices.get("cached_input") is not None and base_prices.get("cached_input") is not None:
        input_price_target = target_prices["cached_input"]
        input_price_base = base_prices["cached_input"]
    else:
        input_price_target = target_prices["input"]
        input_price_base = base_prices["input"]
    
    input_scale = input_price_target / input_price_base
    rescaled_input = int(input_tokens * input_scale)
    
    # Output scaling (includes reasoning tokens)
    output_price_target = target_prices["output"]
    output_price_base = base_prices["output"]
    output_scale = output_price_target / output_price_base
    rescaled_output = int(output_tokens * output_scale)
    
    return rescaled_input, rescaled_output


def get_llm(
    model_id: str,
    llm_key: str,
    max_output_tokens: int = 10000,
    temperature: float = 0.6,
    reasoning_effort: str = "medium"
) -> ChatGoogleGenerativeAI | ChatOpenAI:
    if "gemini" in model_id:
        llm = ChatGoogleGenerativeAI(model=model_id, google_api_key=llm_key, max_output_tokens=max_output_tokens, temperature=temperature, top_p=0.9)
    elif "gpt" in model_id and not ("oss" in model_id or "groq" in model_id or "local" in model_id):
        if "gpt-5" in model_id:
            llm = ChatOpenAI(
                model=model_id,
                api_key=llm_key,
                max_tokens=max_output_tokens,
                temperature=temperature,
                reasoning_effort=reasoning_effort
            )
        else:
            llm = ChatOpenAI(
                model=model_id,
                api_key=llm_key,
                max_tokens=max_output_tokens,
                temperature=temperature,
                top_p=0.9
            )
    elif model_id == "localhost" or "oss" in model_id or model_id.startswith("groq") or model_id.startswith("local"):
        llm = ChatOpenAI(
            model=model_id if model_id != "localhost" else "localhost",
            api_key="None",
            base_url=settings.LOCALLLM_BASE_URL or "http://localhost:8000/v1",
            max_tokens=max_output_tokens,
            temperature=temperature,
        )
    else:
        raise NotImplementedError(f"Support GPT and Gemini for now. Received model_id: '{model_id}'")
    return llm


def _get_usage_tokens(response) -> tuple[int, int]:
    """Read token usage consistently from raw and normal LangChain responses."""
    raw_response = response.get("raw") if isinstance(response, dict) else response
    usage = getattr(raw_response, "usage_metadata", None) or {}
    return (
        int(usage.get("input_tokens", 0) or 0),
        int(usage.get("output_tokens", 0) or 0),
    )


def _raw_message_text(response) -> str:
    """Lấy text thô từ response `include_raw=True` (ChatOpenAI / ChatGoogleGenerativeAI)."""
    raw = response.get("raw") if isinstance(response, dict) else None
    if raw is None:
        return ""
    content = getattr(raw, "content", None)
    if isinstance(content, list):
        parts = []
        for part in content:
            parts.append(str(part.get("text", "")) if isinstance(part, dict) else str(part))
        content = "".join(parts)
    return content if isinstance(content, str) else ""


def _strip_code_fence(text: str) -> str:
    """Bỏ ```json ... ``` bao ngoài nếu model trả kèm code fence."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
        if cleaned[:4].lower() == "json":
            cleaned = cleaned[4:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
    return cleaned.strip()


def _parse_structured_text(text: str, structure):
    """Parse text thành `structure`; trả None nếu không parse được."""
    cleaned = _strip_code_fence(text or "")
    if not cleaned:
        return None
    try:
        return structure.model_validate_json(cleaned)
    except Exception:
        pass
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if 0 <= start < end:
        try:
            return structure.model_validate_json(cleaned[start:end + 1])
        except Exception:
            return None
    return None


def _parse_structured_fallback(response, structure):
    """Fallback khi `parsed` rỗng: parse JSON thô của model vào schema.

    Nhiều model (đặc biệt khi đi qua LiteLLM) trả JSON đúng nhưng LangChain không
    parse được (model không hỗ trợ response_schema / trả kèm code fence) ⇒ `parsed=None`.
    Hàm này chỉ parse lại **những gì model đã trả**, không bịa dữ liệu.
    """
    return _parse_structured_text(_raw_message_text(response), structure)


def _finish_reason(response) -> str:
    """Lấy finish_reason (length/safety/stop...) để chẩn đoán vì sao output rỗng."""
    raw = response.get("raw") if isinstance(response, dict) else None
    meta = getattr(raw, "response_metadata", None) or {}
    return str(meta.get("finish_reason") or meta.get("finishReason") or "")


async def _plain_json_fallback(llm, messages, structure):
    """Gọi lại model ở chế độ plain-text, yêu cầu trả JSON theo schema rồi parse.

    Dùng cho model/proxy không hỗ trợ `with_structured_output` (parsed luôn None).
    Trả (result|None, input_tokens, output_tokens, raw_text).
    """
    schema_text = json.dumps(structure.model_json_schema(), ensure_ascii=False)
    instruction = HumanMessage(
        content=(
            "Return ONLY valid JSON (no markdown, no code fence, no commentary) "
            "that matches this JSON Schema:\n" + schema_text
        )
    )
    response = await llm.ainvoke(messages + [instruction])
    content = getattr(response, "content", "")
    if isinstance(content, list):
        content = "".join(
            str(part.get("text", "")) if isinstance(part, dict) else str(part) for part in content
        )
    raw_text = content if isinstance(content, str) else ""
    result = _parse_structured_text(raw_text, structure)
    input_tokens, output_tokens = _get_usage_tokens({"raw": response})
    return result, input_tokens, output_tokens, raw_text


async def get_answer_with_schema(
    _id: str,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    system_prompt: str,
    content: Union[str, List[BaseMessage]], 
    structure: type[BaseModel],
) -> tuple[AIERROR | None, bool, str, int, int]:
    structured_llm = llm.with_structured_output(structure, include_raw=True)
    schema_name = getattr(structure, "__name__", str(structure))
    
    # Prepare messages
    messages = []
    
    # Add system message if provided and not empty
    if isinstance(system_prompt, str):
        messages.append(SystemMessage(content=system_prompt))
    else:
        messages.append(system_prompt)
    # Handle content - either string or list of messages
    if isinstance(content, list):
        # If content is already a list of messages, extend the messages list
        messages.extend(content)
    else:
        # If content is a string, wrap it in HumanMessage
        messages.append(HumanMessage(content=content))
    input_tokens = 0
    output_tokens = 0
    status_code = 0
    for attempt in range(3):
        try:
            network_error = False
            response = await structured_llm.ainvoke(input=messages)
            result = response.get("parsed") if isinstance(response, dict) else None
            if result is None:
                # Fallback 1: parse lại raw content (JSON bọc code fence / kèm văn bản)
                result = _parse_structured_fallback(response, structure)
                if result is not None:
                    logger.warning(f"[{_id}] {schema_name}: parsed rỗng, đã parse lại từ raw content")
            if result is None:
                raw_text = _raw_message_text(response)
                finish_reason = _finish_reason(response)
                if attempt == 0:
                    # Fallback 2: gọi plain-text + yêu cầu JSON theo schema (1 lần, tránh nhân chi phí)
                    logger.warning(
                        f"[{_id}] {schema_name}: parsed rỗng (finish_reason='{finish_reason}', "
                        f"raw_len={len(raw_text)}). Thử fallback plain-json..."
                    )
                    fb_result, fb_in, fb_out, fb_text = await _plain_json_fallback(llm, messages, structure)
                    input_tokens += fb_in
                    output_tokens += fb_out
                    if fb_result is not None:
                        logger.warning(f"[{_id}] {schema_name}: fallback plain-json thành công")
                        return None, True, fb_result, input_tokens, output_tokens
                    logger.warning(
                        f"[{_id}] {schema_name}: fallback plain-json cũng rỗng "
                        f"(raw_len={len(fb_text)}, preview={fb_text[:200]!r})"
                    )
                else:
                    logger.warning(
                        f"[{_id}] {schema_name}: parsed rỗng lần {attempt + 1}/3 "
                        f"(finish_reason='{finish_reason}', raw_len={len(raw_text)}, "
                        f"preview={raw_text[:200]!r})"
                    )
                raise ValueError(f"Structured output was empty for schema {schema_name}")
            curr_input_tokens, curr_output_tokens = _get_usage_tokens(response)
            input_tokens += curr_input_tokens
            output_tokens += curr_output_tokens
            return None, True, result, input_tokens, output_tokens
        except httpx.RequestError:
            logger.warning(f"[{_id}] {schema_name} attempt {attempt + 1}/3 failed due to network error. Retrying...")
            network_error = True
            await asyncio.sleep(1)
        except openai.LengthFinishReasonError as e:
            logger.info(f"[{_id}] {e}")
            logger.info(f"[{_id}] {e.completion.choices[0].message.content[-500:]}")
            try:
                logger.info(f"[{_id}] {e.completion.usage}")
                input_tokens += e.completion.usage.prompt_tokens
                output_tokens += e.completion.usage.completion_tokens
            except Exception:
                pass
            logger.warning(f"[{_id}] {schema_name} attempt {attempt + 1}/3 failed due to parsing error. Retrying...")
            await asyncio.sleep(1)
        except Exception as e:
            try:
                logger.info(f"[{_id}] {response}")
            except Exception:
                pass
            logger.info(f"[{_id}] {e}")
            error, status_code = handle_llm_exception(e)
            if status_code in [401, 403, 429]:
                logger.error(f"[{_id}] Unrecoverable error (Status {status_code}). Aborting retries.")
                return error, False, error.message, input_tokens, output_tokens
            logger.warning(f"[{_id}] {schema_name} attempt {attempt + 1}/3 failed with status {status_code}. Retrying...")
            await asyncio.sleep(1)
    if network_error:
        final_error = AIERROR(status_code=600, message="Failed to get an answer after 3 attempts due to network error.")
    else:
        if not status_code:
            final_error = AIERROR(status_code=609, message="AI")
        else:
            final_error = AIERROR(status_code=status_code, message=error.message)

    return final_error, False, final_error.message, input_tokens, output_tokens


async def get_answer_with_websearch(
    _id: str,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    system_prompt: str,
    content: str,
    tavily_key: str,
    language: str,
    websearch_context: str = "",
    document_id: str = "",
    return_source: bool = False,
) -> tuple[AIERROR | None, bool, str, int, int, int]:
    # Step 1: Generate search queries using get_answer_with_schema
    logger.info(f"[{_id}] Generating search queries...")
    query_generation_prompt = """You are a search query generator. 
    Generate 3 diverse and specific search queries that would help find relevant information about the given context. 
    Make queries concise (3-6 words each) and focused on different aspects of the topic.
    If the search context is a direct question (for example: "What is the GRDP of Ho Chi Minh city in 2025", "Who is Leo Messi"), leave it as it is
    """
    total_input_tokens = 0
    total_output_tokens = 0
    websearch_content = f"""Generate 3 search queries for the following search context:
    {websearch_context if websearch_context else content}
    Current date is {datetime.today().strftime('%Y-%m-%d')}"""
    error, success, query, q_input, q_output = await get_answer_with_schema(
        _id=_id,
        llm=llm,
        system_prompt=query_generation_prompt,
        content=websearch_content,
        structure=SearchQueries
    )
    total_input_tokens += q_input
    total_output_tokens += q_output
    websearch_count = 0
    if not success:
        logger.error(
            f"[{_id}] get_answer_with_websearch -> query generation failed: "
            f"status_code={getattr(error, 'status_code', None)}, "
            f"error={getattr(error, 'message', error)}"
        )
        return error, success, "", websearch_count, total_input_tokens, total_output_tokens
    try:
        inp = query.queries if len(query.queries) else [content]
        search_results, search_input, search_output, websearch_count = await search_tavily(_id, llm, inp, tavily_key, language, document_id)
    except Exception as e:
        logger.error(
            f"[{_id}] get_answer_with_websearch -> search_tavily failed: "
            f"status_code={getattr(e, 'status_code', None)}, "
            f"error={getattr(e, 'message', e)}; "
            "returning success=True with empty web-search result",
            exc_info=True,
        )
        return e, True, "", websearch_count, total_input_tokens, total_output_tokens
    total_input_tokens += search_input
    total_output_tokens += search_output
    logger.info(f"[{_id}] Generating final answer...")
    final_system_prompt = system_prompt + "\n\nUse the following search results to provide an accurate and comprehensive answer."
    error, success, answer, input_tokens, output_tokens = await get_answer(_id, llm, content + f"\n\n{search_results}", final_system_prompt)
    total_input_tokens += input_tokens
    total_output_tokens += output_tokens
    if not success:
        logger.error(
            f"[{_id}] get_answer_with_websearch -> final answer generation failed: "
            f"status_code={getattr(error, 'status_code', None)}, "
            f"error={getattr(error, 'message', error)}"
        )
        return error, success, "", websearch_count, total_input_tokens, total_output_tokens
    # if return_source:
    #     answer += f"\n\n{search_results}"
    return error, success, answer, websearch_count, total_input_tokens, total_output_tokens


async def get_answer(
    _id: str,
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    inp: dict | str,
    system_prompt: str | None = None,
) -> tuple[AIERROR | None, bool, str, int, int]:
    # Prepare messages based on whether system prompt is provided
    if system_prompt is not None:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=inp if isinstance(inp, str) else str(inp))
        ]
        invoke_input = messages
    else:
        invoke_input = inp
    network_error_count = 0
    api_error = 0
    network_error = False
    for attempt in range(3):
        try:
            network_error = False
            response = await llm.ainvoke(invoke_input)
            result = response.content
            input_tokens = response.usage_metadata.get("input_tokens", 0)
            output_tokens = response.usage_metadata.get("output_tokens", 0)
            return None, True, result, input_tokens, output_tokens
        except httpx.RequestError:
            logger.warning(f"[{_id}] Attempt {attempt + 1}/3 failed due to network error. Retrying...")
            network_error_count += 1
            network_error = True
            await asyncio.sleep(1)
        except Exception as e:
            logger.info(f"[{_id}] {e}")
            error, status_code = handle_llm_exception(e)
            if status_code in [401, 403, 429]:
                logger.error(f"[{_id}] Unrecoverable error (Status {status_code}). Aborting retries.")
                return error, False, error.message, 0, 0
            elif status_code in [503]:
                api_error += 1
            logger.warning(f"[{_id}] Attempt {attempt + 1}/3 failed with status {status_code}. Retrying...")
            await asyncio.sleep(1)
    if network_error:
        final_error = AIERROR(status_code=600, message="Failed to get an answer after 3 attempts due to network error.")
    else:
        final_error = AIERROR(status_code=500, message="Failed to get an answer after 3 attempts due to api error.")
    return final_error, False, final_error.message, 0, 0


class SearchQueries(BaseModel):
    """Model for generating search queries"""
    queries: list[str] = Field(description="List of 3 search query")


class WebSearchSummary(BaseModel):
    """Model for webpage summary"""
    summary: str = Field(description="Concise summary of the webpage content")
    key_excerpts: str = Field(description="Key excerpts from the webpage")


async def summary_web(_id: str, llm: ChatGoogleGenerativeAI | ChatOpenAI, raw_content: str, language: str):
    summary_prompt = """Summarize the following webpage content concisely. Extract key information and important excerpts.
    **Your response must be ONLY the summary itself, nothing else.**
    """
    content = f"Content:\n{raw_content}\nGenerate in {language}"
    _, success, summary, input_tokens, output_tokens = await get_answer_with_schema(_id, llm, summary_prompt, content, WebSearchSummary)
    if not success:
        return raw_content[:500], input_tokens, output_tokens
    else:
        return f"{summary.summary}\n\nKey Excerpts:\n{summary.key_excerpts}", input_tokens, output_tokens


async def search_tavily(
    _id: str, 
    llm: ChatGoogleGenerativeAI | ChatOpenAI,
    queries: list[str],
    tavily_api_key: str,
    language: str,
    document_id: str,
    max_results: int = 5,
) -> tuple[str, int, int, int]:
    try:
        summarized_results = {}
        total_input_tokens = 0
        total_output_tokens = 0
        if document_id:
            mongo_client = get_mongodb_client()
            admin = mongo_client["admin"]
            search_queries_collection = admin["websearch"]
            document_cache_search_queries = await search_queries_collection.find_one({"_id": document_id})
            if document_cache_search_queries:
                cache_queries = {search["search_query"]: search for search in document_cache_search_queries["queries"]}
                for query in range(len(queries) - 1, -1, -1):
                    query_text = queries[query]
                    if query_text in cache_queries:
                        cached = cache_queries[query_text]["search_result"]
                        for result in cached:
                            summarized_results[result["url"]] = {
                                'query': query_text,
                                "title": result["title"],
                                "summary": result["summary"],
                                "cached": True,
                            }
                        queries.pop(query)
        logger.info(f"[{_id}] Search queries: {queries}")
        tavily_client = AsyncTavilyClient(api_key=tavily_api_key)
        search_tasks = [
            tavily_client.search(
                query,
                max_results=max_results,
                include_raw_content=True,
                topic="general"
            )
            for query in queries
        ]
        
        search_results = await asyncio.gather(*search_tasks)
        
        # Deduplicate by URL
        unique_results = {}
        for response in search_results:
            for result in response.get('results', []):
                url = result.get('url')
                if url and url not in unique_results:
                    unique_results[url] = {**result, "query": response['query']}
        if not unique_results:
            if summarized_results:
                formatted_output = "Search Results:\n\n"
                for i, (url, result) in enumerate(summarized_results.items(), 1):
                    formatted_output += f"\n--- SOURCE {i + 1}: {result['title']} ---\n"
                    formatted_output += f"URL: {url}\n\n"
                    formatted_output += f"{result['summary']}\n"
                    formatted_output += "---" + "\n"
            else:
                formatted_output = ""
            
            return formatted_output, total_input_tokens, total_output_tokens, len(queries)
        
        list_raw_content: list[dict] = []
        for url, result in unique_results.items():
            raw_content = result.get('raw_content', result.get('content', ''))
            if not raw_content:
                summarized_results[url] = {
                    'query': result['query'],
                    'title': result.get('title', 'No title'),
                    'summary': result.get('content', 'No content available'),
                    "cached": False,
                }
            else:
                list_raw_content.append({'query': result['query'], "url": url, "title": result.get("title", "No title"), "raw_content": raw_content})
        
        tasks = [summary_web(_id, llm, content["raw_content"], language) for content in list_raw_content]
        results = await asyncio.gather(*tasks)
        cache_map: dict[str, dict] = {}
        for result, content in zip(results, list_raw_content):
            summarized_results[content["url"]] = {
                'query': content['query'],
                "title": content["title"],
                "summary": result[0],
                "cached": False,
            }
            total_input_tokens += result[1]
            total_output_tokens += result[2]
        if document_id:
            for url, summarized_result in summarized_results.items():
                if summarized_result["cached"] is True:
                    continue 
                query = summarized_result["query"]
                if query not in cache_map:
                    cache_map[query] = {
                        "search_query": query,
                        "search_result": []
                    }

                cache_map[query]["search_result"].append({
                    "url": url,
                    "title": summarized_result["title"],
                    "summary": summarized_result["summary"],
                })
        if cache_map:
            await search_queries_collection.update_one(
                {"_id": document_id},
                {"$push": {"queries": {"$each": list(cache_map.values())}}},
                upsert=True
            )
        formatted_output = "Search Results:\n\n"
        for i, (url, result) in enumerate(summarized_results.items(), 1):
            formatted_output += f"\n--- SOURCE {i + 1}: {result['title']} ---\n"
            formatted_output += f"URL: {url}\n\n"
            formatted_output += f"{result['summary']}\n"
            formatted_output += "---" + "\n"
        return formatted_output, total_input_tokens, total_output_tokens, len(queries)
    except InvalidAPIKeyError:
        raise AIERROR(status_code=701, message="Wrong search API key")
    except UsageLimitExceededError:
        raise AIERROR(status_code=702, message="Search limit reached")
    except MissingAPIKeyError:
        raise AIERROR(status_code=703, message="Missing search api key")
    except BadRequestError:
        raise AIERROR(status_code=704, message="Wrong search body")
    except Exception:
        raise AIERROR(status_code=700, message="Unknown search error")
