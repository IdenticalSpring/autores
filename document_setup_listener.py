from aiokafka.structs import ConsumerRecord
import asyncio
import json
import logging
import time
import traceback
# import urllib.request

from document_setup.src.configs.app import settings
from document_setup.src.modules.search_papers import SearchPapers
from document_setup.src.modules.search_journals import SearchJournal
from document_setup.src.modules.idea_generation import IdeaGeneration
from document_setup.src.modules.ingest_docs import VectorDBProcessor
from document_setup.src.modules.outline_generation import OutlineGeneration

from event_type import DocumentSetupEvent
from error_type import DocumentSetupError, Error, ERROR_DICT, TIME_OUT_DICT, MAX_TOKENS_DICT
from exception_type import AIERROR
from listener import BaseAsyncListener
from producers import get_producer
import topics
from utils import get_admin_vector_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

RESEARCH_TYPE = {
    0: "Qualitative research",
    1: "Quantitative research",
    2: "Mix of Qualitative research and Quantitative research",
}


class DocumentSetupEventListener(BaseAsyncListener):
    def __init__(self, topics, consumer_configs: dict, concurency: int = 100):
        super().__init__(topics, consumer_configs, concurency)
        
    async def _handle_record(self, record: ConsumerRecord):
        value_str = record.value
        value = json.loads(value_str)
        topic = record.topic
        try:
            match topic:
                case topics.DOCUMENT_SETUP_REQUEST:
                    response_topic = topics.DOCUMENT_SETUP_RESPONSE
                    if value["event_type"] in [DocumentSetupEvent.EMBED_USER_DOCUMENTS, DocumentSetupEvent.USER_DOCUMENTS, DocumentSetupEvent.ADMIN_DOCUMENTS, DocumentSetupEvent.USER_GUIDE_DOCUMENTS, DocumentSetupEvent.GENERATE_OUTLINE_WITH_REFS]:
                        await asyncio.wait_for(self._handle_document_setup_event(topics.DOCUMENT_SETUP_RESPONSE, value_str, 3600), timeout=3600)
                    else:
                        await asyncio.wait_for(self._handle_document_setup_event(topics.DOCUMENT_SETUP_RESPONSE, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 300))
                case topics.DOCUMENT_SETUP_REQUEST_LOCAL:
                    response_topic = topics.DOCUMENT_SETUP_RESPONSE_LOCAL
                    if value["event_type"] in [DocumentSetupEvent.EMBED_USER_DOCUMENTS, DocumentSetupEvent.USER_DOCUMENTS, DocumentSetupEvent.ADMIN_DOCUMENTS, DocumentSetupEvent.USER_GUIDE_DOCUMENTS, DocumentSetupEvent.GENERATE_OUTLINE_WITH_REFS]:
                        await asyncio.wait_for(self._handle_document_setup_event(topics.DOCUMENT_SETUP_RESPONSE_LOCAL, value_str, 3600), timeout=3600)
                    else:
                        await asyncio.wait_for(self._handle_document_setup_event(topics.DOCUMENT_SETUP_RESPONSE_LOCAL, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 300))
        except asyncio.TimeoutError:
            producer = await get_producer()
            message = {
                "event_type": value["event_type"],
                "error_code": ERROR_DICT.get(value["event_type"]),
                "error": "Time out",
            }
            if "document_id" in value:
                message.update({"document_id": value["document_id"]})
            if "user_id" in value:
                message.update({"user_id": value["user_id"]})

            if "document_id" in value:
                _id = value["document_id"]
            elif "conv_id" in value:
                _id = value["conv_id"]
            else:
                _id = "admin"
            logger.error(f"[{_id}] Task timed out for document_id: {value['event_type']}")
            await self._finalize_task(None, False, _id, producer, response_topic, message, value["event_type"], True)
                
    async def _handle_document_setup_event(self, topic, value_str, timeout=None):
        start_time = time.time()
        value = json.loads(value_str)
        redis_timer = None
        timeout_time = timeout if timeout is not None else TIME_OUT_DICT.get(value["event_type"], 300)
        if "document_id" in value:
            _id = value["document_id"]
            if value["event_type"] in [DocumentSetupEvent.GENERATE_OUTLINE, DocumentSetupEvent.GENERATE_TITLE_DESCRIPTION, DocumentSetupEvent.MIX_TITLES, DocumentSetupEvent.USER_DOCUMENTS, DocumentSetupEvent.GENERATE_TITLES]:
                ttl = 100
            else:
                ttl = 20
            redis_timer = asyncio.create_task(self.task_timer(_id, value["event_type"], topic, timeout_time, ttl))
        elif "conv_id" in value:
            _id = value["conv_id"]
        else:
            _id = "admin"
        task_success = False
        producer = await get_producer()
        try:
            event_type = value["event_type"]
            message = {"event_type": event_type}
            if "model_id" in value:
                llm_key, db_key, search_web_key = await self.get_key(value["model_id"])
        except KeyError as e:
            message = {"error_code": Error.INVALID_PARAMS, "error": f"Missing field '{str(e)}'"}
            await self._finalize_task(redis_timer, task_success, _id, producer, topic, message, "")
            return
        # if "user_id" in value and "document_id" in value:
        #     if value["user_id"] != "" and value["document_id"] != "":
        #         message.update({
        #             "user_id": value["user_id"],
        #             "document_id": value["document_id"],
        #         })
        #         test_error_message = await self.test_error(value["user_id"], ERROR_DICT.get(value["event_type"]), message)
        #         if test_error_message:
        #             await self._finalize_task(redis_timer, task_success, _id, producer, topic, message, event_type)
        #             return
        if event_type == DocumentSetupEvent.SEARCH_PAPERS:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                language = value["language"]
                document_id = value.get("document_id", "")
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                
                search_range = value.get("search_range", "")
                country_filter = value["country_filter"]
                oa_filter = value["oa_filter"]
                is_tool = value["is_tool"]
                is_oa = value["is_oa"]
                query = value.get("query", "")
                domain = value["domain"]
                subdomains = value.get("subdomains", [])
                field = value["field"]
                proposal = value.get("proposal", {})
                downloaded_papers = value["downloaded_papers"]
                cut_off_year_low = value["cut_off_year_low"]
                cut_off_year_high = value["cut_off_year_high"]
                use_web_search = value.get("use_web_search", False)
                
                # Type validation
                self.field_validate_bool({"is_oa": is_oa, "oa_filter": oa_filter})
                self.field_validate_number({"cut_off_year_low": cut_off_year_low, "cut_off_year_high": cut_off_year_high})
                input_str = {
                    "model_id": model_id,
                    "field": field,
                    "language": language,
                    "domain": domain,
                    "search_range": search_range,
                    "country_filter": country_filter, 
                    "query": query,
                }
                
                self.field_validate_str(input_str)
                if is_tool:        
                    inp = {
                        "model_id": model_id,
                        "country_filter": country_filter, 
                    }
                    
                    self.empty_value_check(inp)
                else:
                    self.field_validate_dict({"proposal": proposal})
                    inp = {
                        "model_id": model_id,
                        "field": field,
                        "domain": domain,
                        "country_filter": country_filter, 
                        "proposal": proposal,
                    }
                    if query.strip():
                        inp.pop("field", None)
                        inp.pop("domain", None)
                        inp.pop("proposal", None)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = SearchPapers(
                        model_id, 
                        document_id, 
                        user_id, 
                        use_web_search, 
                        self.redis, 
                        language, 
                        MAX_TOKENS_DICT.get(event_type, 10000), 
                        llm_key
                    )
                    papers = await module.search_papers(
                        field,
                        domain,
                        subdomains,
                        proposal,
                        downloaded_papers,
                        country_filter,
                        oa_filter,
                        is_oa,
                        cut_off_year_low,
                        cut_off_year_high,
                        search_range,
                        query
                    )
                    message["is_tool"] = is_tool
                    message["papers"] = papers
                    message["input_tokens"] = module.input_tokens
                    message["output_tokens"] = module.output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = module.web_search_call
                    logger.error(f'[{_id}] SEND: search_papers')
                    end = time.time()
                    logger.info(f'[{_id}] search_papers time: {end - start_time}')
                    task_success = True
                except asyncio.TimeoutError:
                    logger.error(f"[{_id}] search_papers task timed out for document_id: {document_id}")
                    message["error_code"] = Error.TIME_OUT_REQUEST
                    message["error"] = f"search_papers task timed out for document_id: {document_id}"
                    
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.SEARCH_PAPER_ERROR
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.SEARCH_USER_DOCUMENTS:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                language = value["language"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                docs = value["documents"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                    "document_id": document_id,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_dict({"docs": docs})
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "document_id": document_id,
                    "docs": docs,
                }
                self.empty_value_check(inp)

            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = VectorDBProcessor(llm_key, db_key, MAX_TOKENS_DICT.get(event_type, 10000), language, document_id, model_id)
                    papers, input_tokens, output_tokens = await module.get_publication_info(docs)

                    message["papers"] = papers
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: search_user_docments')
                    end = time.time()
                    logger.error(f'[{_id}] search_user_docments time: {end - start_time}')
                    task_success = True
                    
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.SEARCH_USER_DOCUMENTS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())                  
        
        elif event_type == DocumentSetupEvent.GENERATE_DOMAINS:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                session_id = value["session_id"]
                document_id = value["document_id"]
                message.update({
                    "user_id": user_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "document_id": document_id,
                    "session_id": session_id,
                })
                field = value["field"]
                language = value["language"]
                domains_num = value["domains_num"]
                is_tool = value["is_tool"]
                
                # Type validation
                self.field_validate_number({"domains_num": domains_num})
                input_str = {
                    "model_id": model_id,
                    "field": field,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "field": field,
                    "language": language,
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    domains, input_tokens, output_tokens = await module.generate_domains(field, domains_num)
                    message["is_tool"] = is_tool
                    message["domains"] = domains["domains"]
                    message["field"] = field
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_domains')
                    end = time.time()
                    logger.error(f'[{_id}] generate_domains time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_DOMAINS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.GENERATE_SUBDOMAINS:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                session_id = value["session_id"]
                document_id = value["document_id"]
                message.update({
                    "user_id": user_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "document_id": document_id,
                    "session_id": session_id,
                })
                field = value["field"]
                language = value["language"]
                domain = value["domain"]
                subdomains_num = value["subdomains_num"]
                is_tool = value["is_tool"]
                
                # Type validation
                self.field_validate_number({"subdomains_num": subdomains_num})
                input_str = {
                    "model_id": model_id,
                    "field": field,
                    "domain": domain,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "field": field,
                    "language": language,
                    "domain": domain,
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    subdomains, input_tokens, output_tokens = await module.generate_subdomains(field, domain, subdomains_num)
                    message["is_tool"] = is_tool
                    message["subdomains"] = subdomains["subdomains"]
                    message["field"] = field
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_subdomains')
                    end = time.time()
                    logger.error(f'[{_id}] generate_subdomains time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_SUBDOMAINS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.GENERATE_KEYWORDS:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                proposal = value["proposal"]
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_dict({"proposal": proposal})
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "proposal": proposal
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    keywords, input_tokens, output_tokens = await module.generate_keywords_v3(proposal)
                    message["keywords"] = keywords
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_keywords')
                    end = time.time()
                    logger.error(f'[{_id}] generate_keywords time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_KEYWORDS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.GENERATE_TITLES:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                field = value["field"]
                domain = value["domain"]
                subdomains = value["subdomains"]
                level = value["level"]
                use_web_search = value.get("use_web_search", False)
                
                # Type validation
                self.field_validate_bool({"use_web_search": use_web_search})
                
                input_str = {
                    "model_id": model_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "level": level,
                }

                self.field_validate_str(input_str)
                self.field_validate_list_str({"subdomains": subdomains})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "subdomains": subdomains,
                    "level": level
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(
                        document_id, 
                        model_id, 
                        language, 
                        llm_key, 
                        MAX_TOKENS_DICT.get(event_type, 10000), 
                        search_web_key, 
                        use_web_search
                    )
                    proposals, knowledge_base = await module.generate_titles_v2(field, domain, subdomains, level)
                    message["proposals"] = proposals
                    message["knowledge_base"] = knowledge_base
                    message["input_tokens"] = module.input_tokens
                    message["output_tokens"] = module.output_tokens
                    message["web_search_call"] = module.web_search_call
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.error(f'[{_id}] SEND: generate_titles')
                    end = time.time()
                    logger.error(f'[{_id}] generate_titles time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_TITLES
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())       

        elif event_type == DocumentSetupEvent.GENERATE_TITLE_DESCRIPTION:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                field = value["field"]
                domain = value["domain"]
                knowledge_base = value["knowledge_base"]
                title = value["title"]
                level = value["level"]
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "title": title,
                    "level": level,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_dict({"knowledge_base": knowledge_base})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "knowledge_base": knowledge_base,
                    "title": title,
                    "level": level
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    proposal = await module.update_title(field, domain, knowledge_base, title, level)
                    message["proposal"] = proposal
                    message["input_tokens"] = module.input_tokens
                    message["output_tokens"] = module.output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_title_description')
                    end = time.time()
                    logger.error(f'[{_id}] generate_title_description time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_TITLE_DESCRIPTION
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.GENERATE_TITLE_DESCRIPTION_NO_INFO:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                use_web_search = value.get("use_web_search", False)
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                title = value["title"]
                level = value["level"]
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                    "title": title,
                    "level": level,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_bool({"use_web_search": use_web_search})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "title": title,
                    "level": level
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000), use_web_search)
                    user_info, proposal = await module.update_title_without_info(title, level)
                    message["field"] = user_info["field"]
                    message["domain"] = user_info["domain"]
                    message["subdomains"] = user_info["subdomains"]
                    message["proposal"] = proposal
                    message["input_tokens"] = module.input_tokens
                    message["output_tokens"] = module.output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_title_description_no_info')
                    end = time.time()
                    logger.error(f'[{_id}] generate_title_description_no_info time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_TITLE_DESCRIPTION_NO_INFO
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())            

        elif event_type == DocumentSetupEvent.MIX_TITLES:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                field = value["field"]
                domain = value["domain"]
                level = value["level"]
                proposals = value["proposals"]
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "level": level,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_dict({"proposals": proposals})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "level": level,
                    "proposals": proposals
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    proposal, input_tokens, output_tokens = await module.mix_titles_v2(field, domain, level, proposals)
                    message["proposal"] = proposal[0]
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = module.web_search_call
                    logger.error(f'[{_id}] SEND: Mix titles')
                    end = time.time()
                    logger.error(f'[{_id}] Mix titles time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.MIX_TITLES
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.MODIFY_OUTLINE:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                field = value["field"]
                domain = value["domain"]
                research_type = value["research_type"]
                headings = value["headings"]
                headings_percent = value["headings_percent"]
                new_headings = value["new_headings"]
                new_headings_percent = value["new_headings_percent"]
                final_proposal = value["final_proposal"]

                # Type validation
                self.field_validate_list_number({"headings_percent": headings_percent, "new_headings_percent": new_headings_percent})

                input_str = {
                    "model_id": model_id,
                    "document_id": document_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_str({"headings": headings, "new_headings": new_headings})
                self.field_validate_dict({"final_proposal": final_proposal, "research_type": research_type})

                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "document_id": document_id,
                    "language": language,
                    "headings": headings,
                    "final_proposal": final_proposal,
                    "field": field,
                    "domain": domain,
                }
                
                self.empty_value_check(inp)

                # Extra condition
                if len(headings) != len(headings_percent):
                    raise ValueError("heading and headings_percent need to have the same length")
        
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = OutlineGeneration(document_id, self.mongo_client, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    new_outline, input_tokens, output_tokens = await module.update_outline_percent(
                        field, 
                        domain, 
                        RESEARCH_TYPE.get(research_type["type"], "Mix of Qualitative research and Quantitative research"),
                        final_proposal, 
                        headings, 
                        headings_percent, 
                        new_headings, 
                        new_headings_percent, 
                    )
                    message["new_outline"] = new_outline
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: modify_outline')
                    end = time.time()
                    logger.error(f'[{_id}] modify_outline time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.MODIFY_OUTLINE
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.GENERATE_OUTLINE:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                field = value["field"]
                domain = value["domain"]
                research_type = value["research_type"]
                word_count_str = value["word_count_str"]
                headings = value["headings"]
                headings_percent = value["headings_percent"]
                final_proposal = value["final_proposal"]
                outline_code = value.get("outline_code", ["" for _ in range(len(headings))])
                
                # Type validation
                self.field_validate_list_number({"headings_percent": headings_percent})
                input_str = {
                    "model_id": model_id,
                    "document_id": document_id,
                    "language": language,
                    "word_count_str": word_count_str,
                    "field": field,
                    "domain": domain,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_dict({"headings": headings})
                self.field_validate_dict({"final_proposal": final_proposal, "research_type": research_type})

                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "document_id": document_id,
                    "language": language,
                    "word_count_str": word_count_str,
                    "headings": headings,
                    "final_proposal": final_proposal,
                    "field": field,
                    "domain": domain,
                }
                
                self.empty_value_check(inp)

                # Extra condition
                if len(headings) != len(headings_percent):
                    raise ValueError("heading and headings_percent need to have the same length")
                if len(outline_code) - len(headings) > 2:
                    raise ValueError("heading and outline_code need to have the same length")
        
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = OutlineGeneration(document_id, self.mongo_client, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    outline, input_tokens, output_tokens = await module.generate_outline_description_v2(
                        field, 
                        domain, 
                        RESEARCH_TYPE.get(research_type["type"], "Mix of Qualitative research and Quantitative research"),
                        word_count_str, 
                        headings, 
                        outline_code,
                        headings_percent, 
                        final_proposal, 
                    )
                    message["outline"] = outline
                    message["final_proposal"] = final_proposal
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_outline')
                    end = time.time()
                    logger.error(f'[{_id}] generate_outline time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_OUTLINE
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.GENERATE_OUTLINE_WITH_REFS:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                language = value["language"]
                headings = value["outline"]
                outline_code = value.get("outline_code", ["" for _ in range(len(headings["outline"]))])
                final_proposal = value["final_proposal"]
                processed_docs = value["processed_docs"]
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "document_id": document_id,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_str({"outline_code": outline_code})
                self.field_validate_list_dict({"processed_docs": processed_docs})
                self.field_validate_dict({"headings": headings, "final_proposal": final_proposal})

                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "document_id": document_id,
                    "language": language,
                    "headings": headings,
                    "final_proposal": final_proposal,
                    "processed_docs": processed_docs,
                }
                
                self.empty_value_check(inp)
                if len(outline_code) - len(headings["outline"]) > 2:
                    raise ValueError("heading and outline_code need to have the same length")
        
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = OutlineGeneration(document_id, self.mongo_client, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    outline, input_tokens, output_tokens, embed_tokens = await module.get_outline_with_refs_v2(
                        headings, 
                        final_proposal, 
                        processed_docs,
                        db_key,
                    )
                    message["outline"] = outline
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = embed_tokens
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: generate_outline_with_refs')
                    end = time.time()
                    logger.error(f'[{_id}] generate_outline_with_refs time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GENERATE_OUTLINE_WITH_REFS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
        
        elif event_type == DocumentSetupEvent.GET_JOURNALS:
            try:
                last_updated = value["last_updated"]
                
                self.field_validate_number({"last_updated": last_updated})
                self.empty_value_check({"last_updated": last_updated})
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = SearchJournal(self.redis)
                    await module.setup(last_updated)
                    message = {"message": "Finish crawling ISSN and Q information"}
                    logger.error(f'[{_id}] SEND: ', message)
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == DocumentSetupEvent.ADMIN_DOCUMENTS:
            try:
                docs = value["documents"]

                # Type validation
                self.field_validate_list_dict({"docs": docs})

                # Empty value checks
                self.empty_value_check({"docs": docs})
                
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = VectorDBProcessor(llm_key, db_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    processed_docs, input_tokens, output_tokens, embeddings_model = await module.ingest_admin_docs(docs, self.mongo_client)
                    message["processed_docs"] = processed_docs
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["model_id"] = module.model_id
                    message["embed_tokens"] = 0
                    message["embed_model"] = embeddings_model
                    logger.error(f'[{_id}] SEND: admin_documents')
                    end = time.time()
                    logger.error(f'[{_id}] admin_documents time: {end - start_time}')
                    
                    task_success = True
                except asyncio.TimeoutError:
                    logger.error(f"[{_id}] Task timed out for event {event_type}")
                    message["error_code"] = Error.TIME_OUT_REQUEST
                    message["error"] = f"Task timed out for event {event_type}"

                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for event {event_type}")
                    message["error_code"] = DocumentSetupError.ADMIN_DOCUMENTS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.USER_GUIDE_DOCUMENTS:
            # Echo identifier từ request để response lỗi (603/604/619) vẫn map được job của BE
            for identifier in ("user_id", "document_id", "session_id", "data_id"):
                if identifier in value:
                    message[identifier] = value[identifier]
            try:
                docs = value["documents_urls"]
                # Type validation
                self.field_validate_list_str({"documents_urls": docs})
                # Format validation: mỗi phần tử bắt buộc là "bucket/key"
                for document_url in docs:
                    bucket, separator, object_name = document_url.partition("/")
                    if not bucket or not separator or not object_name.lstrip("/").strip():
                        raise ValueError(
                            f"documents_urls must be 'bucket/key', got '{document_url}'"
                        )
                # Empty value checks
                self.empty_value_check({"docs": docs})
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = VectorDBProcessor(llm_key, db_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    processed_docs, embed_tokens, embeddings_model = await module.ingest_user_guide_docs(docs)
                    message["processed_docs"] = processed_docs
                    message["input_tokens"] = 0
                    message["output_tokens"] = 0
                    message["embed_tokens"] = embed_tokens
                    message["embed_model"] = embeddings_model
                    message["data_id"] = value["data_id"]
                    logger.error(f'[{_id}] SEND: user_guide_documents')
                    end = time.time()
                    logger.error(f'[{_id}] user_guide_documents time: {end - start_time}')
                    
                    task_success = True
                
                except asyncio.TimeoutError:
                    logger.error(f"[{_id}] {event_type} task timed out for document_id: {document_id}")
                    message["error_code"] = Error.TIME_OUT_REQUEST
                    message["error"] = f"{event_type} task timed out for document_id: {document_id}"

                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {docs} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                        
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {docs} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {docs} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {docs} - event {event_type}")
                    message["error_code"] = DocumentSetupError.USER_GUIDE_DOCUMENTS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.USER_DOCUMENTS:
            try:
                user_id = value["user_id"]
                model_id = value["model_id"]
                document_id = value["document_id"]
                language = value.get("language", "Tiếng Việt")
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "session_id": session_id,
                })
                docs = value["documents"]

                # Type validation
                self.field_validate_str({"model_id": model_id, "language": language})
                self.field_validate_list_dict({"docs": docs})

                # Empty value checks
                self.empty_value_check({"docs": docs, "model_id": model_id, "language": language})
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = VectorDBProcessor(llm_key, db_key, MAX_TOKENS_DICT.get(event_type, 10000), language, document_id, model_id)
                    processed_docs, input_tokens, output_tokens = await module.get_key_points_user_docs(self.mongo_client, docs)
                    message["processed_docs"] = processed_docs
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    message["model_id"] = module.model_id
                    logger.error(f'[{_id}] SEND: user_documents')
                    end = time.time()
                    logger.error(f'[{_id}] user_documents time: {end - start_time}')
                    
                    task_success = True

                except asyncio.TimeoutError:
                    logger.error(f"[{_id}] {event_type} task timed out for document_id: {document_id}")
                    message["error_code"] = Error.TIME_OUT_REQUEST
                    message["error"] = f"{event_type} task timed out for document_id: {document_id}"

                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                     
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.USER_DOCUMENTS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.EMBED_USER_DOCUMENTS:
            try:
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                language = value.get("language", "Tiếng Việt")
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "session_id": session_id,
                })
                docs = value["processed_docs"]

                # Type validation
                self.field_validate_list_dict({"docs": docs})
                self.field_validate_str({"language": language})
                # Empty value checks
                self.empty_value_check({"docs": docs, "language": language})
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = VectorDBProcessor(llm_key, db_key, MAX_TOKENS_DICT.get(event_type, 10000), language, document_id)
                    embed_tokens, embeddings_model = await module.embed_user_docs(self.mongo_client, docs)
                    logger.info("Finish embed")
                    message["input_tokens"] = 0
                    message["output_tokens"] = 0
                    message["embed_tokens"] = embed_tokens
                    message["embed_model"] = embeddings_model
                    logger.error(f'[{_id}] SEND: embed_user_documents')
                    end = time.time()
                    logger.error(f'[{_id}] embed_user_documents time: {end - start_time}')
                    
                    task_success = True

                except asyncio.TimeoutError:
                    logger.error(f"[{_id}] {event_type} task timed out for document_id: {document_id}")
                    message["error_code"] = Error.TIME_OUT_REQUEST
                    message["error"] = f"{event_type} task timed out for document_id: {document_id}"

                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.EMBED_USER_DOCUMENTS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.DELETE_DOCUMENTS:
            try:
                documents = value["documents"]
                collection = value["collection"]

                # Type validation
                self.field_validate_str({"collection": collection})
                self.field_validate_list_dict({"documents": documents})

                # Empty value checks
                self.empty_value_check({"documents": documents, "collection": collection})
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    uuids = [id for doc in documents for id in doc["uuids"]]
                    admin_user_guide = await get_admin_vector_db(db_key) 
                    await admin_user_guide.adelete(ids=uuids)
                    logger.error(f'[{_id}] SEND: delete_documents')
                    end = time.time()
                    logger.error(f'[{_id}] delete_documents time: {end - start_time}')
                    message["data_id"] = value["data_id"]
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {documents} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {documents} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {documents} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {documents} - event {event_type}")
                    message["error_code"] = DocumentSetupError.DELETE_DOCUMENTS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == DocumentSetupEvent.GET_RESEARCH_TYPE:
            try:
                model_id = value["model_id"]
                user_id = value["user_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                language = value["language"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })
                proposal = value["proposal"]
                field = value["field"]
                domain = value["domain"]
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "field": field,
                    "domain": domain,
                    "language": language,
                }                
                self.field_validate_str(input_str)
                self.field_validate_dict({"proposal": proposal})
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "proposal": proposal
                }
                
                self.empty_value_check(inp)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = IdeaGeneration(document_id, model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000))
                    research_type, input_tokens, output_tokens = await module.get_research_type(proposal, field, domain)
                    message["research_type"] = research_type
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    logger.error(f'[{_id}] SEND: get_research_type')
                    end = time.time()
                    logger.error(f'[{_id}] get_research_type time: {end - start_time}')
                    
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.GET_RESEARCH_TYPE
                    message["error"] = str(e)
                    logger.error(traceback.format_exc()) 

        elif event_type == DocumentSetupEvent.UPDATE_USER_REFS:
            try:
                document_id = value["document_id"]
                language = value["language"]   
                model_id = value["model_id"]
                docs = value["documents"] 
                user_id = value["user_id"]
                session_id = value["session_id"]
                message.update({
                    "user_id": user_id,
                    "document_id": document_id,
                    "event_type": event_type,
                    "model_id": model_id,
                    "session_id": session_id,
                })  
                # Type validation
                self.field_validate_str({"document_id": document_id})
                self.field_validate_list_dict({"docs": docs})
                # Empty value checks
                self.empty_value_check({"document_id": document_id})
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    logger.info(f"[{_id}] Start extract and ingest docs for {document_id}")
                    module_update = VectorDBProcessor(llm_key, db_key, MAX_TOKENS_DICT.get(event_type, 10000), language, document_id, model_id)
                    update_embed_tokens = await module_update.update_user_docs(self.mongo_client, docs)
                    update_input_tokens = module_update.input_tokens
                    update_output_tokens = module_update.output_tokens
                    logger.info(f"[{_id}] Start update refs table for {document_id}")
                    module_outline = OutlineGeneration(
                        document_id, 
                        self.mongo_client, 
                        model_id, 
                        language, 
                        llm_key, 
                        MAX_TOKENS_DICT.get(event_type, 10000)
                    )
                    await module_outline.update_user_refs_v2(docs, db_key)
                    logger.info(f"[{_id}] Done update refs table for {document_id}")
                    task_success = True
                    outline_input_tokens = module_outline.input_tokens
                    outline_output_tokens = module_outline.output_tokens
                    outline_embed_tokens = module_outline.embed_tokens
                    message["input_tokens"] = update_input_tokens + outline_input_tokens
                    message["output_tokens"] = update_output_tokens + outline_output_tokens
                    message["embed_tokens"] = update_embed_tokens + outline_embed_tokens
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                    
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = DocumentSetupError.UPDATE_USER_REFS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc()) 
                    
        else:
            logger.info(f"[{_id}] Wrong event_type - event {event_type}")
            message["error_code"] = Error.EVENT_NOT_FOUND
            message["error"] = f"Wrong event_type - event {event_type}"
        await self._finalize_task(redis_timer, task_success, _id, producer, topic, message, event_type)


async def start_listener(topics, group_id):
    consumer_config = {
        "bootstrap_servers": settings.KAFKA,
        "group_id": group_id,
        "auto_offset_reset": 'latest',
        "enable_auto_commit": False,
        "retry_backoff_ms": 300,
        "connections_max_idle_ms": 60000,
        "request_timeout_ms": 60000,
        "session_timeout_ms": 30000,
        "max_poll_interval_ms": 300000,
    }
    listener = DocumentSetupEventListener(topics, consumer_config)
    await listener.start()