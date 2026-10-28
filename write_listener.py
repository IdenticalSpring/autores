import asyncio
from bson import ObjectId
from aiokafka.structs import ConsumerRecord
import json
from langgraph.types import Command
import logging
import time
import traceback
import uuid
import urllib
import urllib.error
import urllib.request

from data_analysis.src.modules.analyzer import Analyzer

from write_reports.src.configs.app import settings
from write_reports.src.modules.translate import Translator
from write_reports.src.modules.edit_report import edit_report, validate_edit_report_input
from data_analysis.src.modules.parsed_value import parse_values, validate_parsed_value_input
from write_reports.src.modules.seminar_graph_1 import get_graph as get_graph_seminar_1
from write_reports.src.modules.seminar_graph_2 import get_graph as get_graph_seminar_2
from write_reports.src.modules.seminar_graph_3 import get_graph as get_graph_seminar_3
from write_reports.src.modules.write_graph import get_graph as get_graph_content
from write_reports.src.modules.gen_slide_md import get_graph as get_graph_gen_slide
from data_analysis.src.modules.proposed_method_v2 import get_graph as get_graph_proposed_method

from event_type import WriteEvent
from error_type import WriteSectionError, Error, ERROR_DICT, TIME_OUT_DICT, MAX_TOKENS_DICT
from exception_type import AIERROR
from listener import BaseAsyncListener
from producers import get_producer
import topics
from utils import get_async_redis_checkpoint

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

RESEARCH_TYPE = {
    0: "dinh_tinh",
    1: "dinh_luong",
    2: "hon_hop",
}


class WriteSectionEventListener(BaseAsyncListener):
    def __init__(self, topics, consumer_configs: dict, concurency: int | None = None):
        super().__init__(topics, consumer_configs, concurency)
        
    async def _handle_record(self, record: ConsumerRecord):
        value_str = record.value
        value = json.loads(value_str)
        topic = record.topic
        try:
            match topic:
                case topics.WRITE_SECTION_REQUEST:
                    response_topic = topics.WRITE_SECTION_RESPONSE
                    await asyncio.wait_for(self._handle_write_section_event(topics.WRITE_SECTION_RESPONSE, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 1800))
                case topics.WRITE_SECTION_REQUEST_LOCAL:
                    response_topic = topics.WRITE_SECTION_RESPONSE_LOCAL
                    await asyncio.wait_for(self._handle_write_section_event(topics.WRITE_SECTION_RESPONSE_LOCAL, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 1800))
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
        except Exception as e:
            # Bug #17 (cùng loại): exception thoát khỏi handler ⇒ BE chỉ thấy timeout,
            # không biết lỗi gì. Luôn publish response lỗi có đủ identifier.
            _id = value.get("document_id") or value.get("conv_id") or "admin"
            logger.error(f"[{_id}] Unhandled error for event {value.get('event_type')}: {e}")
            logger.error(traceback.format_exc())
            try:
                producer = await get_producer()
                message = {
                    "event_type": value.get("event_type", ""),
                    "error_code": ERROR_DICT.get(value.get("event_type"), Error.UNKNOWN),
                    "error": str(e),
                }
                for identifier in ("user_id", "document_id", "session_id", "conv_id"):
                    if identifier in value:
                        message[identifier] = value[identifier]
                response_topic = (
                    topics.WRITE_SECTION_RESPONSE if topic == topics.WRITE_SECTION_REQUEST
                    else topics.WRITE_SECTION_RESPONSE_LOCAL
                )
                await self.send_message(producer, response_topic, message)
            except Exception as publish_error:  # noqa: BLE001
                logger.error(f"Failed to publish error response for {value.get('event_type')}: {publish_error}")

    async def check_interrupt(self, _id, chunk, message, outline_code, writer_graph, thread_id):
        interrupt_message = chunk["__interrupt__"][-1]
        if interrupt_message.value["break_point"] == "analyzer":
            message["need_data_analyze"] = True
            if message.get("section"):
                message["section"]["content"] = ""
            logger.info(f"[{_id}] Interrupt for analyzer after section {message['section']['name']} - {interrupt_message.value['break_point']}") if "section" in message else logger.info(f"[{_id}] Interruptted for analyzer - {interrupt_message}")
        elif interrupt_message.value["break_point"] == "propose_method":
            message["need_propose_method"] = True
            if message.get("section"):
                message["section"]["content"] = ""
            logger.info(f"[{_id}] Interrupt after for method section {message['section']['name']} - {interrupt_message.value['break_point']}") if "section" in message else logger.info(f"[{_id}] Interruptted for method - {interrupt_message}")
        else:
            message["disable_edit"] = interrupt_message.value["disable_edit"]
            logger.info(f"[{_id}] Send section {message['section']['name']}") if "section" in message else logger.info(f"[{_id}] Interruptted - {interrupt_message}")
            final_state = await writer_graph.aget_state({"configurable": {"thread_id": thread_id}})
            if message.get("section"):
                message["section"]["content"] = ""
                if "other_sections" in final_state.values:
                    if len(final_state.values["other_sections"]) != 1:
                        message["change_section"] = True
            else:
                if "other_sections" in final_state.values:
                    if len(final_state.values["other_sections"]) != 1:
                        message["change_section"] = True
                        if message.get("section"):
                            message["section"] = {
                                "content": "",
                                "section_code": final_state.values["other_sections"]["section_code"],
                            }
                    else:
                        if "change_section" in message:
                            del message["change_section"]
                        message["index"] = 1
                        message["section"] = {
                            "content": "",
                            "section_code": outline_code[0],
                        }

    async def send_section(self, _id, chunk, message, write_section_start, producer, topic):
        if "section_synthesizer" in chunk:
            if "other_sections" in chunk["section_synthesizer"] and len(chunk["section_synthesizer"]["other_sections"]):
                message["section"] = chunk["section_synthesizer"]["new_content"]
                message["change_section"] = chunk["section_synthesizer"].get("end_section", False)
                write_section_end = time.time()
                message["index"] = write_section_end
                await self.send_message(producer, topic, message)
        if "add_ref" in chunk:
            if "other_sections" in chunk["add_ref"] and len(chunk["add_ref"]["other_sections"]):
                write_section_end = time.time()
                message["section"] = chunk["add_ref"]["other_sections"][-1]
                message["index"] = write_section_end
                message["change_section"] = chunk["add_ref"]["change_section"]
                logger.info(f"[{_id}] Send section {message['section']['name']}")
                logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                await self.send_message(producer, topic, message)

    async def write_appendices(self, chunk, message, producer, topic):
        write_section_end = time.time()
        message["section"] = chunk["write_appendices"]["other_sections"][-1]
        message["index"] = write_section_end
        message["change_section"] = True
        await self.send_message(producer, topic, message)

    async def _delete_qdrant_collection(self, collection_name: str, _id: str) -> bool:
        """Delete one Qdrant collection (blocking HTTP call runs in a thread).

        Returns True when the collection is deleted or was already absent
        (HTTP 404 - DELETE is idempotent). Returns False on any other failure.
        """

        def _delete() -> bool:
            url = f"{settings.QDRANT_URL}/collections/{collection_name}"
            delete_request = urllib.request.Request(url, method="DELETE")
            try:
                with urllib.request.urlopen(delete_request, timeout=30) as response:
                    if response.status == 200:
                        logger.info(f"[{_id}] Successfully deleted collection: {collection_name}")
                        return True
                    logger.error(f"[{_id}] Error deleting {collection_name}: Status code {response.status}")
                    return False
            except urllib.error.HTTPError as http_error:
                if http_error.code == 404:
                    logger.info(f"[{_id}] Collection already absent: {collection_name}")
                    return True
                logger.error(f"[{_id}] Error deleting {collection_name}: HTTP {http_error.code}")
                return False
            except Exception as delete_error:
                logger.error(f"[{_id}] Error deleting {collection_name}: {delete_error}")
                return False

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _delete)

    async def _handle_write_section_event(self, topic, value_str):
        start_time = time.time()
        value = json.loads(value_str)
        if "document_id" in value:
            _id = value["document_id"]
        elif "conv_id" in value:
            _id = value["conv_id"]
        else:
            _id = "admin"
        if value["event_type"] in [WriteEvent.ANALYZER, WriteEvent.GEN_SLIDES]:
            ttl = 60
        else:
            ttl = 300
        timeout_time = TIME_OUT_DICT.get(value["event_type"], 300)
        redis_timer = asyncio.create_task(self.task_timer(_id, value["event_type"], topic, timeout_time, ttl))
        task_success = False
        producer = await get_producer()
        try:
            user_id = value["user_id"]
            document_id = value.get("document_id", "")
            event_type = value["event_type"]
            session_id = value["session_id"]
            
            message = {
                "user_id": user_id,
                "document_id": document_id,
                "event_type": event_type,
                "session_id": session_id,
            }
            if "model_id" in value:
                llm_key, db_key, search_web_key = await self.get_key(value["model_id"])
        except KeyError as e:
            message = {"error_code": Error.INVALID_PARAMS, "error": f"Missing field '{str(e)}'"}
            await self._finalize_task(redis_timer, task_success, _id, producer, topic, message, event_type)
            return
        # if "user_id" in value and "document_id" in value:
        #     if value["user_id"] != "" and value["document_id"] != "":
        #         message.update({
        #             "user_id": value["user_id"],
        #             "document_id": value["document_id"],
        #         })
        #         test_error_message = await self.test_error(value["user_id"], ERROR_DICT.get(value["event_type"]), message)
        #         if test_error_message:
        #             await self._finalize_task(redis_timer, task_success, _id, producer, topic, message)
        #             return
        if event_type == WriteEvent.WRITE_CONTENT:
            try:
                model_id = value["model_id"]
                language = value["language"]
                final_proposal = value["final_proposal"]
                resume = value.get("resume", False)
                use_web_search = value.get("use_web_search", False)
                resume_error = value.get("resume_error", False)
                resume_method = value.get("resume_method", False)
                resume_chapter = value.get("resumeCharacter", False)
                analyze_log = value.get("analyze_log", "")
                detailed_logs = value.get("detailed_logs", [])
                generated_files = value.get("generated_files", {})
                thread_id = value.get("thread_id", str(uuid.uuid4()))
                field = value["field"]
                domain = value["domain"]
                references_style = value["references_style"]
                papers_search = value["papers_search"]
                outline = value["outline"]
                outline_code = value["outline_code"]
                db = value.get("db", "redis")
                retry = value.get("retry", 2)
                # Type validation
                self.field_validate_number({"retry": retry})
                self.field_validate_bool(
                    {
                        "resume": resume, 
                        "resume_error": resume_error, 
                        "resumeCharacter": resume_chapter, 
                        "resume_method": resume_method,
                        "use_web_search": use_web_search,
                    }
                )
                input_str = {
                    "model_id": model_id,
                    "thread_id": thread_id,
                    "field": field,
                    "domain": domain,
                    "language": language,
                    "analyze_log": analyze_log, 
                    "references_style": references_style,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_str({"outline_code": outline_code})
                self.field_validate_dict({"final_proposal": final_proposal, "outline": outline, "generated_files": generated_files})
                self.field_validate_list_dict({"papers_search": papers_search})
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "thread_id": thread_id,
                    "field": field,
                    "domain": domain,
                    "language": language,
                    "references_style": references_style,
                    "outline_code": outline_code,
                    "final_proposal": final_proposal,
                    "outline": outline,
                    "papers_search": papers_search
                }
                
                self.empty_value_check(inp)
                has_finished = False
                # Extra condition
                if len(outline_code) - len(outline["outline"]) > 2:
                    raise ValueError("outline and outline_code need to have the same length")
                if sum([resume, resume_method, resume_chapter, resume_error]) > 1:
                    raise ValueError("Atmost 1 resume flag is allowed")
                
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:                     
                    has_finished = False
                    message["thread_id"] = thread_id
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f"[{_id}] Loading get redis checkpoint")
                    checkpointer = await get_async_redis_checkpoint()
                    logger.info(f"[{_id}] Successfully get redis checkpoint")
                    writer_graph = await get_graph_content(checkpointer)
                    logger.info(f"[{_id}] Start streaming")
                    write_section_start = time.time()
                    if not (resume or resume_error or resume_method or resume_chapter):
                        await self.clean_up(thread_id, document_id)
                        unused_field = ["final_model", "variables", "hypothesis", "survey_questions", "questions"]
                        for field in unused_field:
                            if field in final_proposal:
                                del final_proposal[field]
                        inp = {
                            "model_id": model_id,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "field": field,
                            "references_style": references_style,
                            "domain": domain,
                            "document_id": document_id,
                            "language": language,
                            "proposal": final_proposal,
                            "research_papers": papers_search,
                            "outline": outline,
                            "outline_code": outline_code,
                            "analyze_log": analyze_log,
                            "generated_files": generated_files,
                            "detailed_logs": detailed_logs,
                            "retry": retry,
                            "use_web_search": use_web_search,
                            "search_key": search_web_key,
                            "user_id": user_id,
                        }
                        async for mode, chunk in writer_graph.astream(input=inp, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": thread_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, thread_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    elif resume_error:
                        async for mode, chunk in writer_graph.astream(None, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": thread_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, thread_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    elif resume_chapter:
                        resume_inp = {
                            "model_id": model_id,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "use_web_search": use_web_search,
                            "search_key": search_web_key,
                            "analyze_log": analyze_log,
                            "detailed_logs": detailed_logs,
                            "generated_files": generated_files,
                        }
                        async for mode, chunk in writer_graph.astream(Command(resume=resume_inp), stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": thread_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, thread_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    
                    elif resume_method:
                        resume_inp = {
                            "model_id": model_id,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "proposal": final_proposal,
                            "analyze_log": analyze_log,
                            "detailed_logs": detailed_logs,
                            "generated_files": generated_files,
                        }
                        async for mode, chunk in writer_graph.astream(Command(resume=resume_inp), stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": thread_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, thread_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    
                    else:
                        resume_inp = {
                            "model_id": model_id,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "analyze_log": analyze_log,
                            "detailed_logs": detailed_logs,
                            "generated_files": generated_files,
                        }
                        async for mode, chunk in writer_graph.astream(Command(resume=resume_inp), stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": thread_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, thread_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    if has_finished:
                        unused_field = ["section", "index", "change_section", "ref_table", "disable_edit"]
                        for field in unused_field:
                            if field in message:
                                del message[field]
                        message["model_id"] = final_state["model_id"]
                        message["input_tokens"] = final_state["input_tokens"]
                        message["output_tokens"] = final_state["output_tokens"]
                        message["embed_uuids"] = final_state["embed_uuids"]
                        message["embed_tokens"] = final_state["embed_tokens"]
                        message["embed_model"] = "text-embedding-3-small"
                        logger.info(f'[{_id}] SEND: WRITE_CONTENT')
                        end = time.time()
                        logger.info(f'[{_id}] WRITE_CONTENT time: {end - start_time}')
                        task_success = True
                        await self.clean_up(thread_id)
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
                    message["error_code"] = WriteSectionError.WRITE_CONTENT
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())           
        
        elif event_type == WriteEvent.ANALYZER or event_type == WriteEvent.ANALYZER_TOOL:
            try:
                model_id = value["model_id"]
                conv_id = value.get("conv_id", "")
                language = value.get("language", "English")
                final_proposal = value.get("final_proposal", {})
                data_dict = value.get("data", {})
                variables_data = value.get("variables", [])
                auto_plan_mode = value.get("auto_plan_mode", True)
                auto_action_mode = value.get("auto_action_mode", True)
                db = value.get("db", "redis")
                # Manual mode parameters
                manual_plan_mode = value.get("manual_plan_mode", False)
                tool_configs = value.get("tool_configs", [])

                resume = value.get("resume", False)
                resume_error = value.get("resume_error", False)
                feedback = value.get("feedback", "")
                thread_id = value.get("thread_id")
                if not thread_id:
                    if resume or resume_error:
                        raise ValueError("thread_id is required when resuming analyzer")
                    thread_id = str(uuid.uuid4())
                get_detailed_report = True if event_type == WriteEvent.ANALYZER else False

                # Type validation
                input_bool = {
                    "resume": resume,
                    "resume_error": resume_error,
                    "auto_plan_mode": auto_plan_mode,
                    "auto_action_mode": auto_action_mode,
                    "manual_plan_mode": manual_plan_mode,
                    "get_detailed_report": get_detailed_report,
                }
                self.field_validate_bool(input_bool)
                input_str = {
                    "document_id": document_id,
                    "model_id": model_id,
                    "conv_id": conv_id,
                    "thread_id": thread_id,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_dict({"final_proposal": final_proposal, "data_dict": data_dict})
                self.field_validate_list_dict({"variables_data": variables_data, "tool_configs": tool_configs})
                
                # Empty value checks
                inp = {
                    "document_id": document_id,
                    "model_id": model_id,
                    "thread_id": thread_id,
                    "language": language,
                    "variables_data": variables_data,
                    "data_dict": data_dict,
                }
                
                self.empty_value_check(inp)
                has_finished = False
                message["is_tool"] = event_type == WriteEvent.ANALYZER_TOOL
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    # Validate manual mode configuration
                    if manual_plan_mode and not tool_configs:
                        logger.warning(f"[{_id}] Manual plan mode enabled but no tool_configs provided. Falling back to interactive mode.")
                        manual_plan_mode = False
                        auto_plan_mode = False
                    if not (resume or resume_error):
                        await self.clean_up(thread_id)
                    module = Analyzer(model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000), self.mongo_client)
                    has_finished, result = await module.run_graph(
                        document_id,
                        thread_id,
                        db,
                        get_detailed_report,
                        resume, 
                        resume_error,
                        feedback,
                        final_proposal,
                        data_dict,
                        variables_data,
                        auto_plan_mode,
                        auto_action_mode,
                        manual_plan_mode,
                        tool_configs,
                    )
                    message["conv_id"] = conv_id
                    message["has_finished"] = has_finished
                    message["thread_id"] = thread_id
                    end = time.time()
                    logger.info(f'[{_id}] ANALYER - Process input time: {end - start_time}')
                    message.update(result)
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f'[{_id}] SEND: ANALYZER')
                    task_success = True
                    if has_finished:
                        await self.clean_up(thread_id)
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
                    message["error_code"] = WriteSectionError.ANALYZER
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == WriteEvent.CHUYEN_DE_1 or event_type == WriteEvent.CHUYEN_DE_2:  
            try:
                model_id = value["model_id"]
                language = value["language"]
                references_style = value["references_style"]
                field = value["field"]
                domain = value["domain"]
                final_proposal = value["final_proposal"]
                papers_search = value["papers_search"]
                outline = value["outline"]
                outline_code = value["outline_code"]
                resume_error = value.get("resume_error", False)
                use_web_search = value.get("use_web_search", False)
                resume_chapter = value.get("resumeCharacter", False)

                retry = value.get("retry", 2)
                # Type validation
                self.field_validate_number({"retry": retry})
                self.field_validate_bool({"resume_error": resume_error, "resumeCharacter": resume_chapter, "use_web_search": use_web_search})
                input_str = {
                    "model_id": model_id,
                    "field": field,
                    "domain": domain,
                    "language": language,
                    "references_style": references_style,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_str({"outline_code": outline_code})
                self.field_validate_dict({"final_proposal": final_proposal, "outline": outline})
                self.field_validate_list_dict({"papers_search": papers_search})
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "field": field,
                    "domain": domain,
                    "language": language,
                    "references_style": references_style,
                    "outline_code": outline_code,
                    "final_proposal": final_proposal,
                    "outline": outline,
                    "papers_search": papers_search
                }
                
                self.empty_value_check(inp)

                # Extra condition
                if len(outline_code) - len(outline["outline"]) > 2:
                    raise ValueError("outline and outline_code need to have the same length")
                if sum([resume_chapter, resume_error]) > 1:
                    raise ValueError("Atmost 1 resume flag is allowed")
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                   
            if "error_code" not in message:
                try:
                    has_finished = False
                    inp = {
                        "model_id": model_id,
                        "llm_key": llm_key,
                        "references_style": references_style,
                        "field": field,
                        "db_key": db_key,
                        "domain": domain,
                        "document_id": document_id,
                        "language": language,
                        "proposal": final_proposal,
                        "research_papers": papers_search,
                        "outline": outline,
                        "outline_code": outline_code,
                        "retry": retry,
                        "use_web_search": use_web_search,
                        "search_key": search_web_key,
                    }
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f"[{_id}] Start streaming")
                    write_section_start = time.time()
                    logger.info(f"[{_id}] Loading get redis checkpoint")
                    checkpointer = await get_async_redis_checkpoint()
                    logger.info(f"[{_id}] Successfully get redis checkpoint")
                    if event_type == WriteEvent.CHUYEN_DE_1:
                        writer_graph = await get_graph_seminar_1(checkpointer)
                    else:
                        writer_graph = await get_graph_seminar_2(checkpointer)
                    has_finished = False
                    if not (resume_error or resume_chapter):
                        await self.clean_up(document_id, document_id)
                        async for mode, chunk in writer_graph.astream(input=inp, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, document_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    elif resume_chapter:
                        resume_inp = {
                            "model_id": model_id,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "use_web_search": use_web_search,
                            "search_key": search_web_key,
                        }
                        async for mode, chunk in writer_graph.astream(Command(resume=resume_inp), stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, document_id)
                                    write_section_end = time.time()
                                    logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                    task_success = True
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    else:
                        async for mode, chunk in writer_graph.astream(None, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                            if mode == "updates":
                                if "__interrupt__" in chunk:
                                    await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, document_id)
                                    write_section_end = time.time()
                                    task_success = True
                                    await self.send_message(producer, topic, message)
                                    break
                                await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                if "write_appendices" in chunk:
                                    await self.write_appendices(chunk, message, producer, topic)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                    if has_finished:
                        task_success = True
                        unused_field = ["section", "index", "change_section", "disable_edit"]
                        for field in unused_field:
                            if field in message:
                                del message[field]
                        message["model_id"] = final_state["model_id"]
                        message["input_tokens"] = final_state["input_tokens"]
                        message["output_tokens"] = final_state["output_tokens"]
                        message["embed_uuids"] = final_state["embed_uuids"]
                        message["embed_tokens"] = final_state["embed_tokens"]
                        message["embed_model"] = "text-embedding-3-small"
                        logger.info(f'[{_id}] SEND: {event_type}')
                        end = time.time()
                        logger.info(f'[{_id}] {event_type} time: {end - start_time}')
                        await self.clean_up(document_id)
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
                    message["error_code"] = WriteSectionError.CHUYEN_DE_1 if event_type == WriteEvent.CHUYEN_DE_1 else WriteSectionError.CHUYEN_DE_2
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == WriteEvent.DELETE_REPORT:
            try:
                document_id = value["document_id"]
                thread_id = value["thread_id"]

                # Type validation
                input_str = {
                    "document_id": document_id,
                    "thread_id": thread_id,
                } 
                self.field_validate_list_str(input_str)
            except KeyError as e:
                message["error_code"] = WriteSectionError.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
            except ValueError as e:
                message["error_code"] = WriteSectionError.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
            if "error_code" not in message:
                try:
                    user_docs_collection = self.mongo_client["user_documents"]
                    user_refs = user_docs_collection["reports_refs"]

                    # Best-effort: xoá lần lượt TẤT CẢ document_id, không dừng giữa đường.
                    failed_ids: list[str] = []
                    for doc_id in document_id:
                        doc_failed = False

                        # 1) Mongo: reports_refs theo từng document
                        try:
                            await user_refs.find_one_and_delete({"_id": doc_id})
                        except Exception as mongo_error:
                            doc_failed = True
                            logger.error(f"[{_id}] Failed to delete reports_refs for {doc_id}: {mongo_error}")

                        # 2) Qdrant: {doc_id} và {doc_id}_user
                        if not await self._delete_qdrant_collection(f"{doc_id}", _id):
                            doc_failed = True
                        if not await self._delete_qdrant_collection(f"{doc_id}_user", _id):
                            doc_failed = True

                        if doc_failed:
                            failed_ids.append(doc_id)

                    # 3) Dọn checkpoint (Redis + BotGraph) cho từng thread_id
                    cleanup_results = await asyncio.gather(
                        *(self.clean_up(thread_id_value) for thread_id_value in thread_id),
                        return_exceptions=True,
                    )
                    for thread_id_value, cleanup_result in zip(thread_id, cleanup_results):
                        if isinstance(cleanup_result, BaseException):
                            logger.error(
                                f"[{_id}] Failed to clean up checkpoint for thread {thread_id_value}: {cleanup_result}"
                            )

                    if failed_ids:
                        message["error_code"] = WriteSectionError.DELETE_REPORT
                        message["failed_ids"] = failed_ids
                        message["error"] = f"Failed to delete data for document ids: {', '.join(failed_ids)}"
                        task_success = False
                        logger.error(f"[{_id}] DELETE_REPORT partial failure: {failed_ids}")
                    else:
                        logger.info(f'[{_id}] SEND: {message}')
                        task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for document_id: {document_id} - event {event_type}")
                    message["error_code"] = WriteSectionError.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = WriteSectionError.DELETE_REPORT
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == WriteEvent.GEN_SLIDES:
            try:
                model_id = value["model_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                user_id = value["user_id"]
                raw_report = value["raw_report"]
                user_preference = value["user_preference"]
                manual_mode = value["manual_mode"]
                language = value.get("language", "English")
                resume_error = value.get("resume_error", False)
                
                # Type validation
                input_str = {
                    "model_id": model_id,
                    "raw_report": raw_report,
                    "language": language,
                }
                self.field_validate_str(input_str)
                self.field_validate_bool({"manual_mode": manual_mode, "resume_error": resume_error})
                self.field_validate_number({"user_preference": user_preference})
                # Empty value check
                inp = {
                    "model_id": model_id,
                    "raw_report": raw_report,
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
                    logger.info(f"[{_id}] Loading get redis checkpoint")
                    checkpointer = await get_async_redis_checkpoint()
                    logger.info(f"[{_id}] Successfully get redis checkpoint")
                    gen_slide = await get_graph_gen_slide(checkpointer)
                    if not resume_error:
                        await self.clean_up(document_id)
                        inp = {
                            "user_id": user_id,
                            "model_id": model_id,
                            "language": language,
                            "document_id": document_id,
                            "llm_key": llm_key,
                            "raw_report": raw_report,
                            "user_preference": user_preference,
                            "manual_mode": manual_mode,
                            "max_tokens": MAX_TOKENS_DICT.get(event_type, 10000),
                        }
                        try:
                            # Send initial progress
                            message["progress"] = 0
                            message["status"] = "Starting..."
                            await self.send_message(producer, topic, message)
                            
                            processed_chunks = 0
                            total_chunks = 0
                            
                            async for mode, chunk in gen_slide.astream(inp, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                                if mode == "updates":
                                    if "fetch_html_content" in chunk:
                                        message["progress"] = 10
                                        message["status"] = "Fetching content..."
                                        await self.send_message(producer, topic, message)
                                        
                                    elif "extract_outlines_and_chunk_content" in chunk:
                                        data = chunk["extract_outlines_and_chunk_content"]
                                        if "chunks" in data:
                                            total_chunks = len(data["chunks"])
                                        message["progress"] = 20
                                        message["status"] = f"Analyzing content ({total_chunks} sections)..."
                                        await self.send_message(producer, topic, message)
                                        
                                    elif "summarize_chunk" in chunk:
                                        processed_chunks += 1
                                        if total_chunks > 0:
                                            progress_step = 60 / total_chunks
                                            current_progress = 20 + int(processed_chunks * progress_step)
                                            message["progress"] = min(current_progress, 80)
                                            message["status"] = f"Summarizing content ({processed_chunks}/{total_chunks})..."
                                            await self.send_message(producer, topic, message)
                                            
                                    elif "consolidate_and_write_slides" in chunk:
                                        message["progress"] = 85
                                        message["status"] = "Generating slides..."
                                        await self.send_message(producer, topic, message)
                                        
                                    elif "post_process_structured_slides" in chunk:
                                        message["progress"] = 90
                                        message["status"] = "Refining slides..."
                                        await self.send_message(producer, topic, message)
                                        
                                    elif "export_presentation" in chunk:
                                        message["progress"] = 95
                                        message["status"] = "Finalizing..."
                                        await self.send_message(producer, topic, message)
                                        
                                elif mode == "values":
                                    slides = chunk
                            
                            task_success = True
                        except Exception as e:
                            raise e
                    else:
                        try:
                            # Resume logic - just stream without explicit progress initialization if complicated, 
                            # or just stream similarly. For now, assuming similar structure.
                            processed_chunks = 0
                            total_chunks = 0 # Might need to recover this from state if possible, but simplest is to just show activity.
                            
                            async for mode, chunk in gen_slide.astream(None, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                                if mode == "updates":
                                    # Simple progress updates for resume
                                    if "summarize_chunk" in chunk:
                                        message["status"] = "Resuming summarization..."
                                        await self.send_message(producer, topic, message)
                                    elif "consolidate_and_write_slides" in chunk:
                                        message["status"] = "Generating slides..."
                                        await self.send_message(producer, topic, message)
                                    elif "export_presentation" in chunk:
                                        message["status"] = "Finalizing..."
                                        await self.send_message(producer, topic, message)
                                elif mode == "values":
                                    slides = chunk
                                    
                            task_success = True
                        except Exception as e:
                            raise e
                    if task_success:
                        message["slides"] = slides["structured_slides"]
                        message["pptx_url"] = slides["pptx_url"]
                        message["input_tokens"] = slides["total_input_tokens"]
                        message["output_tokens"] = slides["total_output_tokens"]
                        message["embed_tokens"] = 0
                        message["embed_model"] = "text-embedding-3-small"
                        message["web_search_call"] = 0
                        message["model_id"] = model_id
                        message["document_id"] = document_id
                        message["user_id"] = user_id
                        message["session_id"] = session_id
                        message["progress"] = 100
                        message["status"] = "Completed"
                        message["has_finished"] = True
                        logger.info(f'[{_id}] SEND: Generate slides')
                        await self.clean_up(document_id)
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
                    message["error_code"] = WriteSectionError.GEN_SLIDES
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
        
        elif event_type == WriteEvent.TRANSLATE_REPORT:
            try:
                model_id = value["model_id"]
                document_id = value["document_id"]
                session_id = value["session_id"]
                user_id = value["user_id"]
                is_outline = value["is_outline"]
                language = value.get("target_language", "English")
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
                }
                self.field_validate_str(input_str)
                self.field_validate_bool({"is_outline": is_outline})
                # Empty value check
                self.empty_value_check(input_str)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    translator = Translator(self.mongo_client, document_id, model_id, llm_key, MAX_TOKENS_DICT.get(event_type, 10000), language)
                    input_tokens, output_tokens = await translator.translate(is_outline)
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    message["web_search_call"] = 0
                    message["model_id"] = model_id
                    message["document_id"] = document_id
                    message["user_id"] = user_id
                    message["session_id"] = session_id
                    logger.info(f'[{_id}] SEND: Generated translation')
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
                    message["error_code"] = WriteSectionError.TRANSLATE_REPORT
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == WriteEvent.CHUYEN_DE_3:
            # write_phase tracks which phase is running so that errors are mapped
            # to the correct error code (analyzer phase -> 631, write phase -> 635).
            # Do NOT reuse has_finished for this: it is reset before the write phase.
            write_phase = False
            try:
                model_id = value["model_id"]
                conv_id = value.get("conv_id", "")
                language = value.get("language", "English")
                final_proposal = value["final_proposal"]
                unused_field = ["variables", "final_model", "hypothesis", "questions", "survey_questions"]
                for field in unused_field:
                    if field in final_proposal:
                        del final_proposal[field]
                data_dict = value.get("data", {})
                variables_data = value.get("variables", [])
                auto_plan_mode = value.get("auto_plan_mode", True)
                auto_action_mode = value.get("auto_action_mode", True)
                # Manual mode parameters
                manual_plan_mode = value.get("manual_plan_mode", False)
                resume_error = value.get("resume_error", False)
                write_resume_error = value.get("write_resume_error", False)
                resume_chapter = value.get("resumeCharacter", False)
                tool_configs = value.get("tool_configs", [])

                resume = value.get("resume", False)
                use_web_search = value.get("use_web_search", False)
                feedback = value.get("feedback", "")
                thread_id = value.get("thread_id", str(uuid.uuid4()))
                get_detailed_report = True

                field = value["field"]
                domain = value["domain"]
                references_style = value["references_style"]
                papers_search = value["papers_search"]
                outline = value["outline"]
                outline_code = value["outline_code"]
                db = value.get("db", "redis")

                retry = value.get("retry", 2)
                # Type validation
                self.field_validate_number({"retry": retry})
                input_bool = {
                    "resume": resume,
                    "resume_error": resume_error,
                    "write_resume_error": write_resume_error,
                    "resumeCharacter": resume_chapter,
                    "auto_plan_mode": auto_plan_mode,
                    "auto_action_mode": auto_action_mode,
                    "manual_plan_mode": manual_plan_mode,
                    "get_detailed_report": get_detailed_report,
                    "use_web_search": use_web_search,
                }
                self.field_validate_bool(input_bool)
                input_str = {
                    "conv_id": conv_id,
                    "model_id": model_id,
                    "thread_id": thread_id,
                    "language": language,
                    "field": field,
                    "domain": domain,
                    "references_style": references_style,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_str({"outline_code": outline_code})
                self.field_validate_dict({"final_proposal": final_proposal, "data_dict": data_dict, "outline": outline})
                self.field_validate_list_dict({"variables_data": variables_data, "tool_configs": tool_configs, "papers_search": papers_search})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "thread_id": thread_id,
                    "language": language,
                    "variables_data": variables_data,
                    "data_dict": data_dict,
                    "field": field,
                    "domain": domain,
                    "references_style": references_style,
                    "outline_code": outline_code,
                    "outline": outline,
                    "final_proposal": final_proposal,
                }
                
                self.empty_value_check(inp)
                if sum([resume, resume_error]) > 1 or sum([resume_chapter, write_resume_error]) > 1:
                    raise ValueError("Atmost 1 resume flag is allowed")
                if len(outline_code) - len(outline["outline"]) > 2:
                    raise ValueError("outline and outline_code need to have the same length")
                has_finished = False
                message["is_tool"] = False
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    admin_db = self.mongo_client["admin"]
                    document_collection = admin_db["document_configurations"]
                    if not (write_resume_error or resume_chapter):
                        if manual_plan_mode and not tool_configs:
                            logger.warning(f"[{_id}] Manual plan mode enabled but no tool_configs provided. Falling back to interactive mode.")
                            manual_plan_mode = False
                            auto_plan_mode = False
                        module = Analyzer(model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000), self.mongo_client)
                        if not (resume or resume_error):
                            await self.clean_up(thread_id)
                        has_finished, result = await module.run_graph(
                            document_id,
                            thread_id,
                            db,
                            get_detailed_report,
                            resume, 
                            resume_error,
                            feedback,
                            final_proposal,
                            data_dict,
                            variables_data,
                            auto_plan_mode,
                            auto_action_mode,
                            manual_plan_mode,
                            tool_configs,
                        )
                        message["conv_id"] = conv_id
                        message["thread_id"] = thread_id
                        end = time.time()
                        logger.info(f'[{_id}] CHUYEN_DE_3 - Process input time: {end - start_time}')
                        message.update(result)
                        message["has_finished"] = has_finished
                        if has_finished:
                            await document_collection.update_one(
                                {"documentId": document_id},
                                {
                                    "$set": {
                                        "detailedLogs": result["detailed_logs"],
                                        "analyzeLog": result["analyze_log"],
                                        "generatedFiles": result["generated_files"]
                                    }
                                },
                                upsert=True
                            )
                        task_success = True
                        logger.info(f'[{_id}] SEND: CHUYEN_DE_3 - Process input')
                    if has_finished or write_resume_error or resume_chapter:
                        if has_finished:
                            unused_field = [
                                "analyze_log",
                                "generated_files",
                                "current_variables",
                                "detailed_logs",
                                "result_html",
                                # upload_failed chỉ có nghĩa khi result_html được trả về;
                                # với chuyen_de_3 các field phân tích đã bị bỏ => bỏ luôn cờ này.
                                "upload_failed",
                            ]
                            for field in unused_field:
                                if field in message:
                                    del message[field]
                            await self.clean_up(thread_id)                 
                            analyze_log = result["analyze_log"]
                            generated_files = result["generated_files"]
                            detailed_logs = result["detailed_logs"]
                        else:
                            doc_config = await document_collection.find_one({"documentId": document_id})
                            analyze_log = doc_config.get("analyzeLog", "")
                            detailed_logs = doc_config.get("detailedLogs", [])
                            generated_files = doc_config.get("generatedFiles", {})
                        start_write = time.time()
                        write_phase = True
                        has_finished = False
                        task_success = False
                        inp = {
                            "model_id": model_id,
                            "field": field,
                            "references_style": references_style,
                            "domain": domain,
                            "document_id": document_id,
                            "language": language,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "proposal": final_proposal,
                            "research_papers": papers_search,
                            "outline": outline,
                            "generated_files": generated_files,
                            "analyze_log": analyze_log,
                            "detailed_logs": detailed_logs,
                            "outline_code": outline_code,
                            "retry": retry,
                            "use_web_search": use_web_search,
                            "search_key": search_web_key,
                            "user_id": user_id,
                        }
                        logger.info(f"[{_id}] Start streaming")
                        write_section_start = time.time()
                        logger.info(f"[{_id}] Loading get redis checkpoint")
                        checkpointer = await get_async_redis_checkpoint()
                        logger.info(f"[{_id}] Successfully get redis checkpoint")
                        writer_graph = await get_graph_seminar_3(checkpointer)
                        if not (write_resume_error or resume_chapter):
                            await self.clean_up(document_id, document_id)
                            async for mode, chunk in writer_graph.astream(input=inp, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                                if mode == "updates":
                                    if "__interrupt__" in chunk:
                                        await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, document_id)
                                        write_section_end = time.time() 
                                        logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                        task_success = True
                                        break
                                    await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                    if "write_appendices" in chunk:
                                        await self.write_appendices(chunk, message, producer, topic)
                                        has_finished = True
                                elif mode == "values":
                                    final_state = chunk
                        elif resume_chapter:
                            resume_inp = {
                                "model_id": model_id,
                                "llm_key": llm_key,
                                "db_key": db_key,
                                "use_web_search": use_web_search,
                                "search_key": search_web_key,
                            }
                            async for mode, chunk in writer_graph.astream(Command(resume=resume_inp), stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                                if mode == "updates":
                                    if "__interrupt__" in chunk:
                                        await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, document_id)
                                        write_section_end = time.time()
                                        logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                        task_success = True
                                        break
                                    await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                    if "write_appendices" in chunk:
                                        await self.write_appendices(chunk, message, producer, topic)
                                        has_finished = True
                                elif mode == "values":
                                    final_state = chunk
                        else:
                            async for mode, chunk in writer_graph.astream(None, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                                if mode == "updates":
                                    if "__interrupt__" in chunk:
                                        await self.check_interrupt(_id, chunk, message, outline_code, writer_graph, document_id)
                                        write_section_end = time.time()
                                        logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                        task_success = True
                                        break
                                    await self.send_section(_id, chunk, message, write_section_start, producer, topic)
                                    if "write_appendices" in chunk:
                                        await self.write_appendices(chunk, message, producer, topic)
                                        has_finished = True
                                elif mode == "values":
                                    final_state = chunk
                        if has_finished:
                            task_success = True
                            unused_field = ["section", "index", "change_section", "disable_edit"]
                            for field in unused_field:
                                if field in message:
                                    del message[field]
                            message["model_id"] = final_state["model_id"]
                            message["input_tokens"] = final_state["input_tokens"]
                            message["output_tokens"] = final_state["output_tokens"]
                            message["embed_uuids"] = final_state["embed_uuids"]
                            message["embed_tokens"] = final_state["embed_tokens"]
                            message["embed_model"] = "text-embedding-3-small"
                            logger.info(f'[{_id}] SEND: CHUYEN_DE_3 - Write')
                            end_write = time.time()
                            logger.info(f'[{_id}] CHUYEN_DE_3 - Write time: {end_write - start_write}')
                            await self.clean_up(document_id)
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
                    message["error_code"] = WriteSectionError.CHUYEN_DE_3 if write_phase else WriteSectionError.ANALYZER
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
        elif event_type == WriteEvent.COMMENT_DATA:
            try:
                model_id = value["model_id"]
                language = value.get("language", "English")
                final_proposal = value.get("final_proposal", {})
                data_dict = value.get("data", {})
                variables_data = value.get("variables", [])

                # Manual mode parameters
                thread_id = value.get("thread_id")

                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_dict({"final_proposal": final_proposal, "data_dict": data_dict})
                self.field_validate_list_dict({"variables_data": variables_data})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "variables_data": variables_data,
                    "data_dict": data_dict,
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
                    module = Analyzer(model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000), self.mongo_client)
                    comments, input_tokens, output_tokens = await module.check_variable_conditions(
                        data_dict,
                        variables_data,
                        final_proposal,
                    )
                    end = time.time()
                    logger.info(f'[{_id}] ANALYZER_COMMENT - Process input time: {end - start_time}')
                    message["comments"] = [comment.model_dump() for comment in comments]
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f'[{_id}] SEND: ANALYZER_COMMENT')
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
                    message["error_code"] = WriteSectionError.ANALYZER
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == WriteEvent.CHECK_LOGIC_DATA:
            try:
                model_id = value["model_id"]
                language = value.get("language", "English")
                variables_data = value.get("variables", [])

                # Type validation
                input_str = {
                    "model_id": model_id,
                    "language": language,
                }
                
                self.field_validate_str(input_str)
                self.field_validate_list_dict({"variables_data": variables_data})
                
                # Empty value checks
                inp = {
                    "model_id": model_id,
                    "language": language,
                    "variables_data": variables_data,
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
                    module = Analyzer(model_id, language, llm_key, MAX_TOKENS_DICT.get(event_type, 10000), self.mongo_client)
                    result = await module.check_variable_logic(variables_data)
                    end = time.time()
                    logger.info(f'[{_id}] ANALYZER_CHECK_LOGIC_DATA - Process input time: {end - start_time}')
                    message["errors_logic"] = result["errors"]
                    message["warnings_logic"] = result["warnings"]
                    message["input_tokens"] = 0
                    message["output_tokens"] = 0
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f'[{_id}] SEND: ANALYZER_CHECK_LOGIC_DATA')
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
                    message["error_code"] = WriteSectionError.CHECK_LOGIC_DATA
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
            
        elif event_type == WriteEvent.PROPOSE_METHOD:
            try:
                user_input = value.get("user_input", "")
                model_id = value["model_id"]
                user_id = value["user_id"]
                conv_id = value.get("conv_id", "")
                thread_id = value["thread_id"]
                resume_method = value.get("resume_method", False)
                resume_error = value.get("resume_error", False)
                inherit_mode = value.get("inherit_mode", False)
                auto_mode = value.get("auto_mode", False)
                language = value.get("language", "Vietnamese")
                research_type = value.get("research_type", {})
                final_proposal = value.get("final_proposal", {})
                db = value.get("db", "redis")

                self.field_validate_str({
                    "user_input": user_input,
                    "conv_id": conv_id,
                    "user_id": user_id,
                    "llm_key": llm_key,
                    "language": language,
                    "model_id": model_id,
                    "thread_id": thread_id
                })
                self.field_validate_bool(
                    {
                        "resume_error": resume_error, 
                        "resume_method": resume_method, 
                        "auto_mode": auto_mode,
                    }
                )
                self.field_validate_dict({"research_type": research_type, "final_proposal": final_proposal})
                self.empty_value_check({
                    "user_id": user_id,
                    "language": language,
                    "model_id": model_id,
                    "thread_id": thread_id,
                    "research_type": research_type,
                })
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.info(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    admin_db = self.mongo_client["admin"]
                    article_collection = admin_db["articles"]
                    article = await article_collection.find_one({"_id": ObjectId(document_id)})
                    title_id = article["title"]
                    proposal_collection = admin_db["proposal_titles"]
                    proposal = await proposal_collection.find_one({"_id": ObjectId(title_id)})
                    document_collection = admin_db["document_configurations"]
                    doc_config = await document_collection.find_one({"finalProposal": ObjectId(title_id)})
                    RESEARCH_TYPE_ENG = {
                        0: "Qualitative Research",
                        1: "Quantitative Research",
                        2: "Mix of Qualitative Research and Quantitative Research",
                    }
                    proposal_str = f"""
                        My report's title: {proposal.get("title", "")}
                        My field: {article["docsPrepare"]["field"]}
                        My domain: {article["docsPrepare"]["domain"]}
                        My subdomains: {article["docsPrepare"]["subDomain"]}
                        My main keywords: {doc_config["keywords"]["mainKeywords"]}
                        My supplement keywords: {doc_config["keywords"]["subKeywords"]}
                        My references style: {doc_config["referencesStyle"]}
                        My research type: {RESEARCH_TYPE_ENG[doc_config["researchType"]["type"]]}
                        My research type's reason: {doc_config["researchType"]["reason"]}
                        My problem's statement: {proposal.get("problemStatement", "")}
                        My motivation: {proposal.get("motivation", "")}
                        My web search: {proposal.get("webSearch", "")}
                        My research gap: {proposal.get("researchGap", "")}
                    """
                    if inherit_mode:
                        inherit_context = {
                            "final_assumptions": proposal.get("hypothesis", ""),
                            "final_variables": proposal.get("variables", ""),
                            "final_model": proposal.get("finalModel", ""),
                            "final_surveys": proposal.get("surveyQuestions", ""),
                            "final_questions": proposal.get("questions", ""),
                        }
                    else:
                        inherit_context = {}
                    inp = {
                        "model_id": model_id,
                        "llm_key": llm_key,
                        "research_type": RESEARCH_TYPE.get(research_type["type"], "dinh_tinh"),
                        "research_context": proposal_str,
                        "language": language,
                        "auto_mode": auto_mode,
                        "document_id": document_id,
                        "final_model_count": 0,
                        "max_tokens": MAX_TOKENS_DICT.get(event_type, 10000),
                        "inherit_mode": inherit_mode,
                        "inherit_context": inherit_context,
                    }
                    message["conv_id"] = conv_id
                    message["thread_id"] = thread_id
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    is_done = True
                    retry = 0
                    logger.info(f"[{_id}] Loading get redis checkpoint")
                    checkpointer = await get_async_redis_checkpoint()
                    logger.info(f"[{_id}] Successfully get redis checkpoint")
                    while retry < 2 and not task_success:
                        writer_graph = await get_graph_proposed_method(checkpointer)
                        if not (resume_method or resume_error):
                            await self.clean_up(thread_id)
                            graph_output = await writer_graph.ainvoke(
                                input=inp, 
                                config={
                                    "recursion_limit": 1000, "thread_id": thread_id
                                }
                            )
                            interruptted = graph_output.get("__interrupt__")
                            if interruptted:
                                interrupt_message = interruptted[-1].value
                                logger.info(f"[{_id}] {interrupt_message}")
                                message["ai_message"] = interrupt_message
                                is_done = False
                            task_success = True
                        elif resume_error:
                            graph_output = await writer_graph.ainvoke(
                                None, 
                                config={
                                    "recursion_limit": 1000, "thread_id": thread_id
                                }
                            )
                            interruptted = graph_output.get("__interrupt__")
                            if interruptted:
                                interrupt_message = interruptted[-1].value
                                logger.info(f"[{_id}] {interrupt_message}")
                                message["ai_message"] = interrupt_message
                                is_done = False
                            task_success = True
                        else:
                            resume_inp = {
                                "model_id": model_id,
                                "llm_key": llm_key,
                                "user_input": user_input,
                            }
                            graph_output = await writer_graph.ainvoke(
                                Command(resume=resume_inp), 
                                config={
                                    "recursion_limit": 1000, "thread_id": thread_id
                                }
                            )
                            interruptted = graph_output.get("__interrupt__")
                            if interruptted:
                                interrupt_message = interruptted[-1].value
                                logger.info(f"[{_id}] {interrupt_message}")
                                message["ai_message"] = interrupt_message
                                is_done = False
                            task_success = True
                    if task_success:
                        final_state = await writer_graph.aget_state({"configurable": {"thread_id": thread_id}})
                        message["final_proposal"] = final_proposal
                        message["input_tokens"] = final_state.values["input_tokens"]
                        message["output_tokens"] = final_state.values["output_tokens"]
                        message["has_finished"] = is_done
                        if is_done:
                            if "elements" in final_state.values:
                                final_result = final_state.values["elements"][-1]["object"]
                                final_surveys_list: list[dict] = []
                                for survey in final_result.final_surveys:
                                    survey_dict = {
                                        "variable_name": survey.variable_name,
                                        "variable_code": survey.variable_code,
                                        "variable_type": survey.variable_type,
                                        "measurement_type": survey.measurement_type,
                                        "questions": [question.model_dump() for question in survey.questions],
                                        "source": survey.source if survey.source else "",
                                    }
                                    final_surveys_list.append(survey_dict)
                                final_proposal.update({
                                    "final_model": final_result.final_model.model_dump() if final_result.final_model is not None else {},
                                    "hypothesis": [assumption.model_dump() for assumption in final_result.final_assumptions],
                                    "variables": [variable.model_dump() for variable in final_result.final_variables],
                                    "survey_questions": final_surveys_list,
                                    "parsed_variables": final_result.parsed_variables,
                                    "questions": final_result.final_questions,
                                })
                            else:
                                final_proposal.update({
                                    "final_model": proposal.get("finalModel", ""),
                                    "hypothesis": proposal.get("hypothesis", ""),
                                    "variables": proposal.get("variables", ""),
                                    "survey_questions": proposal.get("surveyQuestions", ""),
                                    "parsed_variables": proposal.get("parsedVariables", ""),
                                    "questions": proposal.get("questions", ""),
                                })
                            message["final_proposal"] = final_proposal
                            await self.clean_up(thread_id)                              
                    else:
                        logger.error(f"[{_id}] {event_type} task timed out for document_id: {thread_id}")
                        message["error_code"] = Error.TIME_OUT_REQUEST
                        message["error"] = f"{event_type} task timed out for document_id: {thread_id}"
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
                    message["error_code"] = WriteSectionError.PROPOSE_METHOD
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
        elif event_type == WriteEvent.EDIT_REPORT:
            try:
                model_id = value["model_id"]
                language = value.get("language", "Vietnamese")
                content = value["content"]
                instruction = value["instruction"]
                section_name = value.get("section_name", "")
                edit_context = value.get("context", "")
                self.field_validate_str({"model_id": model_id, "language": language})
                validate_edit_report_input(content, instruction, section_name, edit_context)
                message["section_name"] = section_name
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
            if "error_code" not in message:
                try:
                    edited, input_tokens, output_tokens = await edit_report(
                        _id, model_id, llm_key, MAX_TOKENS_DICT.get(event_type, 5000),
                        language, content, instruction, section_name, edit_context,
                    )
                    message["content"] = edited
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f"[{_id}] SEND: EDIT_REPORT")
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                except NotImplementedError:
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = WriteSectionError.EDIT_REPORT
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
        elif event_type == WriteEvent.PARSED_VALUE:
            try:
                model_id = value["model_id"]
                language = value.get("language", "Vietnamese")
                survey_questions = value["survey_questions"]
                variables_data = value.get("variables", [])
                self.field_validate_str({"model_id": model_id, "language": language})
                validate_parsed_value_input(survey_questions, variables_data)
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
            if "error_code" not in message:
                try:
                    parsed_variables, input_tokens, output_tokens = await parse_values(
                        _id, model_id, llm_key, MAX_TOKENS_DICT.get(event_type, 5000),
                        language, variables_data, survey_questions,
                    )
                    message["parsed_variables"] = parsed_variables
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    logger.info(f"[{_id}] SEND: PARSED_VALUE")
                    task_success = True
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                except NotImplementedError:
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for document_id: {document_id} - event {event_type}"
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for document_id: {document_id} - event {event_type}")
                    message["error_code"] = WriteSectionError.PARSED_VALUE
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
    listener = WriteSectionEventListener(topics, consumer_config)
    await listener.start()
