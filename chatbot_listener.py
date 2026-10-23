import asyncio
from aiokafka.structs import ConsumerRecord
import json
from langgraph.types import Command
import logging
import time
import traceback

from ai_chatbot.src.configs.app import settings
from ai_chatbot.src.modules.chatbot import Chatbot
from write_reports.src.modules.write_chat_graph import get_graph as writer_bot
from ai_chatbot.src.modules.writer_chatbot import get_graph

from event_type import AIEvent
from error_type import ChatbotError, Error, ERROR_DICT, TIME_OUT_DICT, MAX_TOKENS_DICT
from exception_type import AIERROR
from listener import BaseAsyncListener
from producers import get_producer
from status_type import AIStatus
import topics
from utils import get_async_redis_checkpoint, send_health_check
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

        
class ChatbotEventListener(BaseAsyncListener):
    def __init__(self, topics, consumer_configs: dict, concurency: int = 100):
        super().__init__(topics, consumer_configs, concurency)
        
    async def _handle_record(self, record: ConsumerRecord):
        value_str = record.value
        value = json.loads(value_str)
        topic = record.topic
        try:
            match topic:
                case topics.CHATBOT_REQUEST:
                    response_topic = topics.CHATBOT_RESPONSE
                    await asyncio.wait_for(self._handle_chatbot_event(topics.CHATBOT_RESPONSE, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 300))
                case topics.CHATBOT_REQUEST_LOCAL:
                    response_topic = topics.CHATBOT_RESPONSE_LOCAL
                    await asyncio.wait_for(self._handle_chatbot_event(topics.CHATBOT_RESPONSE_LOCAL, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 300))
        except asyncio.TimeoutError:
            producer = await get_producer()
            message = {
                "event_type": value["event_type"],
                "error_code": ERROR_DICT.get(value["event_type"]),
                "error": "Time out",
            }
            if "document_id" in value:
                message.update({"document_id": value["document_id"]})
            if "conv_id" in value:
                message.update({"conv_id": value["conv_id"]})
            if "user_id" in value:
                message.update({"user_id": value["user_id"]})

            if "document_id" in value:
                _id = value["document_id"]
            elif "conv_id" in value:
                _id = value["conv_id"]
            else:
                _id = "admin"
            logger.error(f"[{_id}] Task timed out for event: {value["event_type"]}")
            await self._finalize_task(None, False, _id, producer, response_topic, message, value["event_type"], True)

    async def _handle_chatbot_event(self, topic, value_str):
        start_time = time.time()
        value = json.loads(value_str)
        producer = await get_producer()
        if value["event_type"] == AIEvent.WRITER:
            _id = value["document_id"]
            await send_health_check(id=_id, status=AIStatus.PROCESSING)
        else:
            _id = value["conv_id"]
        
        try:
            event_type = value["event_type"]
            model_id = value["model_id"]
            language = value["language"]
            message = {
                "model_id": model_id,
                "event_type": event_type,
            }
            llm_key, db_key, search_web_key = await self.get_key(model_id)
        except KeyError as e:
            message = {"error_code": Error.INVALID_PARAMS, "error": f"Missing field '{str(e)}'"}
            # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
            await self.send_message(producer, topic, message)
            return
        # if "user_id" in value and "document_id" in value:
        #     if value["user_id"] != "" and value["document_id"] != "":
        #         message.update({
        #             "user_id": value["user_id"],
        #             "document_id": value["document_id"],
        #         })
        #         test_error_message = await self.test_error(value["user_id"], ERROR_DICT.get(value["event_type"]), message)
        #         if test_error_message:
        #             await self.send_message(producer, topic, message)
        #             return
        
        if event_type == AIEvent.OUTER_CHATBOT:
            try:
                # Extract required fields
                conv_id = value["conv_id"]
                question = value["question"]
                user_id = value["user_id"]
                chat_history = value["chat_history"]
                
                # Extract optional fields
                short_answer = value.get("short_answer", False)
                direct_answer = value.get("direct_answer", False)
                use_web_search = value.get("use_web_search_chatbot", False)
                
                # Validate required string fields
                self.field_validate_str({
                    "conv_id": conv_id,
                    "question": question,
                    "llm_key": llm_key,
                    "db_key": db_key,
                    "user_id": user_id,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate empty values for required fields
                self.empty_value_check({
                    "conv_id": conv_id,
                    "question": question,
                    "user_id": user_id,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate boolean fields
                self.field_validate_bool({
                    "short_answer": short_answer,
                    "direct_answer": direct_answer,
                    "use_web_search": use_web_search
                })
                
                # Validate chat_history is a list
                self.field_validate_list_dict({"chat_history": chat_history})
                
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                await self.send_message(producer, topic, message)
                return
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                logger.error(traceback.format_exc())
                await self.send_message(producer, topic, message)
                return

            if "error_code" not in message:
                try:
                    # Set conv_id and user_id at the start to ensure they are included in error messages
                    message["conv_id"] = conv_id
                    message["user_id"] = user_id

                    # admin_user_guide = await get_admin_vector_db(db_key)
                    module = Chatbot(
                        language,
                        user_id,
                        model_id,
                        llm_key,
                        db_key,
                        MAX_TOKENS_DICT.get(event_type, 10000),
                        conv_id,
                        short_answer,
                        use_web_search,
                        self.mongo_client,
                        self.redis,
                        search_web_key,
                    )
                    await self.clean_up(conv_id)
                    response, input_tokens, output_tokens, embed_tokens, web_search_call = await module.get_answer(
                        question,
                        chat_history,
                        direct_answer,
                    )

                    logger.info(f"[{_id}] Answer: {response}")

                    message.update({
                        "response": response,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "embed_tokens": embed_tokens,
                        "embed_model": getattr(module.embeddings, "model_name", getattr(module.embeddings, "model", "BAAI/bge-small-en-v1.5")) if isinstance(getattr(module.embeddings, "model_name", None), str) else getattr(module.embeddings, "model", "text-embedding-3-small"),
                        "web_search_call": web_search_call,
                        "functionality_error": response == "Function does not support",
                    })
                    
                    end_time = time.time()
                    logger.info(f'[{_id}] SEND: {message}')
                    logger.info(f'[{_id}] Response time: {end_time - start_time}')
                    
                    await self.send_message(producer, topic, message)
                    # await send_health_check(id=_id, status=AIStatus.FINISHED)
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    logger.error(traceback.format_exc())
                    await self.send_message(producer, topic, message)
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}"
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    await self.send_message(producer, topic, message)
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = ChatbotError.OUTER_CHATBOT
                    message["error"] = str(e)
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    logger.error(traceback.format_exc())
                    await self.send_message(producer, topic, message)
        elif event_type == AIEvent.INNER_CHATBOT:
            try:
                # Extract required fields
                document_id = value["document_id"]
                conv_id = value["conv_id"]
                question = value["question"]
                user_id = value["user_id"]
                chat_history = value["chat_history"]
                
                # Extract optional fields
                short_answer = value.get("short_answer", False)
                direct_answer = value.get("direct_answer", False)
                use_web_search = value.get("use_web_search_chatbot", False)
                
                # Validate required string fields
                self.field_validate_str({
                    "document_id": document_id,
                    "conv_id": conv_id,
                    "llm_key": llm_key,
                    "db_key": db_key,
                    "question": question,
                    "user_id": user_id,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate empty values for required fields
                self.empty_value_check({
                    "document_id": document_id,
                    "conv_id": conv_id,
                    "question": question,
                    "user_id": user_id,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate boolean fields
                self.field_validate_bool({
                    "short_answer": short_answer,
                    "direct_answer": direct_answer,
                    "use_web_search": use_web_search
                })
                
                # Validate chat_history is a list
                self.field_validate_list_dict({"chat_history": chat_history})
                
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                await self.send_message(producer, topic, message)
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                logger.error(traceback.format_exc())
                await self.send_message(producer, topic, message)
            if "error_code" not in message:
                try:
                    # admin_user_guide = await get_admin_vector_db(db_key) 
                    module = Chatbot(
                        language, 
                        user_id,
                        model_id, 
                        llm_key, 
                        db_key,
                        MAX_TOKENS_DICT.get(event_type, 10000), 
                        conv_id, 
                        short_answer, 
                        use_web_search, 
                        self.mongo_client,
                        self.redis,
                        search_web_key,
                        document_id, 
                    )
                    await self.clean_up(conv_id)
                    response, input_tokens, output_tokens, embed_tokens, web_search_call = await module.get_answer(
                        question, 
                        chat_history, 
                        direct_answer,
                    )
                    logger.info(f"[{_id}] Answer: {response}")
                    message["response"] = response
                    message["input_tokens"] = input_tokens
                    message["output_tokens"] = output_tokens
                    message["embed_tokens"] = embed_tokens
                    message["embed_model"] = getattr(module.embeddings, "model_name", getattr(module.embeddings, "model", "BAAI/bge-small-en-v1.5")) if isinstance(getattr(module.embeddings, "model_name", None), str) else getattr(module.embeddings, "model", "text-embedding-3-small")
                    message["web_search_call"] = web_search_call
                    message["conv_id"] = conv_id
                    message["document_id"] = document_id
                    message["user_id"] = user_id
                    logger.info(f'[{_id}] SEND: {message}')
                    end = time.time()
                    logger.info(f'[{_id}] Response time: {end - start_time}')
                    await self.send_message(producer, topic, message)
                    # await send_health_check(id=_id, status=AIStatus.FINISHED)
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    logger.error(traceback.format_exc())
                    await self.send_message(producer, topic, message)
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}"
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    await self.send_message(producer, topic, message)
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = ChatbotError.INNER_CHATBOT
                    message["error"] = str(e)
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    logger.error(traceback.format_exc())
                    await self.send_message(producer, topic, message)
        
        elif event_type == AIEvent.WRITER_CHATBOT:
            try:
                conv_id = value["conv_id"]
                question = value["question"]
                user_id = value["user_id"]
                resume = value.get("resume", False)
                short_answer = value.get("short_answer", False)
                use_web_search = value.get("use_web_search_chatbot", False)
                template_key = value.get("template_key", "")

                self.field_validate_str({
                    "conv_id": conv_id,
                    "user_id": user_id,
                    "question": question,
                    "llm_key": llm_key,
                    "language": language,
                    "model_id": model_id
                })
                self.field_validate_bool({"resume": resume, "use_web_search": use_web_search, "short_answer": short_answer})
                self.field_validate_str({"template_key": template_key})

                self.empty_value_check({
                    "conv_id": conv_id,
                    "user_id": user_id,
                    "question": question,
                    "language": language,
                    "model_id": model_id
                })
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                await self.send_message(producer, topic, message)
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.info(traceback.format_exc())
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                await self.send_message(producer, topic, message)
                
            if "error_code" not in message:
                try:
                    inp = {
                        "conv_id": conv_id,
                        "user_query": question,
                        "user_id": user_id,
                        "model_id": model_id,
                        "llm_key": llm_key,
                        "language": language,
                        "max_tokens": MAX_TOKENS_DICT.get(event_type, 10000),
                        "short_answer": short_answer,
                        "template_key": template_key,
                    }
                    message["conv_id"] = conv_id
                    message["user_id"] = user_id
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    is_done = True
                    retry = 0
                    success = False
                    checkpointer = await get_async_redis_checkpoint()
                    while retry < 2 and not success:
                        writer_graph = await get_graph(checkpointer)
                        if not resume:
                            await self.clean_up(conv_id)
                            try:
                                graph_output = await asyncio.wait_for(
                                    writer_graph.ainvoke(
                                        input=inp, 
                                        config={
                                            "recursion_limit": 1000, "thread_id": conv_id
                                        }
                                    ), timeout=60
                                )
                                interruptted = graph_output.get("__interrupt__")
                                if interruptted:
                                    interrupt_message = interruptted[-1].value
                                    logger.info(f"[{_id}] {interrupt_message}")
                                    message["ai_message"] = interrupt_message
                                    is_done = False
                                success = True
                            except asyncio.TimeoutError:
                                retry += 1
                        else:
                            resume_inp = {
                                "model_id": model_id,
                                "llm_key": llm_key,
                                "question": question,
                            }
                            try:
                                graph_output = await asyncio.wait_for(
                                    writer_graph.ainvoke(
                                        Command(resume=resume_inp), 
                                        config={
                                            "recursion_limit": 1000, "thread_id": conv_id
                                        }
                                    ), timeout=60
                                )
                                interruptted = graph_output.get("__interrupt__")
                                if interruptted:
                                    interrupt_message = interruptted[-1].value
                                    logger.info(f"[{_id}] {interrupt_message}")
                                    message["ai_message"] = interrupt_message
                                    is_done = False
                                success = True
                            except asyncio.TimeoutError:
                                retry += 1
                    if success:
                        final_state = await writer_graph.aget_state({"configurable": {"thread_id": conv_id}})
                        message["model_id"] = final_state.values["model_id"]
                        message["input_tokens"] = final_state.values["input_tokens"]
                        message["output_tokens"] = final_state.values["output_tokens"]
                        message["template_key"] = final_state.values.get("template_key", "")
                        message["done_chat"] = is_done
                        if is_done:
                            message["chat_history"] = final_state.values["chat_history"]
                            message["summary"] = final_state.values["dependencies"].model_dump()
                            message["outline"] = final_state.values["outline"] if "outline" in final_state.values else ""

                            await self.clean_up(conv_id)
                        await self.send_message(producer, topic, message)
                        # await send_health_check(id=_id, status=AIStatus.FINISHED)
                    else:
                        logger.error(f"[{_id}] writer_chatbot task timed out for conv_id: {conv_id}")
                        message["error_code"] = Error.TIME_OUT_REQUEST
                        message["error"] = f"writer_chatbot task timed out for conv_id: {conv_id}"
                        # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                        await self.send_message(producer, topic, message)
                except AIERROR as e:
                    await send_health_check(id=_id, status=AIStatus.ERROR)
                    logger.error(f"[{_id}] LLM failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    logger.error(traceback.format_exc())
                    await self.send_message(producer, topic, message)
                except NotImplementedError:
                    await send_health_check(id=_id, status=AIStatus.ERROR)
                    logger.error(f"[{_id}] Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}"
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    await self.send_message(producer, topic, message)
                except Exception as e:
                    await send_health_check(id=_id, status=AIStatus.ERROR)
                    logger.error(f"[{_id}] Task failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = ChatbotError.WRITER_CHATBOT
                    message["error"] = str(e)
                    # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    logger.error(traceback.format_exc())
                    await self.send_message(producer, topic, message)
        elif event_type == AIEvent.WRITER:
            try:
                document_id = value["document_id"]
                conv_id = value["conv_id"]
                user_id = value["user_id"]
                chat_history = value["chat_history"]
                summary = value["summary"]
                outline = value["outline"]
                session_id = value["session_id"]
                domain = value.get("domain", "")
                field = value.get("field", "")
                notes = value.get("notes", [])
                resume_error = value.get("resume_error", False)

                # Agent configuration parameters
                use_web_search = value.get("use_web_search", False)
                max_research_iterations = value.get("max_research_iterations", 3)
                max_researcher_iterations = value.get("max_researcher_iterations", 6)
                max_concurrent_research_units = value.get("max_concurrent_research_units", 3)

                self.field_validate_str({
                    "conv_id": conv_id,
                    "user_id": user_id,
                    "chat_history": chat_history,
                    "llm_key": llm_key,
                    "language": language,
                    "model_id": model_id,
                    "outline": outline,
                    "session_id": session_id,
                    "document_id": document_id,
                })
                self.field_validate_dict({"summary": summary})
                self.field_validate_bool({"use_web_search": use_web_search})
                self.field_validate_number({
                    "max_research_iterations": max_research_iterations,
                    "max_researcher_iterations": max_researcher_iterations,
                    "max_concurrent_research_units": max_concurrent_research_units,
                })
                self.empty_value_check({
                    "conv_id": conv_id,
                    "user_id": user_id,
                    "language": language,
                    "model_id": model_id,
                    "chat_history": chat_history,
                    "summary": summary,
                })
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                await self.send_message(producer, topic, message)
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                await self.send_message(producer, topic, message)
                
            if "error_code" not in message:
                try:
                    has_finished = True
                    message["conv_id"] = conv_id
                    message["user_id"] = user_id
                    message["document_id"] = document_id
                    message["session_id"] = session_id
                    message["web_search_call"] = 0
                    message["embed_tokens"] = 0
                    message["embed_model"] = "text-embedding-3-small"
                    # Initialize cumulative token counters
                    cumulative_input_tokens = 0
                    cumulative_output_tokens = 0
                    cumulative_embed_tokens = 0
                    write_section_start = time.time()
                    checkpointer = await get_async_redis_checkpoint()
                    writer = await writer_bot(checkpointer)
                    if not resume_error:
                        await self.clean_up(document_id)
                        inp = {
                            "document_id": document_id,
                            "user_id": user_id,
                            "conv_id": conv_id,
                            "llm_key": llm_key,
                            "db_key": db_key,
                            "model_id": model_id,

                            "language": language,
                            "domain": domain,
                            "field": field,

                            "summary": summary,
                            "outline_str": outline,
                            "research_note": "\n".join(notes),
                            "use_web_search": use_web_search,
                        }

                        async for mode, chunk in writer.astream(input=inp, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                            if mode == "updates":
                                if "load_refs" in chunk:
                                    # Include cumulative tokens in the message
                                    message["input_tokens"] = cumulative_input_tokens
                                    message["output_tokens"] = cumulative_output_tokens
                                    message["embed_tokens"] = cumulative_embed_tokens
                                    await self.send_message(producer, topic, message)
                                if "section_synthesizer" in chunk:
                                    if "other_sections" in chunk["section_synthesizer"] and len(chunk["section_synthesizer"]["other_sections"]):
                                        message["section"] = chunk["section_synthesizer"]["new_content"]
                                        message["change_section"] = False
                                        write_section_end = time.time()
                                        message["index"] = write_section_end
                                        # Include cumulative tokens in the message
                                        message["input_tokens"] = cumulative_input_tokens
                                        message["output_tokens"] = cumulative_output_tokens
                                        message["embed_tokens"] = cumulative_embed_tokens
                                        await self.send_message(producer, topic, message)
                                if "add_ref" in chunk:
                                    if "other_sections" in chunk["add_ref"] and len(chunk["add_ref"]["other_sections"]):
                                        write_section_end = time.time()
                                        message["section"] = chunk["add_ref"]["other_sections"][-1]
                                        message["index"] = write_section_end
                                        message["change_section"] = True
                                        logger.info(f"[{_id}] Send section {message["section"]['name']}")
                                        # Include cumulative tokens in the message
                                        message["input_tokens"] = cumulative_input_tokens
                                        message["output_tokens"] = cumulative_output_tokens
                                        message["embed_tokens"] = cumulative_embed_tokens
                                        await self.send_message(producer, topic, message)
                                        has_finished = True
                                        logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                if "edit_paragraph" in chunk:
                                    write_section_end = time.time()
                                    message["section"] = chunk["edit_paragraph"]["other_sections"][-1]
                                    message["index"] = write_section_end
                                    message["change_section"] = True
                                    # Include cumulative tokens in the message
                                    message["input_tokens"] = cumulative_input_tokens
                                    message["output_tokens"] = cumulative_output_tokens
                                    message["embed_tokens"] = cumulative_embed_tokens
                                    await self.send_message(producer, topic, message)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                                # Update cumulative token counters from the current state
                                if "input_tokens" in chunk:
                                    cumulative_input_tokens = chunk["input_tokens"]
                                if "output_tokens" in chunk:
                                    cumulative_output_tokens = chunk["output_tokens"]
                                if "embed_tokens" in chunk:
                                    cumulative_embed_tokens = chunk["embed_tokens"]
                    else:
                        async for mode, chunk in writer.astream(None, stream_mode=["updates", "values"], config={"recursion_limit": 1000, "thread_id": document_id}):
                            if mode == "updates":
                                if "load_refs" in chunk:
                                    # Include cumulative tokens in the message
                                    message["input_tokens"] = cumulative_input_tokens
                                    message["output_tokens"] = cumulative_output_tokens
                                    message["embed_tokens"] = cumulative_embed_tokens
                                    await self.send_message(producer, topic, message)
                                if "section_synthesizer" in chunk:
                                    if "other_sections" in chunk["section_synthesizer"] and len(chunk["section_synthesizer"]["other_sections"]):
                                        message["section"] = chunk["section_synthesizer"]["new_content"]
                                        message["change_section"] = False
                                        write_section_end = time.time()
                                        message["index"] = write_section_end
                                        # Include cumulative tokens in the message
                                        message["input_tokens"] = cumulative_input_tokens
                                        message["output_tokens"] = cumulative_output_tokens
                                        message["embed_tokens"] = cumulative_embed_tokens
                                        await self.send_message(producer, topic, message)
                                if "add_ref" in chunk:
                                    if "other_sections" in chunk["add_ref"] and len(chunk["add_ref"]["other_sections"]):
                                        write_section_end = time.time()
                                        message["section"] = chunk["add_ref"]["other_sections"][-1]
                                        message["index"] = write_section_end
                                        message["change_section"] = True
                                        logger.info(f"[{_id}] Send section {message["section"]['name']}")
                                        # Include cumulative tokens in the message
                                        message["input_tokens"] = cumulative_input_tokens
                                        message["output_tokens"] = cumulative_output_tokens
                                        message["embed_tokens"] = cumulative_embed_tokens
                                        await self.send_message(producer, topic, message)
                                        has_finished = True
                                        logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                                if "edit_paragraph" in chunk:
                                    write_section_end = time.time()
                                    message["section"] = chunk["edit_paragraph"]["other_sections"][-1]
                                    message["index"] = write_section_end
                                    message["change_section"] = True
                                    # Include cumulative tokens in the message
                                    message["input_tokens"] = cumulative_input_tokens
                                    message["output_tokens"] = cumulative_output_tokens
                                    message["embed_tokens"] = cumulative_embed_tokens
                                    await self.send_message(producer, topic, message)
                                    has_finished = True
                            elif mode == "values":
                                final_state = chunk
                                # Update cumulative token counters from the current state
                                if "input_tokens" in chunk:
                                    cumulative_input_tokens = chunk["input_tokens"]
                                if "output_tokens" in chunk:
                                    cumulative_output_tokens = chunk["output_tokens"]
                                if "embed_tokens" in chunk:
                                    cumulative_embed_tokens = chunk["embed_tokens"]
                    if has_finished:
                        if "section" in message:
                            del message["section"]
                        if "index" in message:
                            del message["index"]
                        if "change_section" in message:
                            del message["change_section"]
                        message["input_tokens"] = final_state["input_tokens"]
                        message["output_tokens"] = final_state["output_tokens"]
                        message["embed_tokens"] = final_state["embed_tokens"]
                        message["has_finished"] = True
                        await self.send_message(producer, topic, message)
                        write_section_end = time.time()
                        logger.info(f"[{_id}] Time: {write_section_end - write_section_start}")
                        await send_health_check(id=_id, status=AIStatus.FINISHED)
                        await self.clean_up(document_id)
                except AIERROR as e:
                    logger.error(f"[{_id}] LLM failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = e.status_code
                    message["error"] = e.message
                    logger.error(traceback.format_exc())
                    await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    await self.send_message(producer, topic, message)
                except NotImplementedError:
                    logger.error(f"[{_id}] Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = Error.NOT_IMPLEMENT_MODEL
                    message["error"] = f"Currently no support for model {model_id} for conv_id: {conv_id} - event {event_type}"
                    await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    await self.send_message(producer, topic, message)
                except Exception as e:
                    logger.error(f"[{_id}] Task failed for conv_id: {conv_id} - event {event_type}")
                    message["error_code"] = ChatbotError.WRITER
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
                    await self.send_message(producer, topic, message)

        else:
            logger.error(f"[{_id}] Wrong event_type - event {event_type}")
            message["error_code"] = Error.EVENT_NOT_FOUND
            message["error"] = f"Wrong event_type - event {event_type}"
            # await send_health_check(id=_id, status=AIStatus.ERROR, error=message["error_code"], error_message=message["error"])
            await self.send_message(producer, topic, message)     


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
    listener = ChatbotEventListener(topics, consumer_config)
    await listener.start()
