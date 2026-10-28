import asyncio
from aiokafka.structs import ConsumerRecord
import json
import logging
import time
import traceback

from ai_chatbot.src.configs.app import settings
from ai_chatbot.src.modules.ai_in_doc import AIInDoc

from event_type import EnhancementEvent
from error_type import EnhancementError, Error, ERROR_DICT, TIME_OUT_DICT, MAX_TOKENS_DICT
from exception_type import AIERROR
from listener import BaseAsyncListener
from producers import get_producer
import topics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

        
class EnhancementEventListener(BaseAsyncListener):
    def __init__(self, topics, consumer_configs: dict, concurency: int | None = None):
        super().__init__(topics, consumer_configs, concurency)
        
    async def _handle_record(self, record: ConsumerRecord):
        value_str = record.value
        value = json.loads(value_str)
        topic = record.topic
        try:
            match topic:
                case topics.ENHANCEMENT_REQUEST:
                    response_topic = topics.ENHANCEMENT_RESPONSE
                    await asyncio.wait_for(self._handle_enhancement_event(topics.ENHANCEMENT_RESPONSE, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 300))
                case topics.ENHANCEMENT_REQUEST_LOCAL:
                    response_topic = topics.ENHANCEMENT_RESPONSE_LOCAL
                    await asyncio.wait_for(self._handle_enhancement_event(topics.ENHANCEMENT_RESPONSE_LOCAL, value_str), timeout=TIME_OUT_DICT.get(value["event_type"], 300))
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
            logger.error(f"[{_id}] Task timed out for document_id: {value["event_type"]}")
            await self._finalize_task(None, False, _id, producer, response_topic, message, value["event_type"], True)
        except Exception as e:
            # Bug #17 (cùng loại): exception thoát khỏi handler ⇒ BE chỉ thấy timeout.
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
                    topics.ENHANCEMENT_RESPONSE if topic == topics.ENHANCEMENT_REQUEST
                    else topics.ENHANCEMENT_RESPONSE_LOCAL
                )
                await self.send_message(producer, response_topic, message)
            except Exception as publish_error:  # noqa: BLE001
                logger.error(f"Failed to publish error response for {value.get('event_type')}: {publish_error}")

    async def _handle_enhancement_event(self, topic, value_str):
        start_time = time.time()
        value = json.loads(value_str)
        producer = await get_producer()
        if "document_id" in value:
            _id = value["document_id"]
        elif "conv_id" in value:
            _id = value["conv_id"]
        else:
            _id = "admin"
        timeout_time = TIME_OUT_DICT.get(value["event_type"], 300)
        redis_timer = asyncio.create_task(self.task_timer(_id, value["event_type"], topic, timeout_time, 20))
        task_success = False
        try:
            event_type = value["event_type"]
            model_id = value["model_id"]
            language = value["language"]
            
            message = {
                "model_id": model_id,
                "event_type": event_type,
            }
            llm_key, db_key, _ = await self.get_key(value["model_id"])
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
        if event_type == EnhancementEvent.AI_IN_DOC:
            try:
                # Extract required fields
                user_id = value["user_id"]
                document_id = value["document_id"]
                paragraphs = value["paragraphs"]
                paragraph_context = value["paragraph_context"]
                action = value["action"]
                proposal = value["proposal"]
                section_id = value["section_id"]
                
                # Validate required string fields
                self.field_validate_str({
                    "document_id": document_id,
                    "paragraphs": paragraphs,
                    "paragraph_context": paragraph_context,
                    "action": action,
                    "language": language,
                    "model_id": model_id,
                })
                self.field_validate_dict({"proposal": proposal})
                
                # Validate empty values for required fields
                self.empty_value_check({
                    "document_id": document_id,
                    "paragraphs": paragraphs,
                    "paragraph_context": paragraph_context,
                    "action": action,
                    "language": language,
                    "model_id": model_id,
                    "proposal": proposal
                })
  
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = AIInDoc(self.mongo_client, model_id, llm_key, db_key, document_id, language, MAX_TOKENS_DICT.get(event_type, 10000))
                    response = await module.process_paragraph(paragraph_context, paragraphs, action, proposal)
                    
                    message.update({
                        "response": response,
                        "input_tokens": module.input_tokens,
                        "output_tokens": module.output_tokens,
                        "embed_tokens": module.embed_tokens,
                        "embed_model": getattr(module.embeddings, "model_name", getattr(module.embeddings, "model", "BAAI/bge-small-en-v1.5")) if isinstance(getattr(module.embeddings, "model_name", None), str) else getattr(module.embeddings, "model", "text-embedding-3-small"),
                        "web_search_call": 0,
                        "user_id": user_id,
                        "section_id": section_id,
                        "document_id": document_id
                    })
                    
                    end_time = time.time()
                    logger.info(f'[{_id}] SEND: Write sections')
                    logger.info(f'[{_id}] Write sections time: {end_time - start_time}')
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
                    message["error_code"] = EnhancementError.AI_IN_DOC
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())

        elif event_type == EnhancementEvent.GET_SUGGESTIONS:
            try:
                # Extract required fields
                user_id = value["user_id"]
                document_id = value["document_id"]
                docs = value["processed_docs"]
                section = value["section"]
                section_context = value["section_context"]
                
                # Validate required string fields
                self.field_validate_str({
                    "user_id": user_id,
                    "document_id": document_id,
                    "section": section,
                    "section_context": section_context,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate empty values for required fields
                self.empty_value_check({
                    "user_id": user_id,
                    "document_id": document_id,
                    "language": language,
                    "model_id": model_id,
                })
                
                # Validate docs is a list of dictionaries
                self.field_validate_list_dict({"processed_docs": docs})              
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                
            if "error_code" not in message:
                try:
                    module = AIInDoc(self.mongo_client, model_id, llm_key, db_key, "", language, MAX_TOKENS_DICT.get(event_type, 10000))
                    embed_tokens_db = 0
                        
                    suggested_paragraphs, total_input_tokens, total_output_tokens = await module._review(section, section_context)
                    module.embed_tokens += embed_tokens_db
                    
                    message.update({
                        "user_id": user_id,
                        "document_id": document_id,
                        "suggested_paragraphs": suggested_paragraphs,
                        "section_context": section_context,
                        "input_tokens": total_input_tokens,
                        "output_tokens": total_output_tokens,
                        "embed_tokens": module.embed_tokens,
                        "embed_model": getattr(module.embeddings, "model_name", getattr(module.embeddings, "model", "BAAI/bge-small-en-v1.5")) if isinstance(getattr(module.embeddings, "model_name", None), str) else getattr(module.embeddings, "model", "text-embedding-3-small"),
                        "web_search_call": 0,
                    })
                    message["disable_edit"] = len(section) == 0
                    end_time = time.time()
                    logger.info(f'[{_id}] SEND: get_suggestions')
                    logger.info(f'[{_id}] get_suggestions time: {end_time - start_time}')
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
                    message["error_code"] = EnhancementError.GET_SUGGESTIONS
                    message["error"] = str(e)
                    logger.error(traceback.format_exc()) 

        elif event_type == EnhancementEvent.ENHANCE:
            try:
                # Extract required fields
                user_id = value["user_id"]
                document_id = value["document_id"]
                section = value["section"]
                suggested_paragraph = value["suggested_paragraph"]
                
                # Validate required string fields
                self.field_validate_str({
                    "user_id": user_id,
                    "document_id": document_id,
                    "section": section,
                    "language": language,
                    "model_id": model_id,
                })
                
                # Validate empty values for required fields
                self.empty_value_check({
                    "user_id": user_id,
                    "document_id": document_id,
                    "section": section,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate suggested_paragraph is a dictionary
                self.field_validate_dict({"suggested_paragraph": suggested_paragraph})
                self.empty_value_check({"suggested_paragraph": suggested_paragraph})
                
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc()) 
                
            if "error_code" not in message:
                try:
                    module = AIInDoc(self.mongo_client, model_id, llm_key, db_key, document_id, language, MAX_TOKENS_DICT.get(event_type, 10000))
                    updated_paragraphs = await module.update_one(suggested_paragraph, section)
                    
                    message.update({
                        "user_id": user_id,
                        "document_id": document_id,
                        "updated_paragraphs": updated_paragraphs,
                        "input_tokens": module.input_tokens,
                        "output_tokens": module.output_tokens,
                        "embed_tokens": 0,
                        "embed_model": "text-embedding-3-small",
                        "web_search_call": 0,
                    })
                    
                    end_time = time.time()
                    logger.info(f'[{_id}] SEND: enhance_one')
                    logger.info(f'[{_id}] enhance_one time: {end_time - start_time}')
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
                    message["error_code"] = EnhancementError.ENHANCE
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        elif event_type == EnhancementEvent.ENHANCE_ALL:
            try:
                # Extract required fields
                user_id = value["user_id"]
                document_id = value["document_id"]
                section = value["section"]
                suggested_paragraphs = value["suggested_paragraphs"]

                # Validate required string fields
                self.field_validate_str({
                    "user_id": user_id,
                    "document_id": document_id,
                    "section": section,
                    "language": language,
                    "model_id": model_id,
                })
                
                # Validate empty values for required fields
                self.empty_value_check({
                    "user_id": user_id,
                    "document_id": document_id,
                    "section": section,
                    "language": language,
                    "model_id": model_id
                })
                
                # Validate suggested_paragraphs is a list of dictionaries
                self.field_validate_list_dict({"suggested_paragraphs": suggested_paragraphs})
                self.empty_value_check({"suggested_paragraphs": suggested_paragraphs})
                
            except KeyError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = f"Missing field '{str(e)}'"
                
            except ValueError as e:
                message["error_code"] = Error.INVALID_PARAMS
                message["error"] = str(e)
                logger.error(traceback.format_exc())
                 
            if "error_code" not in message:
                try:
                    module = AIInDoc(self.mongo_client, model_id, llm_key, db_key, document_id, language, MAX_TOKENS_DICT.get(event_type, 10000))
                    updated_paragraphs = await module.update_many(suggested_paragraphs, section)
                    
                    message.update({
                        "user_id": user_id,
                        "document_id": document_id,
                        "updated_paragraphs": updated_paragraphs,
                        "input_tokens": module.input_tokens,
                        "output_tokens": module.output_tokens,
                        "embed_tokens": 0,
                        "embed_model": "text-embedding-3-small",
                        "web_search_call": 0,
                    })
                    
                    end_time = time.time()
                    logger.info(f'[{_id}] SEND: enhance_all')
                    logger.info(f'[{_id}] enhance_all time: {end_time - start_time}')
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
                    message["error_code"] = EnhancementError.ENHANCE_ALL
                    message["error"] = str(e)
                    logger.error(traceback.format_exc())
                    
        else:
            logger.info(f"[{_id}] Wrong event_type - event {event_type}")
            message["error_code"] = Error.EVENT_NOT_FOUND
            message["error"] = f"Wrong event_type - event {event_type}"
            # Echo identifier để BE ghép được request (nếu không sẽ chỉ thấy timeout)
            for identifier in ("user_id", "document_id", "session_id", "conv_id"):
                if value.get(identifier):
                    message[identifier] = value[identifier]
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
    listener = EnhancementEventListener(topics, consumer_config)
    await listener.start()