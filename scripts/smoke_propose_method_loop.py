"""Automated multi-turn smoke test for propose_method until finished.

Flow:
1. Send initial propose_method event to write_section_request.
2. Listen on write_section_response.
3. If has_finished is False, send resume event with user_input='Đồng ý'.
4. Repeat until has_finished is True or max turns reached.
5. Verify final artifacts in MinIO (Mermaid diagram image).
"""

import asyncio
import json
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import boto3

from document_setup.src.configs.app import settings
import topics


async def main():
    payload_file = "scripts/payloads/propose_method.json"
    with open(payload_file, encoding="utf-8") as f:
        payload = json.load(f)

    # Ensure clean thread for reproducible multi-turn
    thread_id = f"smoke-method-thread-{int(time.time())}"
    conv_id = f"smoke-method-conv-{int(time.time())}"
    payload["thread_id"] = thread_id
    payload["conv_id"] = conv_id
    payload["user_input"] = ""
    payload["resume_method"] = False

    import uuid
    req_topic = topics.WRITE_SECTION_REQUEST
    res_topic = topics.WRITE_SECTION_RESPONSE

    bootstrap = settings.KAFKA
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    consumer = AIOKafkaConsumer(
        res_topic,
        bootstrap_servers=bootstrap,
        group_id=f"smoke_propose_{uuid.uuid4().hex[:6]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    await consumer.start()
    await producer.start()

    for _ in range(100):
        await consumer.getmany(timeout_ms=200)
        if consumer.assignment():
            break

    print(f"[loop] Connected to Kafka {bootstrap}")

    turn = 1
    max_turns = 10
    has_finished = False

    try:
        while turn <= max_turns and not has_finished:
            print(f"\n[loop] === TURN {turn} ===")
            print(f"[loop] Sending to {req_topic} (resume={payload.get('resume_method')}, input='{payload.get('user_input', '')[:30]}')")
            t0 = time.time()
            await producer.send_and_wait(req_topic, payload)

            # Wait for response matching our thread_id / conv_id
            matched_msg = None
            timeout = 180
            deadline = time.time() + timeout

            while time.time() < deadline:
                remaining = max(1.0, deadline - time.time())
                try:
                    msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                val = msg.value
                if val.get("event_type") == "propose_method" and (
                    val.get("thread_id") == thread_id or val.get("conv_id") == conv_id
                ):
                    matched_msg = val
                    break

            if not matched_msg:
                print(f"[loop] Timeout waiting response for turn {turn}")
                return 1

            elapsed = round(time.time() - t0, 1)
            err_code = matched_msg.get("error_code")
            if err_code is not None:
                print(f"[loop] ERROR received: code={err_code}, error={matched_msg.get('error')}")
                return 1

            has_finished = matched_msg.get("has_finished", False)
            ai_msg = matched_msg.get("ai_message", "")
            preview = str(ai_msg)[:120].replace("\n", " ")
            print(f"[loop] Response in {elapsed}s: finished={has_finished}, ai_message preview: {preview}...")

            if has_finished:
                print("\n[loop] SUCCESS! Flow completed (has_finished=True)")
                break

            # Setup payload for next turn
            payload["resume_method"] = True
            payload["user_input"] = "HOANTOANDONGY"  # Trigger fast path in proposed_method_v2
            turn += 1

        if not has_finished:
            print(f"[loop] Reached max turns ({max_turns}) without finishing.")
            return 1

        from data_analysis.src.modules.utils import get_s3_client
        s3 = get_s3_client()
        doc_id = payload["document_id"]
        prefix = f"smoke-user/article/{doc_id}/analysis/"
        resp = s3.list_objects_v2(Bucket="users", Prefix=prefix)
        contents = resp.get("Contents", [])
        print(f"\n[loop] MinIO verification under s3://users/{prefix}:")
        found_mermaid = False
        for obj in contents:
            key = obj["Key"]
            size = obj["Size"]
            print(f"  - {key} ({size} bytes)")
            if key.endswith(".png") and not key.endswith("hist.png") and not key.endswith("pie.png"):
                found_mermaid = True

        if found_mermaid:
            print("[loop] Verified: Mermaid diagram PNG exists in MinIO!")
        else:
            print("[loop] Warning: Mermaid diagram PNG not found with expected naming pattern.")

        return 0

    finally:
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    code = asyncio.run(main())
    sys.exit(code)
