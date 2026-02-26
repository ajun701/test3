from __future__ import annotations

import json
import re
import time
from http import HTTPStatus
from typing import Any, Dict, List

from app.services.cleaning_service import parse_money

try:
    import dashscope
except Exception:  # pragma: no cover - optional dependency at runtime
    dashscope = None



def make_vl_prompt(expected_amount: float) -> str:
    return (
        "你是电商售后财务审核助手。现在给你一条售后记录的多张截图。"
        "请在所有截图中识别和运费相关的金额（运费/快递费/配送费/邮费/寄件费用/实付运费/总运费）。"
        f"用户填写金额为 {expected_amount:.2f} 元。"
        "严格输出 JSON，不要输出其它文本。JSON 字段: paid_amount, is_match, reason。"
        "规则：1）只认和运费关键词直接相关金额；2）不要把商品金额、订单合计当作运费；"
        "3）运费可能为 0；4）不确定时 paid_amount=null, is_match=null 并说明原因；"
        "5）允许误差 0.01。"
    )



def _parse_vl_json(raw_text: str, expected_amount: float) -> Dict[str, Any]:
    data = None
    try:
        data = json.loads(raw_text)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", raw_text)
        if m:
            try:
                data = json.loads(m.group(0))
            except Exception:
                data = None

    if not isinstance(data, dict):
        return {
            "paid_amount": None,
            "is_match": None,
            "reason": "输出非JSON",
            "raw_text": raw_text,
        }

    paid_f = parse_money(data.get("paid_amount")) if data.get("paid_amount") is not None else None
    is_match = data.get("is_match", None)
    if is_match is None and paid_f is not None:
        is_match = abs(paid_f - expected_amount) <= 0.01

    reason = str(data.get("reason", "")).strip()
    if not reason:
        if is_match is True:
            reason = "一致"
        elif paid_f is None:
            reason = "图片模糊或无法识别金额"
        else:
            reason = "不一致"

    return {
        "paid_amount": paid_f,
        "is_match": is_match,
        "reason": reason,
        "raw_text": raw_text,
    }



def call_qwen_vl_multi_with_retry(
    image_urls: List[str],
    expected_amount: float,
    model: str,
    max_retries: int,
    backoff_base_sec: float,
    api_key: str,
) -> Dict[str, Any]:
    if dashscope is None:
        return {"paid_amount": None, "is_match": None, "reason": "未安装 dashscope 依赖", "attempts": 0}

    if not api_key.strip():
        return {"paid_amount": None, "is_match": None, "reason": "缺少 DashScope API Key", "attempts": 0}

    dashscope.api_key = api_key.strip()

    prompt = make_vl_prompt(expected_amount)
    content = [{"image": url} for url in image_urls]
    content.append({"text": prompt})
    messages = [{"role": "user", "content": content}]

    attempt = 0
    while True:
        try:
            resp = dashscope.MultiModalConversation.call(model=model, messages=messages)

            if hasattr(resp, "status_code") and resp.status_code != HTTPStatus.OK:
                reason = f"API失败: {getattr(resp, 'code', '')} {getattr(resp, 'message', '')}".strip()
                status_code = getattr(resp, "status_code", None)
                if status_code in (429, 502, 503, 504) and attempt < max_retries:
                    time.sleep(backoff_base_sec * (2 ** attempt))
                    attempt += 1
                    continue
                return {"paid_amount": None, "is_match": None, "reason": reason, "attempts": attempt + 1}

            raw_text = resp.output.choices[0]["message"]["content"][0]["text"]
            parsed = _parse_vl_json(raw_text, expected_amount)
            parsed["attempts"] = attempt + 1
            return parsed
        except Exception as exc:
            if attempt < max_retries:
                time.sleep(backoff_base_sec * (2 ** attempt))
                attempt += 1
                continue
            return {"paid_amount": None, "is_match": None, "reason": f"异常: {exc}", "attempts": attempt + 1}
