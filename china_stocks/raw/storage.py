"""raw 层写入工具：保存 API 原始响应用于回溯。"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from ..db import get_session


def save_raw_response(
    platform_code: str,
    api_name: str,
    params: dict[str, Any],
    response: Any,
) -> int:
    """保存一条原始响应到 raw.api_response，返回记录 id。"""
    try:
        resp_json = json.loads(json.dumps(response, default=str, ensure_ascii=False))
    except Exception:
        resp_json = {"raw": str(response)}

    with get_session() as sess:
        row = sess.execute(
            text("""
                INSERT INTO raw.api_response (platform_code, api_name, params, response)
                VALUES (:pc, :api, :params::jsonb, :resp::jsonb)
                RETURNING id
            """),
            {
                "pc": platform_code,
                "api": api_name,
                "params": json.dumps(params, ensure_ascii=False),
                "resp": json.dumps(resp_json, ensure_ascii=False),
            },
        ).fetchone()
        return row[0]
