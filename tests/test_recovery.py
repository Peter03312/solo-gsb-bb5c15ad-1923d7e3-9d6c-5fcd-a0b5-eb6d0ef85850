"""启动恢复的无第三方依赖验证（python3 tests/test_recovery.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），直接在 SQLite 上
模拟「进程停机时房间仍在运行 → 重启执行 init_schema」，校验：

1. 重启在同一事务内把 running 房间冻结为 paused，并追加一条与手动暂停
   同格式的 clock 事件（action=paused）：elapsed_ms 与房间冻结值一致，
   created_at 取本次恢复时刻；沿用原有墙钟差结算公式；
2. 幂等：重复 init_schema 不重复追加；原本 idle/paused 的房间不追加；
3. app.export.build_timeline 与 app.latency 据此冻结：重启后暂停期间
   提交的回执挂靠冻结虚拟毫秒（不虚增时延），继续运行后从冻结值累计；
4. 跨 reset 仍按轮次隔离：恢复事件归属停机时所在轮次。
"""
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import export as rv  # noqa: E402
from app import latency as lat  # noqa: E402


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(dbmod.SCHEMA)
    return conn


def add_event(conn, room_id, t, type_, payload):
    cur = conn.execute(
        "INSERT INTO events (room_id,type,payload,created_at) VALUES (?,?,?,?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), t))
    conn.commit()
    return cur.lastrowid


def clock_events(conn, room_id):
    return conn.execute(
        "SELECT id, type, payload, created_at FROM events"
        " WHERE room_id=? AND type='clock' ORDER BY id", (room_id,)).fetchall()


def timeline_rows(conn, room_id, high_water=None):
    if high_water is None:
        high_water = conn.execute(
            "SELECT COALESCE(MAX(id),0) m FROM events WHERE room_id=?",
            (room_id,)).fetchone()["m"]
    return conn.execute(
        "SELECT id, type, payload, created_at FROM events"
        " WHERE room_id=? AND id<=? ORDER BY id", (room_id, high_water)).fetchall()


def test_running_room_frozen_with_paused_event():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        # 假装 100 墙钟秒前进入 running；started 事件与 last_resume_at 同刻
        t_start = time.time() - 100
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV001','重启恢复','running',0,?,?)",
            (t_start, t_start)).lastrowid
        g1 = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, "红队", "RED001", t_start)).lastrowid
        c1 = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id) VALUES (?,?,?,?,?)",
            (rid, 1, 10, "告警", None)).lastrowid
        conn.commit()
        add_event(conn, rid, t_start, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})
        add_event(conn, rid, t_start, "group_added",
                  {"id": g1, "name": "红队", "join_code": "RED001"})
        add_event(conn, rid, t_start + 10, "card_revealed",
                  {"id": c1, "seq": 1, "trigger_seconds": 10, "title": "告警",
                   "content": "", "group_id": None, "revealed": True,
                   "revealed_at_ms": 10000, "answers_locked": False})
        conn.close()

        # 重启：与 lifespan 完全一致的初始化路径
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        before = time.time()
        dbmod.init_schema(conn)
        after = time.time()

        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        assert room["status"] == "paused"
        assert room["last_resume_at"] is None
        # 沿用既有结算公式：冻结值 ≈ 停机前运行段墙钟 100s
        assert 100_000 <= room["elapsed_ms"] < 100_500, room["elapsed_ms"]
        frozen = room["elapsed_ms"]

        clocks = clock_events(conn, rid)
        assert len(clocks) == 2  # started + 恢复追加的 paused
        rec = clocks[-1]
        payload = json.loads(rec["payload"])
        # 与手动暂停（pause_room）落库的载荷格式逐字段一致
        assert payload == {"action": "paused", "status": "paused",
                          "elapsed_ms": frozen}
        assert before <= rec["created_at"] <= after  # created_at = 本次恢复时刻
        conn.close()
    print("ok: restart freezes running room and appends matching paused clock event")


def test_idempotent_and_skips_idle_paused():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        now = time.time()
        # running：将被冻结并追加事件
        r_run = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV010','运行','running',5000,?,?)",
            (now - 5, now)).lastrowid
        add_event(conn, r_run, now - 100, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})
        # paused：本就有一条手动 paused 事件，重启不得再追加
        r_paused = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV011','已暂停','paused',7000,NULL,?)", (now,)).lastrowid
        add_event(conn, r_paused, now - 50, "clock",
                  {"action": "paused", "status": "paused", "elapsed_ms": 7000})
        # idle：从无时钟事件，重启不得追加
        r_idle = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV012','待机','idle',0,NULL,?)", (now,)).lastrowid
        conn.commit()

        dbmod.init_schema(conn)
        # 再初始化一次（模拟再次重启）：不得重复追加
        dbmod.init_schema(conn)

        run_clocks = clock_events(conn, r_run)
        assert len(run_clocks) == 2
        assert json.loads(run_clocks[-1]["payload"])["action"] == "paused"
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (r_run,)).fetchone()
        assert room["status"] == "paused"
        assert json.loads(run_clocks[-1]["payload"])["elapsed_ms"] == room["elapsed_ms"]

        paused_clocks = clock_events(conn, r_paused)
        assert len(paused_clocks) == 1  # 只有原来的手动 paused
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (r_paused,)).fetchone()
        assert room["status"] == "paused" and room["elapsed_ms"] == 7000

        assert len(clock_events(conn, r_idle)) == 0
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (r_idle,)).fetchone()
        assert room["status"] == "idle"
        conn.close()
    print("ok: recovery event appended once; idle/paused rooms untouched on repeat init")


def test_timeline_freezes_then_resumes_from_frozen_value():
    """重启后暂停期间的回执挂靠冻结值（不虚增时延）；继续后从冻结值累计。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        t_start = time.time() - 100
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV020','时间线','running',0,?,?)",
            (t_start, t_start)).lastrowid
        g1 = conn.execute(
            "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
            (rid, "红队", "RED020", t_start)).lastrowid
        c1 = conn.execute(
            "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id)"
            " VALUES (?,?,?,?,?)", (rid, 1, 10, "告警", None)).lastrowid
        conn.commit()
        add_event(conn, rid, t_start, "group_added",
                  {"id": g1, "name": "红队", "join_code": "RED020"})
        add_event(conn, rid, t_start, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})
        add_event(conn, rid, t_start + 10, "card_revealed",
                  {"id": c1, "seq": 1, "trigger_seconds": 10, "title": "告警",
                   "content": "", "group_id": None, "revealed": True,
                   "revealed_at_ms": 10000, "answers_locked": False})

        dbmod.init_schema(conn)  # 重启冻结
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        frozen = room["elapsed_ms"]
        assert room["status"] == "paused"
        restart_at = clock_events(conn, rid)[-1]["created_at"]

        # 重启后、主持人继续前（暂停期间）墙钟又过 30s：提交回执
        ack = add_event(conn, rid, restart_at + 30, "receipt_updated",
                        {"card_id": c1, "group_id": g1, "acked": True})

        actions, rounds, timeline = rv.build_timeline(timeline_rows(conn, rid), frozen)
        vtime = {it["id"]: it["vtime_ms"] for it in timeline}
        # 关键：暂停期间回执的虚拟时间停在冻结值，不随 30s 墙钟推进
        assert vtime[ack] == frozen, vtime[ack]
        assert [a["action"] for a in actions] == ["started", "paused"]
        assert actions[-1]["vtime_ms"] == frozen

        # 时延面板：回执时延 = 冻结值 − 揭示虚拟 10s，而不是再虚增 30s 墙钟
        rep = lat.build_latency_report(conn, rid, 0, ack, frozen)
        card = rep["groups"][0]["cards"][0]
        assert card["receipt_latency_ms"] == frozen - 10_000
        assert card["answer_latency_ms"] is None

        # 主持人继续运行：从冻结值累计，运行 2s 后提交答复
        t_resume = restart_at + 60
        add_event(conn, rid, t_resume, "clock",
                  {"action": "resumed", "status": "running", "elapsed_ms": frozen})
        ans = add_event(conn, rid, t_resume + 2, "answer_updated",
                        {"card_id": c1, "group_id": g1, "body": "处置", "revision": 1,
                         "updated_at": t_resume + 2})
        elapsed_now = frozen + 2000
        actions, rounds, timeline = rv.build_timeline(timeline_rows(conn, rid), elapsed_now)
        vtime = {it["id"]: it["vtime_ms"] for it in timeline}
        assert vtime[ack] == frozen           # 早先暂停期间的回执仍锚在冻结值
        assert vtime[ans] == frozen + 2000    # 继续后从冻结值接着累计
        assert [a["action"] for a in actions] == ["started", "paused", "resumed"]
        assert rounds[-1]["end_ms"] == frozen + 2000

        rep = lat.build_latency_report(conn, rid, 0, ans, elapsed_now)
        card = rep["groups"][0]["cards"][0]
        assert card["receipt_latency_ms"] == frozen - 10_000
        assert card["answer_latency_ms"] == frozen + 2000 - 10_000
        conn.close()
    print("ok: timeline freezes at recovery; post-restart pause excluded, resume accumulates")


def test_recovery_event_kept_in_current_round():
    """跨重置轮次隔离：恢复 paused 事件归属停机时所在轮次（第 1 轮）。"""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.db")
        conn = open_db(path)
        t0 = time.time() - 200
        rid = conn.execute(
            "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
            " VALUES ('RCV030','跨轮','running',0,?,?)",
            (time.time() - 30, t0)).lastrowid
        conn.commit()
        add_event(conn, rid, t0, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})
        add_event(conn, rid, t0 + 50, "reset",
                  {"action": "reset", "status": "idle", "elapsed_ms": 0})
        # 第二轮再次开始，随后停机
        t1 = time.time() - 30
        add_event(conn, rid, t1, "clock",
                  {"action": "started", "status": "running", "elapsed_ms": 0})

        dbmod.init_schema(conn)
        room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
        frozen = room["elapsed_ms"]

        actions, rounds, _ = rv.build_timeline(timeline_rows(conn, rid), frozen)
        assert [(a["action"], a["round"]) for a in actions] == [
            ("started", 0), ("reset", 0), ("started", 1), ("paused", 1),
        ]
        assert rounds[0]["ended"] is True
        # 第 0 轮终点 = reset 时运行段结算值（started 后 50 墙钟秒）
        reset_action = next(a for a in actions if a["action"] == "reset")
        assert 49_999 <= rounds[0]["end_ms"] == reset_action["vtime_ms"] <= 50_001
        assert rounds[1]["ended"] is False and rounds[1]["end_ms"] == frozen
        conn.close()
    print("ok: recovery paused event belongs to the round that was running at shutdown")


if __name__ == "__main__":
    test_running_room_frozen_with_paused_event()
    test_idempotent_and_skips_idle_paused()
    test_timeline_freezes_then_resumes_from_frozen_value()
    test_recovery_event_kept_in_current_round()
    print("ALL RECOVERY TESTS PASSED")
