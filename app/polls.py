"""现场决策投票 —— 存储与结算域（不依赖 fastapi，便于无第三方依赖测试）。

与 app.main 同一把 state_lock、同一个 SQLite 连接下被调用，因此本模块
不再自行加锁，只负责：

- 创建投票（2～4 个选项 + 指定参与组白名单 + 截止虚拟毫秒，归属当前轮次）
- 组投票 / 改投（UNIQUE(poll_id, group_id) 保证每组只计一票，改投原地更新）
- tick 结算：对「当前轮次、open、deadline_ms <= 虚拟时钟」的投票一次性
  结算——最高票获胜，平票按选项创建顺序（seq 最小）决定，结果写回 polls 行；
  结算只数本轮票，且每条投票只结算一次（status open → settled）
- 重置：未结算（open）投票作废（status → voided、票删除），已结算结果
  按轮次保留不动

暂停冻结截止计时由调用方保证：tick 只扫描 running 房间并以
main.room_elapsed_ms（暂停取冻结值）作为当前虚拟毫秒；重启时 init_schema
把 running 房间安全冻结为 paused，到期扫描自然暂停，状态全部由持久化行推导。
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

MIN_OPTIONS = 2
MAX_OPTIONS = 4
MAX_DEADLINE_SECONDS = 24 * 3600


class PollError(Exception):
    """投票域校验错误。main.py 捕获后转成对应 HTTP 状态码（detail 为消息文本）。"""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# ---------------------------------------------------------------- 创建

def create_poll(
    conn: sqlite3.Connection,
    room_id: int,
    question: str,
    options: list[str],
    group_ids: list[int],
    deadline_ms: int,
    round_idx: int,
    *,
    now: float | None = None,
) -> sqlite3.Row:
    """创建一条 open 投票并写入选项、参与组白名单。调用方须先完成输入校验
    （选项 2～4 个非空、参与组属于本房间、截止虚拟毫秒晚于当前虚拟时钟）。"""
    now = time.time() if now is None else now
    cur = conn.execute(
        "INSERT INTO polls (room_id, question, round, deadline_ms, status, created_at)"
        " VALUES (?, ?, ?, ?, 'open', ?)",
        (room_id, question, round_idx, deadline_ms, now),
    )
    poll_id = cur.lastrowid
    for seq, text in enumerate(options):
        conn.execute(
            "INSERT INTO poll_options (poll_id, seq, text) VALUES (?, ?, ?)",
            (poll_id, seq, text),
        )
    for gid in dict.fromkeys(group_ids):  # 去重保序
        conn.execute(
            "INSERT INTO poll_groups (poll_id, group_id) VALUES (?, ?)",
            (poll_id, gid),
        )
    conn.commit()
    return conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()


def get_poll(conn: sqlite3.Connection, room_id: int, poll_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM polls WHERE id = ? AND room_id = ?", (poll_id, room_id)
    ).fetchone()


def poll_group_ids(conn: sqlite3.Connection, poll_id: int) -> list[int]:
    return [r["group_id"] for r in conn.execute(
        "SELECT group_id FROM poll_groups WHERE poll_id = ? ORDER BY group_id", (poll_id,)
    )]


def poll_options(conn: sqlite3.Connection, poll_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM poll_options WHERE poll_id = ? ORDER BY seq", (poll_id,)
    ).fetchall()


# ---------------------------------------------------------------- 投票 / 改投

@dataclass
class VoteResult:
    poll_id: int
    group_id: int
    option_id: int
    first_vote: bool   # False = 改投（原地更新既有票）


def cast_vote(
    conn: sqlite3.Connection,
    poll: sqlite3.Row,
    group_id: int,
    option_id: int,
    *,
    now: float | None = None,
) -> VoteResult:
    """指定组提交或改投。调用方须先验证：组在白名单内、投票 open、
    选项属于本投票；截止判定（虚拟时钟）也由调用方做，错误统一抛 PollError。"""
    now = time.time() if now is None else now
    existing = conn.execute(
        "SELECT id FROM poll_votes WHERE poll_id = ? AND group_id = ?",
        (poll["id"], group_id),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO poll_votes (room_id, poll_id, group_id, option_id, round, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (poll["room_id"], poll["id"], group_id, option_id, poll["round"], now),
        )
        first_vote = True
    else:
        conn.execute(
            "UPDATE poll_votes SET option_id = ?, round = ?, updated_at = ? WHERE id = ?",
            (option_id, poll["round"], now, existing["id"]),
        )
        first_vote = False
    conn.commit()
    return VoteResult(poll_id=poll["id"], group_id=group_id,
                      option_id=option_id, first_vote=first_vote)


# ---------------------------------------------------------------- tick 结算

@dataclass
class SettleResult:
    poll: sqlite3.Row
    winner: sqlite3.Row
    tallies: dict[int, int]   # option_id -> 票数（未得票选项为 0）


def settle_due_polls(
    conn: sqlite3.Connection,
    room_id: int,
    round_idx: int,
    elapsed_ms: int,
) -> list[SettleResult]:
    """在锁内对当前轮次所有到期且仍 open 的投票一次性结算。

    每条投票只可能被结算一次：先按 status='open' 取出、写回 settled 并
    commit，后续 tick（以及重启后）不会再次结算。最高票获胜；平票按
    选项创建顺序（seq 最小）决定——全员零票时选项 0 获胜，确定性、可复现。
    """
    due = conn.execute(
        """SELECT * FROM polls
           WHERE room_id = ? AND round = ? AND status = 'open' AND deadline_ms <= ?
           ORDER BY deadline_ms, id""",
        (room_id, round_idx, elapsed_ms),
    ).fetchall()
    results: list[SettleResult] = []
    for poll in due:
        options = conn.execute(
            "SELECT * FROM poll_options WHERE poll_id = ? ORDER BY seq", (poll["id"],)
        ).fetchall()
        tallies = {opt["id"]: 0 for opt in options}
        for row in conn.execute(
            "SELECT option_id, COUNT(*) AS c FROM poll_votes"
            " WHERE poll_id = ? AND round = ? GROUP BY option_id",
            (poll["id"], round_idx),
        ):
            if row["option_id"] in tallies:  # 防御：孤立 option_id 不计入
                tallies[row["option_id"]] = row["c"]
        # max 在票数相等时保留先遇到者；options 已按 seq 升序 → 平票取创建最早选项
        winner = max(options, key=lambda opt: (tallies[opt["id"]], -opt["seq"]))
        conn.execute(
            "UPDATE polls SET status = 'settled', winner_option_id = ?, settled_at_ms = ?"
            " WHERE id = ? AND status = 'open'",
            (winner["id"], elapsed_ms, poll["id"]),
        )
        conn.commit()
        fresh = conn.execute("SELECT * FROM polls WHERE id = ?", (poll["id"],)).fetchone()
        results.append(SettleResult(poll=fresh, winner=winner, tallies=tallies))
    return results


# ---------------------------------------------------------------- 重置

def void_open_polls(conn: sqlite3.Connection, room_id: int) -> list[int]:
    """重置：作废当前所有未结算（open）投票并删除其选票；已结算结果保留。

    返回被作废的投票 id 列表（调用方据此逐条广播 poll_voided，仅主持人可见）。
    选项与参与组白名单随投票行保留（复盘作废投票的元数据仍可查）。
    """
    open_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM polls WHERE room_id = ? AND status = 'open' ORDER BY id",
        (room_id,),
    ).fetchall()]
    for poll_id in open_ids:
        conn.execute("DELETE FROM poll_votes WHERE poll_id = ?", (poll_id,))
        conn.execute(
            "UPDATE polls SET status = 'voided' WHERE id = ? AND status = 'open'",
            (poll_id,),
        )
    conn.commit()
    return open_ids


# ---------------------------------------------------------------- 读取

def vote_of_group(conn: sqlite3.Connection, poll_id: int, group_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM poll_votes WHERE poll_id = ? AND group_id = ?",
        (poll_id, group_id),
    ).fetchone()
