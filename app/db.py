"""SQLite 存储层：连接、建表、启动时初始化。

数据库文件路径由环境变量 DATABASE_PATH 指定，默认 /data/inject.db。
所有写操作都在 app.main 的 state_lock 下执行（单事件循环内），
因此这里使用单连接即可。
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

DB_PATH = os.environ.get("DATABASE_PATH", "/data/inject.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS rooms (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    code           TEXT NOT NULL UNIQUE,
    name           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'idle',      -- idle | running | paused
    elapsed_ms     INTEGER NOT NULL DEFAULT 0,        -- 已累计的虚拟时钟毫秒（暂停时冻结值）
    last_resume_at REAL,                              -- 最近一次进入 running 的墙钟时间（秒）
    created_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS groups (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    join_code  TEXT NOT NULL UNIQUE,                  -- 参与者入场凭证，与房间码同一命名空间
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS cards (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id         INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,                 -- 房间内稳定序号，揭示次序的决胜键
    trigger_seconds INTEGER NOT NULL,                 -- 相对演练开始的触发秒数
    title           TEXT NOT NULL,
    content         TEXT NOT NULL DEFAULT '',
    group_id        INTEGER REFERENCES groups(id),    -- NULL = 全员；否则仅该参与组可见
    revealed        INTEGER NOT NULL DEFAULT 0,
    revealed_at_ms  INTEGER,                          -- 揭示时的虚拟时钟毫秒
    depends_on_card_id  INTEGER REFERENCES cards(id), -- 回执门槛：前置卡片；NULL = 无门槛
    gate_timeout_seconds INTEGER,                     -- 虚拟时钟超时秒数（自触发时刻起算）；NULL = 不设超时
    timed_out       INTEGER NOT NULL DEFAULT 0,       -- 1 = 因门槛超时而被揭示
    answers_locked  INTEGER NOT NULL DEFAULT 0,       -- 1 = 主持人已锁定本卡处置答复（参与者只读）
    UNIQUE (room_id, seq)
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,     -- 全局单调递增事件 ID
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,                         -- JSON
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    card_id    INTEGER NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    group_id   INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    created_at REAL NOT NULL,
    UNIQUE (card_id, group_id)                        -- 同一参与组对同一卡片仅保留一条回执
);

CREATE TABLE IF NOT EXISTS answers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    card_id    INTEGER NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    group_id   INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    body       TEXT NOT NULL DEFAULT '',
    revision   INTEGER NOT NULL DEFAULT 1,            -- 乐观并发版本号，每次修改 +1
    updated_at REAL NOT NULL,
    UNIQUE (card_id, group_id)                        -- 同一参与组对同一卡片仅保留一份答复
);

CREATE TABLE IF NOT EXISTS observer_shares (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id          INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    token            TEXT NOT NULL UNIQUE,          -- 随机观察凭证（URL 安全，不可猜）
    group_ids        TEXT NOT NULL,                 -- JSON 数组：1~3 个参与组 id
    expires_at       REAL NOT NULL,                 -- 墙钟到期时刻（秒），到期即失效
    revoked          INTEGER NOT NULL DEFAULT 0,    -- 主持人撤销标记，撤销即失效
    expired_notified INTEGER NOT NULL DEFAULT 0,    -- ticker 是否已广播到期并断开连接
    created_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS anchors (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    title      TEXT NOT NULL,
    note       TEXT NOT NULL DEFAULT '',
    round      INTEGER NOT NULL,                  -- 记录时绑定的轮次（= 此前 reset 事件数）
    vtime_ms   INTEGER NOT NULL,                  -- 记录时绑定的虚拟时钟毫秒（暂停取冻结值）
    event_id   INTEGER NOT NULL,                  -- 绑定的事件水位：创建事件落库前的 MAX(events.id)，离线回放截断到此
    created_event_id INTEGER NOT NULL,            -- 其自身 anchor_created 事件 id；导出按它做水位截断
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- 现场决策投票：每条投票归属创建时轮次；重置只作废未结算投票，已结算结果按轮次保留。
CREATE TABLE IF NOT EXISTS polls (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id       INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    question      TEXT NOT NULL,
    round         INTEGER NOT NULL,                  -- 创建时轮次（= 此前 reset 事件数）
    deadline_ms   INTEGER NOT NULL,                  -- 截止虚拟毫秒（相对本轮开始），暂停冻结
    status        TEXT NOT NULL DEFAULT 'open',      -- open | settled | voided
    winner_option_id INTEGER,                        -- 结算获胜选项；平票按选项创建顺序（seq 最小）
    settled_at_ms INTEGER,                           -- 结算时的虚拟时钟毫秒
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS poll_options (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    poll_id INTEGER NOT NULL REFERENCES polls(id) ON DELETE CASCADE,
    seq     INTEGER NOT NULL,                        -- 选项创建顺序：平票决胜键
    text    TEXT NOT NULL,
    UNIQUE (poll_id, seq)
);

CREATE TABLE IF NOT EXISTS poll_groups (
    poll_id  INTEGER NOT NULL REFERENCES polls(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    PRIMARY KEY (poll_id, group_id)                  -- 仅指定参与组可投票/可见
);

CREATE TABLE IF NOT EXISTS poll_votes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    poll_id    INTEGER NOT NULL REFERENCES polls(id) ON DELETE CASCADE,
    group_id   INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    option_id  INTEGER NOT NULL REFERENCES poll_options(id) ON DELETE CASCADE,
    round      INTEGER NOT NULL,                     -- 投票时轮次：结算统计只数本轮票
    updated_at REAL NOT NULL,
    UNIQUE (poll_id, group_id)                       -- 每组只计一票，改投即原地更新
);

-- 应急车辆申请：每房间一个总量固定的车辆池（重置不清池）；
-- 每组同时仅一份申请，按受理顺序（queue_seq）立即分配或入队，取消后从队首补位。
CREATE TABLE IF NOT EXISTS vehicle_pools (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL UNIQUE REFERENCES rooms(id) ON DELETE CASCADE,
    total      INTEGER NOT NULL,                     -- 车辆池总量（1～20，创建后固定）
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS vehicle_requests (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    group_id   INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    quantity   INTEGER NOT NULL,                     -- 申请数量（1～5 且不超过池总量）
    status     TEXT NOT NULL,                        -- allocated | queued
    queue_seq  INTEGER NOT NULL,                     -- 受理顺序（房间内单调递增；队列与补位按它排序）
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (room_id, group_id)                       -- 每组同时仅一份申请，取消后才能重新申请
);

-- 现场情报核验板：主持人创建待核验命题并指定至少两个参与组；
-- 每组每命题仅一份最新报告（同组修改覆盖、可撤回），重置清空报告但保留命题。
CREATE TABLE IF NOT EXISTS intel_claims (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    statement  TEXT NOT NULL,                        -- 待核验命题
    round      INTEGER NOT NULL,                     -- 创建时轮次（仅作归属标记；命题跨重置保留）
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS intel_claim_groups (
    claim_id INTEGER NOT NULL REFERENCES intel_claims(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    PRIMARY KEY (claim_id, group_id)                 -- 仅指定参与组（≥2）可见命题/提交报告
);

CREATE TABLE IF NOT EXISTS intel_reports (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    claim_id   INTEGER NOT NULL REFERENCES intel_claims(id) ON DELETE CASCADE,
    group_id   INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    stance     TEXT NOT NULL,                        -- support | oppose
    rationale  TEXT NOT NULL,                        -- 简短依据（1～500 字）
    revision   INTEGER NOT NULL DEFAULT 1,           -- 同组每次修改 +1
    updated_at REAL NOT NULL,
    UNIQUE (claim_id, group_id)                      -- 每组每命题仅一份最新报告：
                                                     -- 同组多次提交绝不计为多份证据
);

CREATE INDEX IF NOT EXISTS idx_cards_due ON cards(room_id, revealed, trigger_seconds, seq);
CREATE INDEX IF NOT EXISTS idx_cards_group ON cards(room_id, group_id);
CREATE INDEX IF NOT EXISTS idx_cards_gate ON cards(depends_on_card_id);
CREATE INDEX IF NOT EXISTS idx_groups_room ON groups(room_id);
CREATE INDEX IF NOT EXISTS idx_events_room ON events(room_id, id);
CREATE INDEX IF NOT EXISTS idx_receipts_room ON receipts(room_id, card_id);
CREATE INDEX IF NOT EXISTS idx_receipts_group ON receipts(group_id);
CREATE INDEX IF NOT EXISTS idx_answers_room ON answers(room_id, card_id);
CREATE INDEX IF NOT EXISTS idx_observer_shares_room ON observer_shares(room_id);
CREATE INDEX IF NOT EXISTS idx_anchors_room ON anchors(room_id, round, vtime_ms, id);
CREATE INDEX IF NOT EXISTS idx_polls_room ON polls(room_id, round, status);
CREATE INDEX IF NOT EXISTS idx_poll_groups_group ON poll_groups(group_id);
CREATE INDEX IF NOT EXISTS idx_poll_votes_poll ON poll_votes(poll_id, group_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_requests_room ON vehicle_requests(room_id, status, queue_seq);
CREATE INDEX IF NOT EXISTS idx_intel_claims_room ON intel_claims(room_id, id);
CREATE INDEX IF NOT EXISTS idx_intel_claim_groups_group ON intel_claim_groups(group_id);
CREATE INDEX IF NOT EXISTS idx_intel_reports_claim ON intel_reports(claim_id, group_id);
CREATE INDEX IF NOT EXISTS idx_intel_reports_room ON intel_reports(room_id, claim_id);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """旧库就地升级：cards 表补缺列。

    - group_id：既有卡片一律视为全员；
    - depends_on_card_id / gate_timeout_seconds / timed_out：回执门槛，
      既有卡片一律视为无门槛；
    - answers_locked：处置答复锁定，既有卡片一律视为未锁定。

    必须在 executescript(SCHEMA) 之前执行：SCHEMA 里的 idx_cards_group、
    idx_cards_gate 索引依赖这些列，旧库需先补列才能建索引。

    复盘锚点表 anchors 与现场决策投票表 polls/poll_options/poll_groups/
    poll_votes 均为全新表（answers / observer_shares 同款），由
    SCHEMA 中的 CREATE TABLE IF NOT EXISTS 直接在旧库补齐，无需额外迁移；
    旧库既有演练视为没有任何锚点与投票。重置不删锚点与已结算投票，
    升级后仍按所属轮次保留。
    应急车辆申请表 vehicle_pools / vehicle_requests 同为全新表，随 SCHEMA
    在旧库直接补齐；旧库演练视为尚未创建车辆池、没有任何申请。
    现场情报核验板表 intel_claims / intel_claim_groups / intel_reports
    同为全新表，随 SCHEMA 在旧库直接补齐；旧库演练视为没有任何命题与报告。
    """
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cards'"
    ).fetchone()
    if table is None:
        return  # 全新库：由 SCHEMA 一并建好，无需迁移
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(cards)")}
    # 下列列在 SCHEMA 建索引（idx_cards_due 等）时必须已存在，旧库先补齐。
    # 最早版本的 cards 表只有 id/room_id/seq/trigger_seconds/title/content，
    # revealed/revealed_at_ms 与 group_id 等都是后续演进加入的。
    if "revealed" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN revealed INTEGER NOT NULL DEFAULT 0")
    if "revealed_at_ms" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN revealed_at_ms INTEGER")
    if "group_id" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN group_id INTEGER REFERENCES groups(id)")
    if "depends_on_card_id" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN depends_on_card_id INTEGER REFERENCES cards(id)")
    if "gate_timeout_seconds" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN gate_timeout_seconds INTEGER")
    if "timed_out" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN timed_out INTEGER NOT NULL DEFAULT 0")
    if "answers_locked" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN answers_locked INTEGER NOT NULL DEFAULT 0")
    conn.commit()


def connect() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """建表，并把进程停止时仍处于 running 的房间安全冻结为 paused。

    虚拟时钟在暂停时不推进：重启后这些房间保持暂停，已流逝时间按
    停机前的墙钟差结算进 elapsed_ms，不会丢失也不会偷偷前进。
    """
    _migrate(conn)
    conn.executescript(SCHEMA)
    now = time.time()
    rows = conn.execute("SELECT * FROM rooms WHERE status = 'running'").fetchall()
    for row in rows:
        elapsed = row["elapsed_ms"]
        if row["last_resume_at"] is not None:
            elapsed += int((now - row["last_resume_at"]) * 1000)
        conn.execute(
            "UPDATE rooms SET status = 'paused', elapsed_ms = ?, last_resume_at = NULL WHERE id = ?",
            (max(0, elapsed), row["id"]),
        )
    conn.commit()
