# CCTV 作業流程監控 — 流程圖

主程式：`cctv_monitor/detect.py`（即「cctv 作業流程監控.py」，檔名改用 ASCII 以利 import）

## 0. 設計重點對應

| 需求 | 設計 |
| --- | --- |
| 視頻 loop 持續運行、不斷擷取最新 frame | `FrameGrabber` 常駐執行緒，讀到就覆蓋 `latest_frame`（丟棄舊幀，永遠只留最新一張，避免延遲累積） |
| start / stop 兩支 API，只監控這段期間 | `ControlAPI` 常駐執行緒；`/start` 建立 Session 並啟動流程引擎，`/stop` 設定 `stop_event`；引擎只在 Session 存活期間取用 frame 做辨識 |
| 支援多個動態流程疊加、依序執行 | `steps` 步驟表（可由 `/start` 帶入或設定檔載入），每個 Step = 擷取畫面 → AI 辨識 → 邏輯判斷 → 計時 |

三條執行緒彼此解耦：擷取不受辨識速度拖累，辨識不阻塞 API，`/stop` 能即時中斷。

---

## 1. 系統架構（三個常駐元件）

```mermaid
flowchart LR
    subgraph T1["執行緒 A：影像擷取（常駐，永不停止）"]
      A1["開啟 RTSP / CCTV 串流"] --> A2["cap.read 取得 frame"]
      A2 --> A3["覆寫 latest_frame<br/>只保留最新一張 + 時戳"]
      A3 --> A2
      A2 -.->|"讀取失敗"| A4["重連退避 backoff"] -.-> A1
    end

    subgraph T2["執行緒 B：控制 API（常駐）"]
      B1["POST /start<br/>帶入 steps 步驟表"]
      B2["POST /stop"]
      B3["GET /status"]
    end

    subgraph T3["執行緒 C：流程監控引擎<br/>僅在 Session 期間存在"]
      C1["依序執行 Step 1..N"]
    end

    A3 -.->|"共享記憶體<br/>latest_frame（加鎖）"| C1
    B1 ==>|"建立 Session + 啟動"| C1
    B2 ==>|"stop_event.set()"| C1
    C1 --> R[("Session 報告<br/>各步驟耗時 / 結果 / 存證影像")]
    B3 -.->|"查詢目前步驟"| C1
```

---

## 2. 主流程圖

```mermaid
flowchart TD
    S(["程式啟動"]) --> INIT["初始化：讀設定、載入 AI 模型、建立步驟表"]
    INIT --> CAP["啟動影像擷取執行緒<br/>持續更新 latest_frame"]
    CAP --> API["啟動 API 服務"]
    API --> IDLE{{"IDLE：等待 /start"}}

    IDLE -->|"未收到"| IDLE
    IDLE -->|"收到 /start"| NEW["建立 Session<br/>session_id、t_start、清空結果"]
    NEW --> LOAD["載入本次流程步驟 steps 1..N<br/>（可動態疊加，例如 6 步）"]
    LOAD --> I["i = 1"]

    I --> CHK{"收到 /stop<br/>或 Session 總逾時？"}
    CHK -->|"是"| ABORT["ABORTED：標記中止<br/>記錄中斷在第幾步"]
    CHK -->|"否"| STEP[["執行 Step i<br/>詳見『單一步驟流程』"]]

    STEP --> RES{"Step i 結果"}
    RES -->|"PASS"| NEXT{"i < N ？"}
    RES -->|"FAIL / TIMEOUT"| POL{"該步驟失敗策略<br/>on_fail"}

    POL -->|"retry 未達上限"| STEP
    POL -->|"skip（非必要步驟）"| NEXT
    POL -->|"abort（關鍵步驟）"| FAIL["FAILED：流程不通過"]

    NEXT -->|"是"| INC["i = i + 1"]
    INC --> CHK
    NEXT -->|"否"| DONE["COMPLETED：全部步驟完成"]

    DONE --> REPORT["產出報告：<br/>每步驟起訖時間、耗時、判定結果<br/>總作業時間、存證影像路徑"]
    ABORT --> REPORT
    FAIL --> REPORT
    REPORT --> CLEAN["釋放 Session、回到待命"]
    CLEAN --> IDLE
```

> 注意：擷取執行緒 **不隨 /stop 停止**，只有「是否被引擎取用來做辨識」有差別。這樣 `/start` 進來時第一幀就是即時畫面，不需暖機。

---

## 3. 單一步驟流程（Step i 內部）

每個步驟固定四段：擷取畫面 → AI 辨識 → 邏輯判斷 → 執行時間。

```mermaid
flowchart TD
    A(["進入 Step i"]) --> A0["step_start = now()<br/>套用該步驟參數：ROI、模型、規則、timeout_i、interval"]
    A0 --> B["擷取畫面：複製 latest_frame + 時戳"]
    B --> B1{"frame 有效且夠新鮮？<br/>now - ts < max_age"}
    B1 -->|"否"| WAIT
    B1 -->|"是"| C["前處理：ROI 裁切 / resize / 去雜訊"]
    C --> D["AI 辨識：模型推論<br/>輸出 boxes、class、score"]
    D --> E["邏輯判斷：套用該步驟規則<br/>例：目標類別存在 且 score ≥ 閾值<br/>且在 ROI 內 且 連續 K 幀成立"]
    E --> F{"條件成立？"}

    F -->|"是"| G["step_end = now()<br/>duration = step_end - step_start"]
    G --> H["存證：關鍵幀存檔 + 標註框 + 寫入步驟結果"]
    H --> P(["Step i = PASS"])

    F -->|"否"| TO{"已達 timeout_i<br/>或 stop_event 被設定？"}
    TO -->|"否"| WAIT["sleep(interval)<br/>降低 CPU / GPU 負載"]
    WAIT --> B
    TO -->|"是"| Q["記錄 duration 與失敗原因<br/>timeout / stopped"]
    Q --> R(["Step i = FAIL / TIMEOUT / STOPPED"])
```

---

## 4. Session 狀態機

```mermaid
stateDiagram-v2
    [*] --> IDLE : 服務啟動
    IDLE --> RUNNING : POST /start
    RUNNING --> STEP_RUNNING : 取出 Step i
    STEP_RUNNING --> STEP_RUNNING : PASS 且仍有下一步 / retry
    STEP_RUNNING --> COMPLETED : 全部步驟 PASS
    STEP_RUNNING --> FAILED : 關鍵步驟 FAIL 或 TIMEOUT
    STEP_RUNNING --> ABORTED : POST /stop
    COMPLETED --> IDLE : 產出報告、釋放 Session
    FAILED --> IDLE : 產出報告、釋放 Session
    ABORTED --> IDLE : 產出報告、釋放 Session
```

---

## 5. 時序圖（六步驟為例）

```mermaid
sequenceDiagram
    autonumber
    participant U as 上位系統 / 操作員
    participant API as ControlAPI
    participant ENG as WorkflowEngine
    participant CAP as FrameGrabber
    participant AI as AI 模型

    CAP-->>CAP: while True：持續更新 latest_frame（常駐）
    U->>API: POST /start {steps:[s1..s6]}
    API->>ENG: 建立 Session、啟動流程執行緒
    API-->>U: 200 {session_id}

    loop Step 1 ~ Step 6
        loop 直到條件成立或 timeout
            ENG->>CAP: get_latest_frame()
            CAP-->>ENG: frame + timestamp
            ENG->>AI: infer(ROI(frame))
            AI-->>ENG: detections
            ENG->>ENG: 規則判斷 / 連續 K 幀確認
        end
        ENG->>ENG: 記錄 step duration、存證影像
    end

    U->>API: POST /stop
    API->>ENG: stop_event.set()
    ENG->>ENG: 於當前步驟檢查點中斷
    ENG-->>API: Session 報告
    API-->>U: 200 {status, steps[], total_elapsed}
```

---

## 6. 步驟表格式（動態疊加）

`/start` 可直接帶入，或引用設定檔中預先定義的流程模板：

```json
{
  "workflow_id": "assembly_line_A",
  "session_timeout": 1800,
  "steps": [
    {
      "name": "01_人員就位",
      "roi": [0, 0, 1280, 720],
      "model": "person",
      "rule": {"class": "person", "min_score": 0.6, "min_count": 1, "consecutive": 5},
      "timeout": 120,
      "interval": 0.2,
      "on_fail": "abort"
    },
    { "name": "02_取料", "...": "同上結構" },
    { "name": "03_組裝", "...": "同上結構" },
    { "name": "04_鎖付", "...": "同上結構" },
    { "name": "05_檢查", "...": "同上結構" },
    { "name": "06_入庫", "...": "同上結構" }
  ]
}
```

| 欄位 | 用途 |
| --- | --- |
| `roi` | 該步驟只看畫面的哪一塊，減少誤判與運算量 |
| `model` / `rule` | AI 辨識用哪個模型、判定條件（類別、分數、數量、連續幀數） |
| `timeout` | 該步驟最長等待秒數，逾時即判 FAIL |
| `interval` | 兩次辨識間隔，控制取樣率與 GPU 負載 |
| `on_fail` | `abort`（關鍵步驟）／`skip`（非必要）／`retry`（可重試，含次數上限） |

## 7. 報告輸出

```json
{
  "session_id": "20260902-101533-a1b2",
  "status": "COMPLETED",
  "started_at": "2026-09-02T10:15:33",
  "ended_at": "2026-09-02T10:23:07",
  "total_elapsed": 454.2,
  "steps": [
    {"name": "01_人員就位", "result": "PASS", "elapsed": 12.4, "evidence": "evidence/.../01.jpg"},
    {"name": "02_取料",     "result": "PASS", "elapsed": 63.1, "evidence": "evidence/.../02.jpg"}
  ]
}
```
