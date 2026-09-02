# -*- coding: utf-8 -*-
"""CCTV 作業流程監控（cctv 作業流程監控.py）

架構對應 FLOWCHART.md：

    執行緒 A  FrameGrabber    常駐影像迴圈，永遠只保留最新一張 frame
    執行緒 B  ControlAPI      /start /stop /status，界定監控區間
    執行緒 C  WorkflowEngine  多步驟流程依序執行（擷取→辨識→判斷→計時）

AI 模型的部分以 Detector 介面隔離，換成 YOLO / 自訓模型只要實作 infer()。

用法：
    python detect.py --source rtsp://... --port 8000
    curl -X POST localhost:8000/start -d @workflow.json
    curl -X POST localhost:8000/stop
"""

import argparse
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import cv2
except ImportError:  # 允許在沒有 OpenCV 的環境下先檢視流程結構
    cv2 = None


# --------------------------------------------------------------------------
# 執行緒 A：影像擷取（常駐，不隨 start/stop 起停）
# --------------------------------------------------------------------------
class FrameGrabber(threading.Thread):
    """持續讀取串流，只保留最新一張 frame。

    刻意丟棄舊幀：辨識端慢於串流時，取到的仍是「現在」的畫面，
    不會累積延遲。
    """

    def __init__(self, source, reconnect_delay=2.0, max_reconnect_delay=30.0):
        super().__init__(daemon=True, name="FrameGrabber")
        self.source = source
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self._lock = threading.Lock()
        self._frame = None
        self._ts = 0.0
        self._running = threading.Event()
        self._running.set()

    def run(self):
        delay = self.reconnect_delay
        while self._running.is_set():
            cap = cv2.VideoCapture(self.source)
            if not cap.isOpened():
                time.sleep(delay)
                delay = min(delay * 2, self.max_reconnect_delay)  # 退避重連
                continue
            delay = self.reconnect_delay
            while self._running.is_set():
                ok, frame = cap.read()
                if not ok:
                    break                       # 串流中斷 → 跳出去重連
                with self._lock:
                    self._frame = frame         # 覆寫，不排隊
                    self._ts = time.time()
            cap.release()

    def latest(self):
        """回傳 (frame_copy, timestamp)；尚未有畫面時回 (None, 0)。"""
        with self._lock:
            if self._frame is None:
                return None, 0.0
            return self._frame.copy(), self._ts

    def stop(self):
        self._running.clear()


# --------------------------------------------------------------------------
# AI 辨識介面
# --------------------------------------------------------------------------
@dataclass
class Detection:
    label: str
    score: float
    box: tuple  # (x1, y1, x2, y2)


class Detector:
    """接上實際模型的地方：載入權重、infer() 回傳 Detection 清單。"""

    def __init__(self, weights=None):
        self.weights = weights
        self.model = None  # e.g. YOLO(weights)

    def infer(self, frame, model_name=None):
        raise NotImplementedError("接上實際模型後實作此方法")


class DummyDetector(Detector):
    """沒有模型時的佔位實作，讓流程可以先跑通。"""

    def infer(self, frame, model_name=None):
        return []


# --------------------------------------------------------------------------
# 步驟定義（動態疊加：一次 /start 可帶入任意數量的步驟）
# --------------------------------------------------------------------------
@dataclass
class Step:
    name: str
    model: str = "default"
    roi: tuple = None                     # (x1, y1, x2, y2)，None = 全畫面
    rule: dict = field(default_factory=dict)
    timeout: float = 120.0                # 該步驟最長等待秒數
    interval: float = 0.2                 # 兩次辨識間隔
    max_frame_age: float = 2.0            # frame 超過這個秒數視為過期
    on_fail: str = "abort"                # abort / skip / retry
    max_retry: int = 0

    @classmethod
    def from_dict(cls, d):
        known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        if known.get("roi"):
            known["roi"] = tuple(known["roi"])
        return cls(**known)


@dataclass
class StepResult:
    name: str
    result: str            # PASS / FAIL / TIMEOUT / STOPPED / SKIPPED
    elapsed: float
    attempts: int = 1
    reason: str = ""
    evidence: str = ""


# --------------------------------------------------------------------------
# 邏輯判斷
# --------------------------------------------------------------------------
def crop(frame, roi):
    if not roi:
        return frame
    x1, y1, x2, y2 = roi
    return frame[y1:y2, x1:x2]


def rule_matched(detections, rule):
    """預設規則：指定類別、分數門檻、最少數量都滿足才算成立。"""
    target = rule.get("class")
    min_score = rule.get("min_score", 0.5)
    min_count = rule.get("min_count", 1)
    hits = [d for d in detections
            if (target is None or d.label == target) and d.score >= min_score]
    return len(hits) >= min_count


# --------------------------------------------------------------------------
# 執行緒 C：流程監控引擎
# --------------------------------------------------------------------------
class WorkflowEngine(threading.Thread):
    """依序執行 Step 1..N，每步驟：擷取畫面 → AI 辨識 → 邏輯判斷 → 計時。"""

    def __init__(self, session_id, steps, grabber, detector,
                 stop_event, session_timeout=1800.0, evidence_dir="evidence"):
        super().__init__(daemon=True, name="WorkflowEngine")
        self.session_id = session_id
        self.steps = steps
        self.grabber = grabber
        self.detector = detector
        self.stop_event = stop_event
        self.session_timeout = session_timeout
        self.evidence_dir = os.path.join(evidence_dir, session_id)

        self.status = "RUNNING"
        self.current_index = -1
        self.results = []
        self.started_at = time.time()
        self.ended_at = None

    # ---- 單一步驟 -------------------------------------------------------
    def run_step(self, step, attempt):
        step_start = time.time()
        deadline = step_start + step.timeout
        consecutive_needed = step.rule.get("consecutive", 1)
        consecutive = 0

        while True:
            if self.stop_event.is_set():
                return StepResult(step.name, "STOPPED", round(time.time() - step_start, 2),
                                  attempt, "收到 /stop")
            if time.time() >= deadline:
                return StepResult(step.name, "TIMEOUT", round(time.time() - step_start, 2),
                                  attempt, "步驟逾時")

            frame, ts = self.grabber.latest()
            if frame is None or (time.time() - ts) > step.max_frame_age:
                time.sleep(step.interval)          # 畫面尚未就緒或已過期
                continue

            detections = self.detector.infer(crop(frame, step.roi), step.model)

            if rule_matched(detections, step.rule):
                consecutive += 1
                if consecutive >= consecutive_needed:   # 連續 K 幀成立才判定通過
                    elapsed = round(time.time() - step_start, 2)
                    evidence = self.save_evidence(frame, step, detections)
                    return StepResult(step.name, "PASS", elapsed, attempt,
                                      evidence=evidence)
            else:
                consecutive = 0                    # 中斷即重新累計

            time.sleep(step.interval)

    def save_evidence(self, frame, step, detections):
        if cv2 is None:
            return ""
        os.makedirs(self.evidence_dir, exist_ok=True)
        for d in detections:
            x1, y1, x2, y2 = [int(v) for v in d.box]
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(frame, "%s %.2f" % (d.label, d.score), (x1, max(y1 - 6, 0)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
        path = os.path.join(self.evidence_dir, "%s.jpg" % step.name)
        cv2.imwrite(path, frame)
        return path

    # ---- 主迴圈 ---------------------------------------------------------
    def run(self):
        for idx, step in enumerate(self.steps):
            self.current_index = idx

            if self.stop_event.is_set():
                self.finish("ABORTED")
                return
            if time.time() - self.started_at > self.session_timeout:
                self.finish("FAILED")
                return

            attempt = 1
            while True:
                res = self.run_step(step, attempt)

                if res.result == "PASS":
                    self.results.append(res)
                    break
                if res.result == "STOPPED":
                    self.results.append(res)
                    self.finish("ABORTED")
                    return
                # FAIL / TIMEOUT → 依該步驟策略決定
                if step.on_fail == "retry" and attempt <= step.max_retry:
                    attempt += 1
                    continue
                self.results.append(res)
                if step.on_fail == "skip":
                    break
                self.finish("FAILED")
                return

        self.finish("COMPLETED")

    def finish(self, status):
        self.status = status
        self.ended_at = time.time()
        self.current_index = -1

    def report(self):
        end = self.ended_at or time.time()
        return {
            "session_id": self.session_id,
            "status": self.status,
            "current_step": (self.steps[self.current_index].name
                             if 0 <= self.current_index < len(self.steps) else None),
            "started_at": datetime.fromtimestamp(self.started_at).isoformat(),
            "ended_at": datetime.fromtimestamp(end).isoformat() if self.ended_at else None,
            "total_elapsed": round(end - self.started_at, 2),
            "steps": [asdict(r) for r in self.results],
        }


# --------------------------------------------------------------------------
# 執行緒 B：控制 API
# --------------------------------------------------------------------------
class Supervisor:
    """管理「只有 start 到 stop 這段期間才監控」的 Session 生命週期。"""

    def __init__(self, grabber, detector, default_steps=None):
        self.grabber = grabber
        self.detector = detector
        self.default_steps = default_steps or []
        self._lock = threading.Lock()
        self.engine = None
        self.stop_event = None
        self.last_report = None

    def start(self, payload):
        with self._lock:
            if self.engine and self.engine.is_alive():
                return False, {"error": "監控中，請先 /stop", **self.engine.report()}

            raw_steps = payload.get("steps") or self.default_steps
            if not raw_steps:
                return False, {"error": "沒有可執行的步驟"}

            steps = [Step.from_dict(s) for s in raw_steps]
            session_id = "%s-%s" % (datetime.now().strftime("%Y%m%d-%H%M%S"),
                                    uuid.uuid4().hex[:4])
            self.stop_event = threading.Event()
            self.engine = WorkflowEngine(
                session_id, steps, self.grabber, self.detector, self.stop_event,
                session_timeout=payload.get("session_timeout", 1800.0))
            self.engine.start()
            return True, {"session_id": session_id, "status": "RUNNING",
                          "step_count": len(steps)}

    def stop(self):
        with self._lock:
            if not self.engine:
                return False, {"error": "目前沒有監控中的 Session"}
            self.stop_event.set()
        self.engine.join(timeout=10.0)          # 等引擎在檢查點收斂
        self.last_report = self.engine.report()
        return True, self.last_report

    def status(self):
        if self.engine and self.engine.is_alive():
            return self.engine.report()
        return self.last_report or {"status": "IDLE"}


class Handler(BaseHTTPRequestHandler):
    supervisor = None

    def _json(self, code, body):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except ValueError:
            return self._json(400, {"error": "JSON 格式錯誤"})

        if self.path.rstrip("/") == "/start":
            ok, body = self.supervisor.start(payload)
        elif self.path.rstrip("/") == "/stop":
            ok, body = self.supervisor.stop()
        else:
            return self._json(404, {"error": "not found"})
        self._json(200 if ok else 409, body)

    def do_GET(self):
        if self.path.rstrip("/") in ("/status", ""):
            return self._json(200, self.supervisor.status())
        self._json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        pass  # 靜音 access log


# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="CCTV 作業流程監控")
    parser.add_argument("--source", default="0", help="RTSP URL 或攝影機索引")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--weights", default=None, help="AI 模型權重路徑")
    parser.add_argument("--workflow", default=None, help="預設流程 JSON 檔")
    args = parser.parse_args()

    if cv2 is None:
        raise SystemExit("需要 OpenCV：pip install opencv-python")

    source = int(args.source) if args.source.isdigit() else args.source
    grabber = FrameGrabber(source)
    grabber.start()                                   # 影像 loop 立即常駐運行

    detector = DummyDetector(args.weights)            # 換成實際模型即可

    default_steps = []
    if args.workflow:
        with open(args.workflow, encoding="utf-8") as f:
            default_steps = json.load(f).get("steps", [])

    Handler.supervisor = Supervisor(grabber, detector, default_steps)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print("CCTV 監控服務已啟動：http://0.0.0.0:%d  (POST /start, POST /stop, GET /status)"
          % args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        grabber.stop()


if __name__ == "__main__":
    main()
