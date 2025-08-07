from __future__ import annotations

from typing import Dict, List

from .models import PacketSequence, UnifiedLog


class SessionProcessor:
    """Aggregate raw logs into ordered packet sequences per session.

    The processor keeps an in-memory buffer for each session ``uid``. When a
    session times out or the buffer reaches a threshold, a ``PacketSequence`` is
    emitted.
    """

    def __init__(self, threshold: int = 100, batch_size: int = 50):
        self.threshold = threshold
        self.batch_size = batch_size
        self.session_buffer: Dict[str, List[UnifiedLog]] = {}

    def process(self, log: UnifiedLog) -> List[PacketSequence]:
        """Process a single ``UnifiedLog`` and return any generated sequences."""

        result: List[PacketSequence] = []
        buffer = self.session_buffer.setdefault(log.uid, [])
        buffer.append(log)

        if log.raw.get("is_timeout"):
            result.extend(self._flush_session(log.uid))
        elif len(buffer) >= self.threshold:
            result.extend(self._emit_batches(log.uid))

        return result

    def _emit_batches(self, uid: str) -> List[PacketSequence]:
        buffer = self.session_buffer[uid]
        buffer.sort(key=lambda l: l.timestamp)
        result: List[PacketSequence] = []
        while len(buffer) >= self.batch_size:
            batch = buffer[: self.batch_size]
            del buffer[: self.batch_size]
            result.append(
                PacketSequence(
                    user_id=batch[0].user_id,
                    app_id=batch[0].app_id,
                    protocol=batch[0].protocol,
                    timestamp=batch[0].timestamp,
                    packets=batch,
                )
            )
        return result

    def _flush_session(self, uid: str) -> List[PacketSequence]:
        buffer = self.session_buffer.pop(uid, [])
        if not buffer:
            return []
        buffer.sort(key=lambda l: l.timestamp)
        return [
            PacketSequence(
                user_id=buffer[0].user_id,
                app_id=buffer[0].app_id,
                protocol=buffer[0].protocol,
                timestamp=buffer[0].timestamp,
                packets=buffer,
            )
        ]
