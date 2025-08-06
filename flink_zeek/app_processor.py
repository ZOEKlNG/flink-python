from __future__ import annotations

from typing import Dict, List

from .models import PacketSequence, SampleFile


class ApplicationProcessor:
    """Aggregate packet sequences into sample files per application."""

    def __init__(self, threshold: int = 100, batch_size: int = 50):
        self.threshold = threshold
        self.batch_size = batch_size
        self.app_queues: Dict[str, List[PacketSequence]] = {}
        self.user_info: Dict[str, str] = {}

    def process(self, seq: PacketSequence, user_name: str | None = None) -> List[SampleFile]:
        if user_name:
            self.user_info[seq.user_id] = user_name

        queue = self.app_queues.setdefault(seq.app_id, [])
        queue.append(seq)

        result: List[SampleFile] = []
        if len(queue) >= self.threshold:
            result.extend(self._emit_batches(seq.app_id))

        return result

    def flush_app(self, app_id: str) -> List[SampleFile]:
        """Flush all remaining sequences for ``app_id`` as a single sample file."""
        queue = self.app_queues.pop(app_id, [])
        if not queue:
            return []
        queue.sort(key=lambda s: s.timestamp)
        return [self._create_sample_file(app_id, queue)]

    def _emit_batches(self, app_id: str) -> List[SampleFile]:
        queue = self.app_queues[app_id]
        queue.sort(key=lambda s: s.timestamp)
        result: List[SampleFile] = []
        while len(queue) >= self.batch_size:
            batch = queue[: self.batch_size]
            del queue[: self.batch_size]
            result.append(self._create_sample_file(app_id, batch))
        return result

    def _create_sample_file(self, app_id: str, seqs: List[PacketSequence]) -> SampleFile:
        user_id = seqs[0].user_id
        user_name = self.user_info.get(user_id, "")
        return SampleFile(
            user_id=user_id,
            user_name=user_name,
            app_id=app_id,
            generation_timestamp=seqs[-1].timestamp,
            packet_sequences=seqs,
            metadata={},
        )
