"""Session level aggregation."""
from __future__ import annotations

from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor

from ..common.types import PacketSequence, UnifiedLog


class SessionAggregator(KeyedProcessFunction):
    """Aggregate UnifiedLog into PacketSequence per session uid."""

    def __init__(self, buffer_size: int = 100, emit_size: int = 50):
        self.buffer_size = buffer_size
        self.emit_size = emit_size

    def open(self, runtime_context: RuntimeContext):
        descriptor = MapStateDescriptor(
            "session_buffer", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
        )
        self.session_buffer = runtime_context.get_map_state(descriptor)

    def process_element(self, value: UnifiedLog, ctx: KeyedProcessFunction.Context):
        logs = list(self.session_buffer.get(value.uid) or [])
        if value.metadata.get("is_timeout"):
            if logs:
                logs.sort(key=lambda x: x.timestamp)
                yield PacketSequence(
                    value.user_id, value.app_id, logs[0].timestamp, logs
                )
            self.session_buffer.remove(value.uid)
            return
        logs.append(value)
        if len(logs) >= self.buffer_size:
            logs.sort(key=lambda x: x.timestamp)
            seq_logs = logs[: self.emit_size]
            remaining = logs[self.emit_size :]
            if remaining:
                self.session_buffer.put(value.uid, remaining)
            else:
                self.session_buffer.remove(value.uid)
            yield PacketSequence(
                value.user_id, value.app_id, seq_logs[0].timestamp, seq_logs
            )
        else:
            self.session_buffer.put(value.uid, logs)
