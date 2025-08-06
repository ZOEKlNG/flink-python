"""Application level aggregation."""
from __future__ import annotations

from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor

from ..common.types import PacketSequence, SampleFile


class AppAggregator(KeyedProcessFunction):
    """Aggregate PacketSequence into SampleFile per app_id."""

    def __init__(
        self,
        queue_size: int = 100,
        emit_size: int = 50,
        timeout_ms: int = 60_000,
    ):
        self.queue_size = queue_size
        self.emit_size = emit_size
        self.timeout_ms = timeout_ms

    def open(self, runtime_context: RuntimeContext):
        descriptor = MapStateDescriptor(
            "app_queues", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
        )
        self.app_queues = runtime_context.get_map_state(descriptor)

    def process_element(
        self,
        value: PacketSequence,
        ctx: KeyedProcessFunction.Context,
    ):
        q = list(self.app_queues.get(value.app_id) or [])
        q.append(value)
        if len(q) >= self.queue_size:
            q.sort(key=lambda x: x.timestamp)
            emit = q[: self.emit_size]
            remaining = q[self.emit_size :]
            if remaining:
                self.app_queues.put(value.app_id, remaining)
            else:
                self.app_queues.remove(value.app_id)
            yield SampleFile(value.user_id, "", value.app_id, emit)
        else:
            self.app_queues.put(value.app_id, q)
            timer = ctx.timer_service().current_processing_time() + self.timeout_ms
            ctx.timer_service().register_processing_time_timer(timer)

    def on_timer(
        self,
        timestamp: int,
        ctx: KeyedProcessFunction.OnTimerContext,
    ):
        key = ctx.get_current_key()
        q = list(self.app_queues.get(key) or [])
        if q:
            q.sort(key=lambda x: x.timestamp)
            self.app_queues.remove(key)
            yield SampleFile(
                user_id=q[0].user_id if q else "",
                user_name="",
                app_id=key,
                sequences=q,
            )
