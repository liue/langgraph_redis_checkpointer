import random
from typing import Any, Sequence
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    CheckpointTuple,
    WRITES_IDX_MAP
)
from .util import (
    EMPTY_ID_SENTINEL,
    from_safe_id,
    from_safe_str,
    to_safe_id,
    to_safe_str,
)


class BaseRedisSaver(BaseCheckpointSaver[str]):
    def get_next_version(self, current: str | None, channel=None) -> str:
        """Generate the next version ID for a channel.

        This method creates a new version identifier for a channel based on its current version.
        Compatible with PostgreSQL implementation.

        Args:
            current (Optional[str]): The current version identifier of the channel.

        Returns:
            str: The next version identifier, which is guaranteed to be monotonically increasing.
        """
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def _make_checkpoint_key(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
        """Generate Redis key for checkpoint data."""
        return f'checkpoint:{to_safe_id(thread_id)}:{to_safe_str(checkpoint_ns)}:{to_safe_id(checkpoint_id)}'

    def _make_writes_key(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
        """Generate Redis key for writes data."""
        return f'writes:{to_safe_id(thread_id)}:{to_safe_str(checkpoint_ns)}:{to_safe_id(checkpoint_id)}'

    def _make_index_key(self, thread_id: str, checkpoint_ns: str) -> str:
        """Generate Redis key for checkpoint index."""
        return f'idx:{to_safe_id(thread_id)}:{to_safe_str(checkpoint_ns)}'

    def _make_scan_patterns(self, thread_id: str) -> list[str]:
        """Generate scan patterns for deleting thread data."""
        return [
            # checkpoint:{thread_id}:*:*
            f'checkpoint:{to_safe_id(thread_id)}:*',
            f'writes:{to_safe_id(thread_id)}:*',      # writes:{thread_id}:*:*
            f'idx:{to_safe_id(thread_id)}:*'          # idx:{thread_id}:*
        ]

    def dumps(self, value: Any) -> bytes:
        encoding, data = self.serde.dumps_typed(value)
        serialized_value = f"{encoding}:".encode() + data
        return serialized_value

    def loads(self, serialized_value: bytes) -> Any:
        encoding, data = serialized_value.split(b":", 1)
        return self.serde.loads_typed((encoding.decode(), data))

    def _build_checkpoint_tuple(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str,
                                checkpoint_data: dict, writes_data: list) -> CheckpointTuple:
        """Build CheckpointTuple from Redis data."""
        # 反序列化checkpoint和metadata
        checkpoint = self.loads(checkpoint_data[b'checkpoint'])
        metadata = self.loads(checkpoint_data[b'metadata'])
        parent_checkpoint_id = checkpoint_data.get(
            b'parent_checkpoint_id', b'').decode()

        # 构造parent config
        parent_config = None
        if parent_checkpoint_id:
            parent_config = {
                "configurable": {
                    "thread_id": to_safe_id(thread_id),
                    "checkpoint_ns": to_safe_str(checkpoint_ns),
                    "checkpoint_id": to_safe_id(parent_checkpoint_id),
                }
            }

        # 解析writes
        pending_writes = []
        for write_data in writes_data:
            write_item = self.loads(write_data)
            pending_writes.append((
                write_item["task_id"],
                write_item["channel"],
                self.loads(write_item["value"])
            ))

        return CheckpointTuple(
            {
                "configurable": {
                    "thread_id": from_safe_id(thread_id),
                    "checkpoint_ns": from_safe_str(checkpoint_ns),
                    "checkpoint_id": from_safe_id(checkpoint_id),
                }
            },
            checkpoint,
            metadata,
            parent_config,
            pending_writes
        )

    def _serialize_writes(self, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = "") -> list[bytes]:
        """Serialize writes data for Redis storage."""
        writes_data = []
        for idx, (channel, value) in enumerate(writes):
            write_item = {
                "task_id": task_id,
                "task_path": task_path,
                "idx": WRITES_IDX_MAP.get(channel, idx),
                "channel": channel,
                "value": self.dumps(value)
            }
            writes_data.append(self.dumps(write_item))
        return writes_data
