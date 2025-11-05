from typing import Sequence, Any, Iterator

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    ChannelVersions,
    CheckpointTuple,
    get_checkpoint_metadata,
    get_checkpoint_id,
    WRITES_IDX_MAP
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from redis import Redis
from .aio import AsyncRedisSaver
from .base import BaseRedisSaver


class RedisSaver(BaseRedisSaver):
    def __init__(self, conn: Redis, *, serde: SerializerProtocol | None = None, ttl: int = 60):
        """Initialize Redis checkpointer with TTL support.
        
        Args:
            conn: Redis connection instance
            serde: Serialization protocol (optional)
            ttl: Time to live in seconds for all keys (default: 3600 = 1 hour)
        """
        super().__init__(serde=serde)
        self.conn = conn
        self.ttl = ttl  # TTL for all keys (short-term memory)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # 确定要获取的checkpoint ID
        if checkpoint_id := get_checkpoint_id(config):
            # 获取特定checkpoint
            target_id = checkpoint_id
        else:
            # 获取最新checkpoint
            index_key = self._make_index_key(thread_id, checkpoint_ns)
            checkpoint_ids = self.conn.smembers(index_key)

            if not checkpoint_ids:
                return None

            # 利用UUID6的单调性：max()直接得到最新ID
            target_id = max(id.decode() for id in checkpoint_ids)

        # 获取checkpoint和writes数据
        checkpoint_key = self._make_checkpoint_key(thread_id, checkpoint_ns, target_id)
        writes_key = self._make_writes_key(thread_id, checkpoint_ns, target_id)

        pipe = self.conn.pipeline()
        pipe.hgetall(checkpoint_key)
        pipe.lrange(writes_key, 0, -1)

        checkpoint_data, writes_data = pipe.execute()

        if not checkpoint_data:
            return None

        return self._build_checkpoint_tuple(thread_id, checkpoint_ns, target_id, checkpoint_data, writes_data)

    def list(self, config: RunnableConfig | None, *, filter: dict[str, Any] | None = None,
             before: RunnableConfig | None = None, limit: int | None = None) -> Iterator[CheckpointTuple]:
        if not config:
            return

        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        index_key = f'idx:{{{thread_id}:{checkpoint_ns}}}'
        checkpoint_ids = self.conn.smembers(index_key)

        if not checkpoint_ids:
            return

        checkpoint_ids = [id.decode() for id in checkpoint_ids]
        checkpoint_ids.sort(reverse=True)

        if before:
            before_checkpoint_id = get_checkpoint_id(before)
            if before_checkpoint_id and before_checkpoint_id in checkpoint_ids:
                before_index = checkpoint_ids.index(before_checkpoint_id)
                checkpoint_ids = checkpoint_ids[before_index + 1:]

        if limit:
            checkpoint_ids = checkpoint_ids[:limit]

        # 批量获取checkpoint数据
        for checkpoint_id in checkpoint_ids:
            checkpoint_key = self._make_checkpoint_key(thread_id, checkpoint_ns, checkpoint_id)
            writes_key = self._make_writes_key(thread_id, checkpoint_ns, checkpoint_id)

            pipe = self.conn.pipeline()
            pipe.hgetall(checkpoint_key)
            pipe.lrange(writes_key, 0, -1)

            checkpoint_data, writes_data = pipe.execute()

            if not checkpoint_data:
                continue  # 跳过不存在的checkpoint

            # 构建checkpoint tuple
            checkpoint_tuple = self._build_checkpoint_tuple(thread_id, checkpoint_ns, checkpoint_id, checkpoint_data, writes_data)
            
            # 应用metadata过滤
            if filter:
                # 检查metadata是否匹配所有过滤条件
                if not all(checkpoint_tuple.metadata.get(k) == v for k, v in filter.items()):
                    continue

            yield checkpoint_tuple

    def put(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata,
            new_versions: ChannelVersions) -> RunnableConfig:
        configurable = config["configurable"].copy()

        thread_id = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")
        checkpoint_id = configurable.pop("checkpoint_id", None)
        copy = checkpoint.copy()
        copy["channel_values"] = copy["channel_values"].copy()

        next_config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

        checkpoint_key = self._make_checkpoint_key(thread_id, checkpoint_ns, checkpoint["id"])
        serialized_checkpoint = self.serde.dumps(checkpoint)
        serialized_metadata = self.serde.dumps(
            get_checkpoint_metadata(config, metadata)
        )

        pipe = self.conn.pipeline()
        pipe.hset(
            checkpoint_key,
            mapping={
                'checkpoint': serialized_checkpoint,
                'metadata': serialized_metadata,
                'parent_checkpoint_id': checkpoint_id or ''
            }
        )
        # 设置checkpoint key的TTL
        pipe.expire(checkpoint_key, self.ttl)

        # 维护checkpoint ID列表（用于快速查找最新）
        index_key = self._make_index_key(thread_id, checkpoint_ns)
        pipe.sadd(index_key, checkpoint["id"])  # 用集合存储checkpoint IDs
        # 设置索引key的TTL
        pipe.expire(index_key, self.ttl)

        pipe.execute()
        return next_config

    def put_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str,
                   task_path: str = "") -> None:
        use_upsert = all(w[0] in WRITES_IDX_MAP for w in writes)

        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = str(config["configurable"]["checkpoint_ns"])
        checkpoint_id = str(config["configurable"]["checkpoint_id"])

        writes_key = self._make_writes_key(thread_id, checkpoint_ns, checkpoint_id)
        writes_data = self._serialize_writes(writes, task_id, task_path)
        pipe = self.conn.pipeline()

        if use_upsert:
            pipe.delete(writes_key)
            if writes_data:
                pipe.lpush(writes_key, *writes_data)
                # 设置writes key的TTL
                pipe.expire(writes_key, self.ttl)
        else:
            if writes_data:
                pipe.lpush(writes_key, *writes_data)
                # 设置writes key的TTL
                pipe.expire(writes_key, self.ttl)

        pipe.execute()

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes associated with a thread ID.
        
        This method removes all data related to a specific thread, including:
        - All checkpoint data
        - All writes data  
        - All index data
        
        Args:
            thread_id: The thread ID to delete.
        """
        # 使用SCAN查找所有相关的key模式
        patterns = self._make_scan_patterns(thread_id)

        deleted_keys = 0

        for pattern in patterns:
            cursor = 0
            while True:
                cursor, keys = self.conn.scan(cursor, match=pattern, count=1000)
                if keys:
                    deleted_keys += len(keys)
                    # 批量删除找到的key
                    self.conn.delete(*keys)
                if cursor == 0:
                    break

        # 可选：记录删除的key数量（用于调试）
        if deleted_keys > 0:
            print(f"已删除 thread_id '{thread_id}' 的 {deleted_keys} 个keys")


__all__ = ["RedisSaver", "AsyncRedisSaver"]
