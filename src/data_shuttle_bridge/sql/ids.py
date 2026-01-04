import threading
import time
from typing import Final, Optional, Callable

EPOCH_MS: Final[int] = 1735689600000  # January 1, 2025
TIMESTAMP_BITS = 41
NODE_BITS = 10
SEQUENCE_BITS = 12

MAX_NODE = (1 << NODE_BITS) - 1
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1

NODE_SHIFT = SEQUENCE_BITS
TIME_SHIFT = SEQUENCE_BITS + NODE_BITS

# Thread-local storage for ID generators to support multitenancy
_local = threading.local()
_default_id_generator: Optional[Callable[[], int]] = None
_registry_lock = threading.Lock()


def set_id_generator(node_id: int | str) -> None:
    """
    Set the ID generator for this thread/request context.

    For single-tenant applications: Call once during startup.
    For multi-tenant applications: Call at the beginning of each request with the tenant's node_id.

    This should be called once during application startup, after getting the node_id.

    Args:
        node_id: The node identifier (int or string). If string, will be converted to int hash.
    """
    # Convert string node_id to int if necessary
    if isinstance(node_id, str):
        # Use hash of string, then mod by MAX_NODE to fit in valid range
        node_id_int = abs(hash(node_id)) % (MAX_NODE + 1)
    else:
        node_id_int = node_id

    # Store in thread-local storage
    _local.id_generator = KSortedID(node_id=node_id_int)


def get_id_generator() -> Callable[[], int]:
    """
    Get the ID generator for this thread/request context.

    Returns:
        Callable that generates unique IDs. Raises RuntimeError if not set.
    """
    # Check thread-local storage first
    if hasattr(_local, "id_generator") and _local.id_generator is not None:
        return _local.id_generator

    # Fall back to default (for single-tenant cases)
    global _default_id_generator
    with _registry_lock:
        if _default_id_generator is not None:
            return _default_id_generator

    raise RuntimeError(
        "ID generator not initialized. Call set_id_generator(node_id) first."
    )


def clear_id_generator() -> None:
    """
    Clear the thread-local ID generator. Useful for cleaning up after request processing.
    """
    if hasattr(_local, "id_generator"):
        _local.id_generator = None


class KSortedID:
    def __init__(self, node_id: int):
        if not (0 <= node_id <= MAX_NODE):
            raise ValueError("node_id out of range 0..1023")
        self.node_id = node_id
        self._lock = threading.Lock()
        self._last_ms = -1
        self._seq = 0

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def __call__(self) -> int:
        with self._lock:
            ms = self._now_ms() - EPOCH_MS
            if ms < 0:
                time.sleep((-ms) / 1000.0)
                ms = 0
            if ms == self._last_ms:
                self._seq = (self._seq + 1) & MAX_SEQUENCE
                if self._seq == 0:
                    while True:
                        cur = self._now_ms() - EPOCH_MS
                        if cur > ms:
                            ms = cur
                            break
            else:
                self._seq = 0
            self._last_ms = ms
            return (ms << TIME_SHIFT) | (self.node_id << NODE_SHIFT) | self._seq
