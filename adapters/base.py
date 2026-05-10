"""
Base types for the adapter layer.

Defines:
- TickEvent: the normalized internal representation of a single trade.
  All adapters yield TickEvent objects regardless of source format.
- TickAdapter: the abstract interface every adapter implements.

Design rule: anything source-specific (Databento side codes, IQFeed
field names, vendor timestamp formats) must be translated to TickEvent
inside the adapter. The processor and visualization layers must never
see source-specific data.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncIterator, Literal


HealthStatus = Literal["green", "yellow", "red"]
AggressorSide = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class TickEvent:
    """A single normalized trade event.

    Frozen so events cannot be mutated after creation. slots=True for
    a small memory win at high tick volumes.

    Fields:
        timestamp: when the trade occurred (timezone-aware UTC preferred)
        price: trade price as float; format with instrument tick precision at display time
        size: number of contracts traded (int >= 1)
        aggressor_side: 'buy' if the aggressor lifted the ask, 'sell' if hit the bid
        instrument_symbol: e.g. 'NQZ5', 'GCG6'
        trade_conditions: vendor-specific condition codes; empty tuple if none
    """

    timestamp: datetime
    price: float
    size: int
    aggressor_side: AggressorSide
    instrument_symbol: str
    trade_conditions: tuple[str, ...] = field(default_factory=tuple)


class TickAdapter(ABC):
    """Abstract base class for all tick data sources.

    Subclasses implement connect / stream / disconnect and expose a
    health property. The stream method is an async generator yielding
    TickEvent objects.

    Lifecycle:
        adapter = ConcreteAdapter(...)
        await adapter.connect()
        async for tick in adapter.stream():
            ...
        await adapter.disconnect()
    """

    @abstractmethod
    async def connect(self) -> None:
        """Open the underlying resource (file, socket, websocket)."""
        ...

    @abstractmethod
    def stream(self) -> AsyncIterator[TickEvent]:
        """Yield TickEvent objects until the source is exhausted or disconnected.

        Note: this is declared as a regular method returning AsyncIterator
        rather than `async def` because async generators in Python are
        functions that *return* an async iterator when called, and ABCs
        require the signature to match what subclasses actually provide.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Release the underlying resource."""
        ...

    @property
    @abstractmethod
    def health(self) -> HealthStatus:
        """Current connection health.

        green: receiving ticks normally
        yellow: connected but stale (no ticks for a threshold period)
        red: disconnected or errored
        """
        ...
