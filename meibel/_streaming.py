"""
Streaming Module

Provides iterators for Server-Sent Events (SSE) streams.
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, Generic, Iterator, Optional, Type, TypeVar, Union, cast

import httpx
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class SSEEvent:
    """Represents a Server-Sent Event."""

    def __init__(
        self,
        event: Optional[str] = None,
        data: Optional[str] = None,
        id: Optional[str] = None,
        retry: Optional[int] = None,
    ):
        self.event = event
        self.data = data
        self.id = id
        self.retry = retry

    def json(self) -> Dict[str, Any]:
        """Parse data as JSON."""
        if self.data:
            return cast(Dict[str, Any], json.loads(self.data))
        return {}

    def __repr__(self) -> str:
        return f"SSEEvent(event={self.event!r}, data={self.data!r}, id={self.id!r})"


class SSEIterator(Generic[T]):
    """
    Synchronous iterator for Server-Sent Events streams.
    """

    def __init__(
        self,
        response: httpx.Response,
        event_models: Optional[Dict[str, Type[T]]] = None,
    ):
        """
        Initialize the SSE iterator.

        Args:
            response: The httpx response object (streaming)
            event_models: Optional mapping of event types to Pydantic models
        """
        self._response = response
        self._event_models = event_models or {}
        self._lines_iter = response.iter_lines()

    def __iter__(self) -> Iterator[Union[SSEEvent, T]]:
        return self

    def __next__(self) -> Union[SSEEvent, T]:
        event = self._parse_next_event()
        if event is None:
            raise StopIteration
        return self._maybe_parse_model(event)

    def _parse_next_event(self) -> Optional[SSEEvent]:
        """Parse the next SSE event from the stream."""
        event_type: Optional[str] = None
        data_lines: list[str] = []
        event_id: Optional[str] = None
        retry: Optional[int] = None

        try:
            for line in self._lines_iter:
                # Empty line marks end of event
                if not line:
                    if data_lines:
                        return SSEEvent(
                            event=event_type,
                            data="\n".join(data_lines),
                            id=event_id,
                            retry=retry,
                        )
                    continue

                # Parse field
                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].strip())
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("retry:"):
                    try:
                        retry = int(line[6:].strip())
                    except ValueError:
                        pass
                # Ignore comments (lines starting with :)
        except StopIteration:
            # Stream ended, return any pending event
            if data_lines:
                return SSEEvent(
                    event=event_type,
                    data="\n".join(data_lines),
                    id=event_id,
                    retry=retry,
                )

        return None

    def _maybe_parse_model(self, event: SSEEvent) -> Union[SSEEvent, T]:
        """Parse event data into a Pydantic model if a model is registered."""
        if event.event and event.event in self._event_models:
            model_class = self._event_models[event.event]
            return model_class.model_validate(event.json())
        return event

    def close(self) -> None:
        """Close the underlying response."""
        self._response.close()

    def __enter__(self) -> "SSEIterator[T]":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class AsyncSSEIterator(Generic[T]):
    """
    Asynchronous iterator for Server-Sent Events streams.
    """

    def __init__(
        self,
        response: httpx.Response,
        event_models: Optional[Dict[str, Type[T]]] = None,
    ):
        """
        Initialize the async SSE iterator.

        Args:
            response: The httpx response object (streaming)
            event_models: Optional mapping of event types to Pydantic models
        """
        self._response = response
        self._event_models = event_models or {}
        self._lines_iter = response.aiter_lines()

    def __aiter__(self) -> AsyncIterator[Union[SSEEvent, T]]:
        return self

    async def __anext__(self) -> Union[SSEEvent, T]:
        event = await self._parse_next_event()
        if event is None:
            raise StopAsyncIteration
        return self._maybe_parse_model(event)

    async def _parse_next_event(self) -> Optional[SSEEvent]:
        """Parse the next SSE event from the stream."""
        event_type: Optional[str] = None
        data_lines: list[str] = []
        event_id: Optional[str] = None
        retry: Optional[int] = None

        try:
            async for line in self._lines_iter:
                # Empty line marks end of event
                if not line:
                    if data_lines:
                        return SSEEvent(
                            event=event_type,
                            data="\n".join(data_lines),
                            id=event_id,
                            retry=retry,
                        )
                    continue

                # Parse field
                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].strip())
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("retry:"):
                    try:
                        retry = int(line[6:].strip())
                    except ValueError:
                        pass
                # Ignore comments (lines starting with :)
        except StopAsyncIteration:
            # Stream ended, return any pending event
            if data_lines:
                return SSEEvent(
                    event=event_type,
                    data="\n".join(data_lines),
                    id=event_id,
                    retry=retry,
                )

        return None

    def _maybe_parse_model(self, event: SSEEvent) -> Union[SSEEvent, T]:
        """Parse event data into a Pydantic model if a model is registered."""
        if event.event and event.event in self._event_models:
            model_class = self._event_models[event.event]
            return model_class.model_validate(event.json())
        return event

    async def close(self) -> None:
        """Close the underlying response."""
        await self._response.aclose()

    async def __aenter__(self) -> "AsyncSSEIterator[T]":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
