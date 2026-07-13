"""
Pagination Module

Provides iterators for paginated API responses.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)

from pydantic import BaseModel

if TYPE_CHECKING:
    from ._http import AsyncHttpClient, HttpClient

T = TypeVar("T")


class PaginatedIterator(Generic[T]):
    """
    Iterator for paginated API responses.

    Accepts a declarative description of how to paginate and handles
    fetching, cursor extraction, and item extraction internally.
    """

    def __init__(
        self,
        http: "HttpClient",
        method: str,
        path: str,
        *,
        items_field: str = "items",
        cursor_param: str = "cursor",
        next_field: str = "next_cursor",
        params: Optional[Dict[str, Any]] = None,
        model_class: Optional[Type[T]] = None,
    ):
        self._http = http
        self._method = method
        self._path = path
        self._items_field = items_field
        self._cursor_param = cursor_param
        self._next_field = next_field
        self._params = dict(params) if params else {}
        self._model_class = model_class
        self._buffer: List[T] = []
        self._exhausted = False

    def _parse_item(self, item: Any) -> T:
        model_class = self._model_class
        if model_class is not None and isinstance(item, dict):
            return cast(T, cast("Type[BaseModel]", model_class).model_validate(item))
        return cast(T, item)

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        if self._buffer:
            return self._buffer.pop(0)

        if self._exhausted:
            raise StopIteration

        response = self._http.request(self._method, self._path, params=self._params)
        data = response if isinstance(response, dict) else {}
        items = data.get(self._items_field, [])
        next_cursor = data.get(self._next_field)

        if next_cursor:
            self._params[self._cursor_param] = next_cursor
        else:
            self._exhausted = True

        if not items:
            raise StopIteration

        parsed = [self._parse_item(i) for i in items]
        self._buffer = parsed[1:]
        return parsed[0]

    def __repr__(self) -> str:
        name = self._model_class.__name__ if self._model_class else "unknown"
        return f"PaginatedIterator[{name}]"

    def collect(self) -> List[T]:
        """Collect all items into a list."""
        return list(self)

    def take(self, n: int) -> List[T]:
        """Take up to n items."""
        result: List[T] = []
        for item in self:
            result.append(item)
            if len(result) >= n:
                break
        return result


class AsyncPaginatedIterator(Generic[T]):
    """
    Async iterator for paginated API responses.

    Accepts a declarative description of how to paginate and handles
    fetching, cursor extraction, and item extraction internally.
    """

    def __init__(
        self,
        http: "AsyncHttpClient",
        method: str,
        path: str,
        *,
        items_field: str = "items",
        cursor_param: str = "cursor",
        next_field: str = "next_cursor",
        params: Optional[Dict[str, Any]] = None,
        model_class: Optional[Type[T]] = None,
    ):
        self._http = http
        self._method = method
        self._path = path
        self._items_field = items_field
        self._cursor_param = cursor_param
        self._next_field = next_field
        self._params = dict(params) if params else {}
        self._model_class = model_class
        self._buffer: List[T] = []
        self._exhausted = False

    def _parse_item(self, item: Any) -> T:
        model_class = self._model_class
        if model_class is not None and isinstance(item, dict):
            return cast(T, cast("Type[BaseModel]", model_class).model_validate(item))
        return cast(T, item)

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        if self._buffer:
            return self._buffer.pop(0)

        if self._exhausted:
            raise StopAsyncIteration

        response = await self._http.request(self._method, self._path, params=self._params)
        data = response if isinstance(response, dict) else {}
        items = data.get(self._items_field, [])
        next_cursor = data.get(self._next_field)

        if next_cursor:
            self._params[self._cursor_param] = next_cursor
        else:
            self._exhausted = True

        if not items:
            raise StopAsyncIteration

        parsed = [self._parse_item(i) for i in items]
        self._buffer = parsed[1:]
        return parsed[0]

    def __repr__(self) -> str:
        name = self._model_class.__name__ if self._model_class else "unknown"
        return f"AsyncPaginatedIterator[{name}]"

    async def collect(self) -> List[T]:
        """Collect all items into a list."""
        result: List[T] = []
        async for item in self:
            result.append(item)
        return result

    async def take(self, n: int) -> List[T]:
        """Take up to n items."""
        result: List[T] = []
        async for item in self:
            result.append(item)
            if len(result) >= n:
                break
        return result
