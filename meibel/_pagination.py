"""
Pagination Module

Provides iterators for paginated API responses.
"""

from __future__ import annotations

from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)

T = TypeVar("T")


class PaginatedIterator(Generic[T]):
    """
    Iterator for paginated API responses.

    Supports cursor-based, offset-based, and page-based pagination.
    """

    def __init__(
        self,
        fetch_page: Callable[[Optional[str]], Dict[str, Any]],
        extract_items: Callable[[Dict[str, Any]], List[T]],
        extract_cursor: Callable[[Dict[str, Any]], Optional[str]],
    ):
        """
        Initialize the paginated iterator.

        Args:
            fetch_page: Function that fetches a page given a cursor (None for first page)
            extract_items: Function that extracts items from the response
            extract_cursor: Function that extracts the next cursor from the response
        """
        self._fetch_page = fetch_page
        self._extract_items = extract_items
        self._extract_cursor = extract_cursor
        self._cursor: Optional[str] = None
        self._buffer: List[T] = []
        self._exhausted = False

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        # If we have items in the buffer, return the next one
        if self._buffer:
            return self._buffer.pop(0)

        # If we've exhausted all pages, stop
        if self._exhausted:
            raise StopIteration

        # Fetch the next page
        response = self._fetch_page(self._cursor)
        items = self._extract_items(response)
        next_cursor = self._extract_cursor(response)

        # Update state
        if next_cursor:
            self._cursor = next_cursor
        else:
            self._exhausted = True

        # If no items, stop
        if not items:
            raise StopIteration

        # Buffer items and return the first one
        self._buffer = items[1:]
        return items[0]

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

    Supports cursor-based, offset-based, and page-based pagination.
    """

    def __init__(
        self,
        fetch_page: Callable[[Optional[str]], Any],  # Returns Awaitable
        extract_items: Callable[[Dict[str, Any]], List[T]],
        extract_cursor: Callable[[Dict[str, Any]], Optional[str]],
    ):
        """
        Initialize the async paginated iterator.

        Args:
            fetch_page: Async function that fetches a page given a cursor (None for first page)
            extract_items: Function that extracts items from the response
            extract_cursor: Function that extracts the next cursor from the response
        """
        self._fetch_page = fetch_page
        self._extract_items = extract_items
        self._extract_cursor = extract_cursor
        self._cursor: Optional[str] = None
        self._buffer: List[T] = []
        self._exhausted = False

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        # If we have items in the buffer, return the next one
        if self._buffer:
            return self._buffer.pop(0)

        # If we've exhausted all pages, stop
        if self._exhausted:
            raise StopAsyncIteration

        # Fetch the next page
        response = await self._fetch_page(self._cursor)
        items = self._extract_items(response)
        next_cursor = self._extract_cursor(response)

        # Update state
        if next_cursor:
            self._cursor = next_cursor
        else:
            self._exhausted = True

        # If no items, stop
        if not items:
            raise StopAsyncIteration

        # Buffer items and return the first one
        self._buffer = items[1:]
        return items[0]

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
