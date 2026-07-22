"""
HTTP Client Module

Provides sync and async HTTP clients using httpx.
"""

from __future__ import annotations

import httpx
from typing import Any, Dict, Optional, TypeVar, Type, Union, cast, TYPE_CHECKING
from pydantic import BaseModel

from .exceptions import ApiError, AuthenticationError, RateLimitError, NotFoundError

if TYPE_CHECKING:
    from ._streaming import SSEIterator, AsyncSSEIterator

T = TypeVar("T", bound=BaseModel)


def _build_user_agent() -> str:
    """Build User-Agent string from package metadata."""
    _pkg = __name__.split(".")[0]
    try:
        from . import __version__
        return f"{_pkg}-python/{__version__}"
    except Exception:
        return f"{_pkg}-python/unknown"


_USER_AGENT = _build_user_agent()


def _extract_error_message(error_body: Dict[str, Any]) -> str:
    """Extract a human-readable error message from various API error formats."""
    if "message" in error_body:
        return str(error_body["message"])
    if "error" in error_body:
        return str(error_body["error"])
    if "detail" in error_body:
        detail = error_body["detail"]
        if isinstance(detail, str):
            return detail
        if isinstance(detail, list):
            parts = []
            for item in detail:
                if isinstance(item, dict):
                    loc = ".".join(str(x) for x in item.get("loc", []))
                    msg = item.get("msg", "")
                    parts.append(f"{loc}: {msg}" if loc else msg)
                else:
                    parts.append(str(item))
            return "; ".join(parts)
        return str(detail)
    return "Unknown error"


class HttpClient:
    """Synchronous HTTP client."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._headers = {"User-Agent": _USER_AGENT}
        if headers:
            self._headers.update(headers)

        if api_key:
            self._headers["Meibel-API-Key"] = api_key
        if bearer_token:
            self._headers["Authorization"] = f"Bearer {bearer_token}"

        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            headers=self._headers,
        )

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        response_model: Optional[Type[T]] = None,
        timeout: Optional[float] = None,
    ) -> Union[T, Dict[str, Any], str, bytes, None]:
        """Make an HTTP request."""
        # Filter out None values from params
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        # Merge headers
        request_headers = {**self._headers}
        if headers:
            request_headers.update(headers)

        response = self._client.request(
            method=method,
            url=path,
            params=params,
            json=self._serialize_body(json),
            headers=request_headers,
            timeout=timeout if timeout is not None else self._timeout,
        )

        return self._handle_response(response, response_model)

    def stream(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        data: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "SSEIterator[Any]":
        """Make an HTTP request and return a streaming SSE iterator.

        Args:
            data: Form fields to send as application/x-www-form-urlencoded.
                  Mutually exclusive with json.
        """
        from ._streaming import SSEIterator

        if params:
            params = {k: v for k, v in params.items() if v is not None}

        request_headers = {**self._headers, "Accept": "text/event-stream"}
        if headers:
            request_headers.update(headers)

        response = self._client.send(
            self._client.build_request(
                method=method,
                url=path,
                params=params,
                json=self._serialize_body(json) if json is not None else None,
                data=data,
                headers=request_headers,
            ),
            stream=True,
        )

        if response.status_code >= 400:
            response.read()
            self._raise_error(response)

        return SSEIterator(response)

    def upload(
        self,
        method: str,
        path: str,
        *,
        file: Any,
        file_name: str,
        field_name: str = "file",
        params: Optional[Dict[str, Any]] = None,
        form_fields: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        response_model: Optional[Type[T]] = None,
        timeout: Optional[float] = None,
    ) -> Union[T, Dict[str, Any], str, bytes, None]:
        """Upload a file with streaming multipart/form-data."""
        from ._upload import create_multipart_stream

        content_stream, content_type = create_multipart_stream(
            file, field_name=field_name, file_name=file_name, form_fields=form_fields,
        )

        request_headers = {k: v for k, v in self._headers.items() if k.lower() != "content-type"}
        request_headers["Content-Type"] = content_type
        if headers:
            request_headers.update(headers)
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        response = self._client.request(
            method=method, url=path, content=content_stream, params=params, headers=request_headers,
            timeout=timeout if timeout is not None else self._timeout,
        )
        return self._handle_response(response, response_model)

    def upload_stream(
        self,
        method: str,
        path: str,
        *,
        file: Any,
        file_name: str,
        field_name: str = "file",
        params: Optional[Dict[str, Any]] = None,
        form_fields: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "SSEIterator[Any]":
        """Upload a file via streaming multipart/form-data and consume an SSE response."""
        from ._streaming import SSEIterator
        from ._upload import create_multipart_stream

        content_stream, content_type = create_multipart_stream(
            file, field_name=field_name, file_name=file_name, form_fields=form_fields,
        )

        request_headers = {k: v for k, v in self._headers.items() if k.lower() != "content-type"}
        request_headers["Content-Type"] = content_type
        request_headers["Accept"] = "text/event-stream"
        if headers:
            request_headers.update(headers)
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        response = self._client.send(
            self._client.build_request(
                method=method,
                url=path,
                content=content_stream,
                params=params,
                headers=request_headers,
            ),
            stream=True,
        )

        if response.status_code >= 400:
            response.read()
            self._raise_error(response)

        return SSEIterator(response)

    def _serialize_body(self, body: Any) -> Any:
        """Serialize request body, converting Pydantic models to dicts."""
        if body is None:
            return None
        if isinstance(body, BaseModel):
            return body.model_dump(mode="json", by_alias=True, exclude_none=True)
        if isinstance(body, list):
            return [self._serialize_body(item) for item in body]
        return body

    def _handle_response(
        self,
        response: httpx.Response,
        response_model: Optional[Type[T]] = None,
    ) -> Union[T, Dict[str, Any], str, bytes, None]:
        """Handle HTTP response, raising errors or parsing data."""
        if response.status_code == 204:
            return None

        if response.status_code >= 400:
            self._raise_error(response)

        if response_model:
            return response_model.model_validate(response.json())

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            return cast(Dict[str, Any], response.json())

        # Binary responses (file downloads, archives, images, etc.)
        if any(t in content_type for t in ("application/octet-stream", "application/zip",
               "application/gzip", "application/pdf", "image/")):
            return response.content

        # Text responses (text/plain, text/html, text/csv, etc.)
        if response.text:
            return response.text

        return None

    def _raise_error(self, response: httpx.Response) -> None:
        """Raise appropriate error based on status code."""
        try:
            error_body = response.json()
        except Exception:
            error_body = {"message": response.text}

        message = _extract_error_message(error_body)

        if response.status_code == 401:
            raise AuthenticationError(message, response.status_code, error_body)
        elif response.status_code == 404:
            raise NotFoundError(message, response.status_code, error_body)
        elif response.status_code == 429:
            raise RateLimitError(message, status_code=response.status_code, response_body=error_body)
        else:
            raise ApiError(message, response.status_code, error_body)


class AsyncHttpClient:
    """Asynchronous HTTP client."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._headers = {"User-Agent": _USER_AGENT}
        if headers:
            self._headers.update(headers)

        if api_key:
            self._headers["Meibel-API-Key"] = api_key
        if bearer_token:
            self._headers["Authorization"] = f"Bearer {bearer_token}"

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers=self._headers,
        )

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncHttpClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        response_model: Optional[Type[T]] = None,
        timeout: Optional[float] = None,
    ) -> Union[T, Dict[str, Any], str, bytes, None]:
        """Make an HTTP request."""
        # Filter out None values from params
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        # Merge headers
        request_headers = {**self._headers}
        if headers:
            request_headers.update(headers)

        response = await self._client.request(
            method=method,
            url=path,
            params=params,
            json=self._serialize_body(json),
            headers=request_headers,
            timeout=timeout if timeout is not None else self._timeout,
        )

        return self._handle_response(response, response_model)

    async def stream(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        data: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "AsyncSSEIterator[Any]":
        """Make an HTTP request and return a streaming SSE iterator.

        Args:
            data: Form fields to send as application/x-www-form-urlencoded.
                  Mutually exclusive with json.
        """
        from ._streaming import AsyncSSEIterator

        if params:
            params = {k: v for k, v in params.items() if v is not None}

        request_headers = {**self._headers, "Accept": "text/event-stream"}
        if headers:
            request_headers.update(headers)

        response = await self._client.send(
            self._client.build_request(
                method=method,
                url=path,
                params=params,
                json=self._serialize_body(json) if json is not None else None,
                data=data,
                headers=request_headers,
            ),
            stream=True,
        )

        if response.status_code >= 400:
            await response.aread()
            self._raise_error(response)

        return AsyncSSEIterator(response)

    async def upload(
        self,
        method: str,
        path: str,
        *,
        file: Any,
        file_name: str,
        field_name: str = "file",
        params: Optional[Dict[str, Any]] = None,
        form_fields: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        response_model: Optional[Type[T]] = None,
        timeout: Optional[float] = None,
    ) -> Union[T, Dict[str, Any], str, bytes, None]:
        """Upload a file with streaming multipart/form-data."""
        from ._upload import create_async_multipart_stream

        content_stream, content_type = await create_async_multipart_stream(
            file, field_name=field_name, file_name=file_name, form_fields=form_fields,
        )

        request_headers = {k: v for k, v in self._headers.items() if k.lower() != "content-type"}
        request_headers["Content-Type"] = content_type
        if headers:
            request_headers.update(headers)
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        response = await self._client.request(
            method=method, url=path, content=content_stream, params=params, headers=request_headers,
            timeout=timeout if timeout is not None else self._timeout,
        )
        return self._handle_response(response, response_model)

    async def upload_stream(
        self,
        method: str,
        path: str,
        *,
        file: Any,
        file_name: str,
        field_name: str = "file",
        params: Optional[Dict[str, Any]] = None,
        form_fields: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "AsyncSSEIterator[Any]":
        """Upload a file via streaming multipart/form-data and consume an SSE response."""
        from ._streaming import AsyncSSEIterator
        from ._upload import create_async_multipart_stream

        content_stream, content_type = await create_async_multipart_stream(
            file, field_name=field_name, file_name=file_name, form_fields=form_fields,
        )

        request_headers = {k: v for k, v in self._headers.items() if k.lower() != "content-type"}
        request_headers["Content-Type"] = content_type
        request_headers["Accept"] = "text/event-stream"
        if headers:
            request_headers.update(headers)
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        response = await self._client.send(
            self._client.build_request(
                method=method,
                url=path,
                content=content_stream,
                params=params,
                headers=request_headers,
            ),
            stream=True,
        )

        if response.status_code >= 400:
            await response.aread()
            self._raise_error(response)

        return AsyncSSEIterator(response)

    def _serialize_body(self, body: Any) -> Any:
        """Serialize request body, converting Pydantic models to dicts."""
        if body is None:
            return None
        if isinstance(body, BaseModel):
            return body.model_dump(mode="json", by_alias=True, exclude_none=True)
        if isinstance(body, list):
            return [self._serialize_body(item) for item in body]
        return body

    def _handle_response(
        self,
        response: httpx.Response,
        response_model: Optional[Type[T]] = None,
    ) -> Union[T, Dict[str, Any], str, bytes, None]:
        """Handle HTTP response, raising errors or parsing data."""
        if response.status_code == 204:
            return None

        if response.status_code >= 400:
            self._raise_error(response)

        if response_model:
            return response_model.model_validate(response.json())

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            return cast(Dict[str, Any], response.json())

        # Binary responses (file downloads, archives, images, etc.)
        if any(t in content_type for t in ("application/octet-stream", "application/zip",
               "application/gzip", "application/pdf", "image/")):
            return response.content

        # Text responses (text/plain, text/html, text/csv, etc.)
        if response.text:
            return response.text

        return None

    def _raise_error(self, response: httpx.Response) -> None:
        """Raise appropriate error based on status code."""
        try:
            error_body = response.json()
        except Exception:
            error_body = {"message": response.text}

        message = _extract_error_message(error_body)

        if response.status_code == 401:
            raise AuthenticationError(message, response.status_code, error_body)
        elif response.status_code == 404:
            raise NotFoundError(message, response.status_code, error_body)
        elif response.status_code == 429:
            raise RateLimitError(message, status_code=response.status_code, response_body=error_body)
        else:
            raise ApiError(message, response.status_code, error_body)
