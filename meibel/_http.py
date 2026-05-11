"""
HTTP Client Module

Provides sync and async HTTP clients using httpx.
"""

from __future__ import annotations

import httpx
from typing import Any, Dict, Optional, TypeVar, Type, Union
from pydantic import BaseModel

from .exceptions import ApiError, AuthenticationError, RateLimitError, NotFoundError

T = TypeVar("T", bound=BaseModel)


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
        self._headers = headers or {}

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
    ) -> Union[T, Dict[str, Any], None]:
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
    ) -> "SSEIterator":
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
    ) -> Union[T, Dict[str, Any], None]:
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
        )
        return self._handle_response(response, response_model)

    def _serialize_body(self, body: Any) -> Any:
        """Serialize request body, converting Pydantic models to dicts."""
        if body is None:
            return None
        if isinstance(body, BaseModel):
            return body.model_dump(mode="json", exclude_none=True)
        if isinstance(body, list):
            return [self._serialize_body(item) for item in body]
        return body

    def _handle_response(
        self,
        response: httpx.Response,
        response_model: Optional[Type[T]] = None,
    ) -> Union[T, Dict[str, Any], None]:
        """Handle HTTP response, raising errors or parsing data."""
        if response.status_code == 204:
            return None

        if response.status_code >= 400:
            self._raise_error(response)

        if response_model:
            return response_model.model_validate(response.json())

        try:
            return response.json()
        except Exception:
            return None

    def _raise_error(self, response: httpx.Response) -> None:
        """Raise appropriate error based on status code."""
        try:
            error_body = response.json()
        except Exception:
            error_body = {"message": response.text}

        message = error_body.get("message", error_body.get("error", "Unknown error"))

        if response.status_code == 401:
            raise AuthenticationError(message, response.status_code, error_body)
        elif response.status_code == 404:
            raise NotFoundError(message, response.status_code, error_body)
        elif response.status_code == 429:
            raise RateLimitError(message, response.status_code, error_body)
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
        self._headers = headers or {}

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
    ) -> Union[T, Dict[str, Any], None]:
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
    ) -> "AsyncSSEIterator":
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
    ) -> Union[T, Dict[str, Any], None]:
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
        )
        return self._handle_response(response, response_model)

    def _serialize_body(self, body: Any) -> Any:
        """Serialize request body, converting Pydantic models to dicts."""
        if body is None:
            return None
        if isinstance(body, BaseModel):
            return body.model_dump(mode="json", exclude_none=True)
        if isinstance(body, list):
            return [self._serialize_body(item) for item in body]
        return body

    def _handle_response(
        self,
        response: httpx.Response,
        response_model: Optional[Type[T]] = None,
    ) -> Union[T, Dict[str, Any], None]:
        """Handle HTTP response, raising errors or parsing data."""
        if response.status_code == 204:
            return None

        if response.status_code >= 400:
            self._raise_error(response)

        if response_model:
            return response_model.model_validate(response.json())

        try:
            return response.json()
        except Exception:
            return None

    def _raise_error(self, response: httpx.Response) -> None:
        """Raise appropriate error based on status code."""
        try:
            error_body = response.json()
        except Exception:
            error_body = {"message": response.text}

        message = error_body.get("message", error_body.get("error", "Unknown error"))

        if response.status_code == 401:
            raise AuthenticationError(message, response.status_code, error_body)
        elif response.status_code == 404:
            raise NotFoundError(message, response.status_code, error_body)
        elif response.status_code == 429:
            raise RateLimitError(message, response.status_code, error_body)
        else:
            raise ApiError(message, response.status_code, error_body)
