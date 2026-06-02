"""HTTP client for Alasco FinCon API."""

from typing import Any

import httpx


class AlascoClient:
    """HTTP client with authentication and pagination handling for Alasco API.

    The Alasco API uses two headers for authentication:
    - X-API-KEY: The API key
    - X-API-TOKEN: The API token

    All responses follow the JSON:API specification with pagination support.
    """

    DEFAULT_BASE_URL = "https://api.alasco.de/v1"
    DEFAULT_TIMEOUT = 30.0

    def __init__(
        self,
        token: str,
        key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """Initialize the Alasco client.

        Args:
            token: API token (X-API-TOKEN header)
            key: API key (X-API-KEY header)
            base_url: Base URL for the API (default: https://api.alasco.de/v1)
            timeout: Request timeout in seconds (default: 30)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = httpx.Client(
            headers={
                "X-API-TOKEN": token,
                "X-API-KEY": key,
                "Accept": "application/json",
            },
            timeout=timeout,
        )

    def __enter__(self) -> "AlascoClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint path."""
        # Handle absolute URLs (for pagination)
        if endpoint.startswith("http"):
            return endpoint
        # Ensure endpoint starts with /
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{self.base_url}{endpoint}"

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make an authenticated GET request.

        Args:
            endpoint: API endpoint path (e.g., "/properties/")
            params: Optional query parameters

        Returns:
            JSON response as dictionary

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        url = self._build_url(endpoint)
        response = self._client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_all(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages of a paginated endpoint.

        Follows the `links.next` URL to fetch subsequent pages until
        all data is retrieved or max_pages is reached.

        Args:
            endpoint: API endpoint path
            params: Optional query parameters for the first request
            max_pages: Maximum number of pages to fetch (None for unlimited)

        Returns:
            List of all resource objects from the "data" field across all pages
        """
        all_data: list[dict[str, Any]] = []
        page_count = 0
        next_url: str | None = endpoint

        while next_url is not None:
            # Check page limit
            if max_pages is not None and page_count >= max_pages:
                break

            # Make request (only include params for first page)
            if page_count == 0:
                response = self.get(next_url, params=params)
            else:
                response = self.get(next_url)

            # Extract data
            data = response.get("data", [])
            if isinstance(data, list):
                all_data.extend(data)
            else:
                # Single resource response
                all_data.append(data)

            # Get next page URL
            links = response.get("links", {})
            next_url = links.get("next")
            page_count += 1

        return all_data

    def download(self, url: str) -> bytes:
        """Download a file by following redirect to a pre-signed URL.

        Document download endpoints return HTTP 302 redirects to temporary
        pre-signed URLs. This method follows the redirect and returns the
        file content as bytes.

        Args:
            url: The download endpoint URL (absolute or relative)

        Returns:
            File content as bytes

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        full_url = self._build_url(url)
        # Follow the redirect manually to handle auth headers correctly:
        # - First request: with auth headers to get the 302 redirect
        # - Second request: to the pre-signed URL without auth headers
        response = self._client.get(full_url, follow_redirects=False)

        if response.status_code in (301, 302, 303, 307, 308):
            redirect_url = response.headers["location"]
            # Pre-signed URL doesn't need (and shouldn't have) auth headers
            with httpx.Client(timeout=self.timeout) as plain_client:
                redirect_response = plain_client.get(redirect_url)
                redirect_response.raise_for_status()
                return redirect_response.content

        # If no redirect, return content directly
        response.raise_for_status()
        return response.content

    def get_single(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetch a single resource.

        Args:
            endpoint: API endpoint path (e.g., "/properties/{id}/")
            params: Optional query parameters

        Returns:
            Single resource object from the "data" field
        """
        response = self.get(endpoint, params=params)
        return response.get("data", {})

    def post(self, endpoint: str, json: dict[str, Any]) -> dict[str, Any]:
        """Make an authenticated POST request with a JSON:API body.

        Args:
            endpoint: API endpoint path (e.g., "/contracts/")
            json: Request body (JSON:API document, e.g. ``{"data": {...}}``)

        Returns:
            JSON response as dictionary

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        url = self._build_url(endpoint)
        response = self._client.post(url, json=json)
        response.raise_for_status()
        return response.json()

    def patch(self, endpoint: str, json: dict[str, Any]) -> dict[str, Any]:
        """Make an authenticated PATCH request with a JSON:API body.

        Args:
            endpoint: API endpoint path (e.g., "/contracts/{id}/")
            json: Request body (JSON:API document, e.g. ``{"data": {...}}``)

        Returns:
            JSON response as dictionary

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        url = self._build_url(endpoint)
        response = self._client.patch(url, json=json)
        response.raise_for_status()
        return response.json()

    def post_multipart(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make an authenticated multipart/form-data POST request.

        Used for file uploads (e.g. contract documents). httpx sets the
        ``Content-Type: multipart/form-data`` boundary header automatically when
        ``files`` is provided.

        Args:
            endpoint: API endpoint path (e.g., "/contracts/{id}/documents/")
            data: Optional form fields (non-file)
            files: File parts, e.g. ``{"upload": (filename, content, content_type)}``

        Returns:
            JSON response as dictionary

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        url = self._build_url(endpoint)
        response = self._client.post(url, data=data, files=files)
        response.raise_for_status()
        return response.json()
