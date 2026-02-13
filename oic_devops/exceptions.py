"""
Exceptions module for the OIC DevOps package.

This module defines custom exceptions used throughout the package.
"""


class OICError(Exception):
    """Base exception for all OIC DevOps errors."""


class OICAuthenticationError(OICError):
    """Exception raised for authentication failures."""


class OICResourceNotFoundError(OICError):
    """Exception raised when a resource is not found."""


class OICConfigurationError(OICError):
    """Exception raised for configuration errors."""


class OICValidationError(OICError):
    """Exception raised for validation errors."""


class OICAPIError(OICError):
    """Exception raised for API errors."""

    def __init__(self, message, status_code=None, response=None):
        """
        Initialize OICAPIError.

        Args:
            message: Error message.
            status_code: HTTP status code of the error.
            response: Full response object.

        """
        self.status_code = status_code
        self.response = response
        super().__init__(message)

    @property
    def error_payload(self):
        """
        Safely parse and return the response JSON (dict) if available,
        otherwise None. Never raises.
        """
        if self.response is None:
            return None
        try:
            return self.response.json()
        except Exception:
            return None


    @property
    def title(self):
        """
        Return the best-guess 'title' from the error, trying common shapes:
        - payload["title"]
        - payload["details"]["title"]
        Fallback to response.text or the base message if nothing else.
        """
        payload = self.error_payload
        if isinstance(payload, dict):
            if "title" in payload:
                return payload.get("title")
            if isinstance(payload.get("details"), dict) and "title" in payload["details"]:
                return payload["details"]["title"]

        # Fallbacks
        if self.response is not None:
            txt = (self.response.text or "").strip()
            if txt:
                return txt
        # Last resort: the exception message passed to __init__
        return str(self)
