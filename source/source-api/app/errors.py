"""Business-rule violations, returned as 4xx with a machine-readable code."""

from fastapi import HTTPException


class DomainError(HTTPException):
    def __init__(self, status: int, code: str, message: str, **details):
        super().__init__(status_code=status, detail={"code": code, "message": message, **details})


def not_found(entity: str, entity_id) -> DomainError:
    return DomainError(404, f"{entity}_not_found", f"{entity} {entity_id} does not exist")


def conflict(code: str, message: str, **details) -> DomainError:
    return DomainError(409, code, message, **details)
