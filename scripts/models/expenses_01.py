from pydantic import BaseModel, field_validator, ConfigDict
from typing import Optional, Any
from datetime import datetime, date


class Expenses01Row(BaseModel):

    model_config = ConfigDict(str_strip_whitespace=True)

    # required fields, cannot be NULL or empty string
    expense_record_date: datetime
    expense_date: date
    account_code: int
    expense_description: str
    expense_amount: float
    expense_sender: str

    # these fields are optional
    expense_comments: Optional[str] = None

    # adding a field validator for '' empty strings, so it can be treated as None *BEFORE* it reaches pydyantic
    @field_validator('expense_record_date','expense_date','account_code','expense_description','expense_amount','expense_sender', mode='before' )
    @classmethod
    def validate_empty_fields(cls, v):
        if isinstance(v, str) and not v.strip():
            raise ValueError('Required fields cannot be empty')
        if v is None: 
            raise ValueError('Required fields cannot be NULL')
        return v