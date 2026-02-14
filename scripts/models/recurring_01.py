from pydantic import BaseModel, field_validator, ConfigDict
from typing import Optional, Any
from datetime import datetime, date

class Recurring01Row(BaseModel):
    model_config = ConfigDict(str_strip_white_space=True)

    #required fields
    recurring_fee_record_date: datetime
    recurring_fee_date: date
    recurring_fee_name: str
    recurring_fee_amount: float
    recurring_fee_status: str
    recurring_fee_payment_status: str
    recurring_fee_account_code: int
    recurring_fee_payment_terms: str

    recurring_fee_type: Optional[str] = None
    recurring_fee_contract_duration: Optional[str] = None
    recurring_fee_comment: Optional[str] = None
    
    @field_validator('recurring_fee_record_date','recurring_fee_date','recurring_fee_name','recurring_fee_amount','recurring_fee_status','recurring_fee_payment_status','recurring_fee_account_code','recurring_fee_payment_terms',mode='before')
    @classmethod

    def validate_empty_fields(cls, v):
        if isinstance(v, str) and not v.strip():
            raise ValueError('Required fields cannot be empty')
        if v is None:
            raise ValueError('Required fields cannot be NULL')
        return v