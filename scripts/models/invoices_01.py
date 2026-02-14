from pydantic import BaseModel, field_validator, ConfigDict 
from typing import Optional, Any
from datetime import datetime, date

class Invoices01Row(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    # required fields, cannot be NULL or empty string
    invoice_record_date: datetime
    invoice_date: date
    invoice_item: str
    invoice_total_cost: float
    invoice_description: str
    invoice_name: str
    account_code: int

    # these fields are optional
    invoice_qty: Optional[float] = None
    invoice_unit_type: Optional[str] = None
    invoice_unit_price: Optional[float] = None
    invoice_comments: Optional[str] = None
    invoice_supplier_name: Optional[str] = None

    @field_validator('invoice_record_date','invoice_date','invoice_item','invoice_total_cost','invoice_description','invoice_name','account_code', mode='before')
    @classmethod
    def validate_empty_fields(cls, v):  
        if isinstance(v, str) and not v.strip():
            raise ValueError('Required fields cannot be empty')
        if v is None:
            raise ValueError('Required fields cannot be NULL')
        return v