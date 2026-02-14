from pydantic import BaseModel, field_validator, ConfigDict
from typing import Optional, Any

 
class ChartOfAccountsRow(BaseModel):

    model_config = ConfigDict(strict=True, str_strip_whitespace=True)

    # required fields, cannot be NULL or empty string 
    account_code: int
    account_name: str
    account_parent_code: int
    account_main_category: str
    account_sub_category: str
    account_coa_category: str
    account_dup_code: bool

    # these fields are optional
    account_description: Optional[str] = None
    account_in_expense_dashboard: str

    # adding a field validator for '' empty strings, so it can be treated as None *BEFORE* it reaches pydyantic 
    @field_validator('account_code', 'account_name', 'account_parent_code', 'account_main_category', 'account_sub_category', 'account_coa_category', mode='before')
    @classmethod
    def validate_empty_fields(cls, v):
        if isinstance(v, str) and not v.strip():
            raise ValueError('Required fields cannot be empty')
        if v is None:
            raise ValueError('Required fields cannot be NULL')
        return v
    
    @field_validator('account_dup_code', mode='before')
    @classmethod
    def parse_arrayformula_bool(cls, v: Any) -> bool: 
        if isinstance(v, str):
            if v.upper() == 'TRUE': return True
            if v.upper() == 'FALSE': return False
        return bool(v)