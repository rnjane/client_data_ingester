from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any


class TransformerError(Exception):
    pass


class BaseTransformer(ABC):

    id = None

    @abstractmethod
    def transform(self, value: Any) -> Any:
        pass


class DecimalTransformer(BaseTransformer):

    id = 'decimal'

    def transform(self, value: Any) -> Decimal:
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        if isinstance(value, str):
            # Remove currency symbols and spaces
            cleaned = value.replace('$', '').replace('£', '').replace(',', '').strip()
            return Decimal(cleaned)
        return Decimal('0')


class TextTransformer(BaseTransformer):

    id = 'text'

    def transform(self, value: Any) -> str:
        return str(value).strip()

class IntegerTransformer(BaseTransformer):

    id = 'integer'

    def transform(self, value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            cleaned = value.strip()
            try:
                return int(float(cleaned))
            except ValueError:
                return 0
        return 0
    
class BooleanTransformer(BaseTransformer):

    id = 'boolean'

    boolean_yes = ['yes', 'true', '1']  
    boolean_no = ['no', 'false', '0']

    def transform(self, value: Any) -> bool:
        cleaned = str(value).strip().lower()
        if cleaned in self.boolean_yes:
            return True
        if cleaned in self.boolean_no:
            return False
        raise TransformerError(f"Invalid boolean value: {value}")