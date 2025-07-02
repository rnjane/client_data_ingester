import csv
from pydantic import dataclass, BaseModel, Field
import zipfile
from abc import ABC, abstractmethod
from typing import Dict, Tuple, List, io, Any


class ParserConfig(BaseModel):
    parser_id: str
    column_mapping: Dict[str, Tuple[str, str]] = Field(default_factory=dict,
                                                       description="A mapping of client column names to (multiply column names and transformers")

class IngestionReport(BaseModel):
    success: bool
    message: str
    processed_items: int
    report: List[Any]
    stats: Dict[str, Any]

@dataclass
class ParsedElement:
    column_name: str
    value: Any
    is_interpreted: bool = False

    def interpret(self, client_column_name: str, multiply_column_name: str, transformer):
        assert not self.is_interpreted, "Cannot re-interpret an interpreted item"
        assert self.column_name == client_column_name
        assert multiply_column_name in ALL_MULTIPLY_COLUMN_NAMES

        interpreted_value = transformer.transform(self.value)

        return ParsedElement(
            column_name=multiply_column_name,
            value=interpreted_value,
            is_interpreted=True
        )

@dataclass
class ParsedItem:
    elements: List[ParsedElement]

    def interpret(self, column_mapping: Dict[str, Tuple[str, str]]):
        interpreted_elements = []

        for element in self.elements:
            client_column_name = element.column_name

            if client_column_name in column_mapping:
                multiply_column_name, transformer_name = column_mapping[client_column_name]
                transformer = get_transformer(transformer_name)
                interpreted_elements.append(
                    element.interpret(client_column_name, multiply_column_name, transformer)
                )

        self.elements = interpreted_elements

    @property
    def is_interpreted(self):
        return all(element.is_interpreted for element in self.elements)


