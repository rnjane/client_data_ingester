from abc import ABC, abstractmethod
import csv
import io
from typing import List, Dict, Tuple

from mply_ingester.config import ConfigBroker
from mply_ingester.ingestion.base import ParsedItem, ParsedElement


class ClientDataParser(ABC):
    """Parse and interpret client data"""

    id = None

    def __init__(self, config_broker: ConfigBroker):
        self.config_broker = config_broker

    def process_client_data(self, client_data: bytes, column_mapping: Dict[str, Tuple[str, str]]) -> List[ParsedItem]:
        parsed_items = self.parse_client_data(client_data)

        for item in parsed_items:
            item.interpret(self.config_broker, column_mapping)

        return parsed_items

    @abstractmethod
    def parse_client_data(self, client_data: bytes) -> List[ParsedItem]:
        pass

class CSVParser(ClientDataParser):

    id = 'csv'

    def parse_client_data(self, client_data: bytes) -> List[ParsedItem]:
        content = client_data.decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(content))

        parsed_items = []
        for row in csv_reader:
            elements = []
            for column_name, value in row.items():
                if column_name and value is not None:
                    elements.append(ParsedElement(column_name=column_name.strip(), value=value))

            if elements:
                parsed_items.append(ParsedItem(elements=elements))

        return parsed_items
