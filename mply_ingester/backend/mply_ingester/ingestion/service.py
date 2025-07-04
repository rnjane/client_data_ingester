from sqlalchemy.orm import Session
from sqlalchemy import update
from typing import List

from mply_ingester.config import ConfigBroker
from mply_ingester.db.models import Client, ClientProduct
from mply_ingester.ingestion.base import ParserConfig, ParsedItem, IngestionReport

class DataIngestionService:
    def __init__(self, config_broker: ConfigBroker, db: Session, client: Client):
        self.config_broker = config_broker
        self.db = db
        self.client = client

    def ingest_data(self, parser_config: ParserConfig, client_data: bytes) -> IngestionReport:
        def do_ingest():
            parser = self.config_broker.get_parser(parser_config.parser_id)
            parsed_items = parser.process_client_data(client_data, parser_config.column_mapping)

            processed_count = self._apply_to_database(parsed_items)
            stats = {
                "processed_count": processed_count,
                # "error_count": 0,
                # "skipped_count": 0,
                # "total_count": processed_count
            }
            
            return IngestionReport(
                success=True,
                message="Success",
                processed_items=processed_count,
                report=[],  # TODO: Add a sample of the data that was ingested
                stats=stats
            )

        try:
            return do_ingest()
        except Exception as e:
            raise
            return IngestionReport(
                success=False,
                message=f"Error processing data: {str(e)}",
                processed_items=0,
                report=[],
                stats={}
            )
    
    def _apply_to_database(self, parsed_items: List[ParsedItem]) -> int:
        processed_count = 0
        
        for item in parsed_items:
            assert item.is_interpreted, "Parsed item is not interpreted"
            
            # Convert parsed elements to database record
            record_data = {}
            for element in item.elements:
                record_data[element.column_name] = element.value
            
            if record_data:
                # For this example, we'll always insert new records
                # In a real application, you might want to check for existing records
                # and update them instead
                db_record = ClientProduct(**(record_data | {'client_id': self.client.id}))
                self.db.add(db_record)
                processed_count += 1
        
        self.db.commit()
        return processed_count