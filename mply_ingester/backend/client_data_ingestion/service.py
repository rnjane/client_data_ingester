from sqlalchemy.orm import Session
from sqlalchemy import update
from typing import List

from .base import ParserConfig, ParsedItem, IngestionReport
from .parsers import get_parser
from .transformers import get_transformer

class DataIngestionService:
    def __init__(self, db: Session):
        self.db = db
    
    def ingest_data(self, parser_config: ParserConfig, client_data: bytes) -> IngestionReport:
        def do_ingest():
            # Get parser
            parser = get_parser(parser_config.parser_id)
            
            # Parse and interpret data
            parsed_items = parser.process_client_data(client_data, parser_config.column_mapping)
            
            # Apply to database
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
            return IngestionReport(
                success=False,
                message=f"Error processing data: {str(e)}",
                processed_items=0,
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
                db_record = ClientData(**record_data)
                self.db.add(db_record)
                processed_count += 1
        
        self.db.commit()
        return processed_count