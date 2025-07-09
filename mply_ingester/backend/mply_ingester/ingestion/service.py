from sqlalchemy.orm import Session
from sqlalchemy import update, func
from typing import List, Set

from mply_ingester.config import ConfigBroker
from mply_ingester.db.models import Client, ClientProduct
from mply_ingester.ingestion.base import ParserConfig, ParsedItem, IngestionReport

class DataIngestionService:
    def __init__(self, config_broker: ConfigBroker, db: Session, client: Client):
        self.config_broker = config_broker
        self.db = db
        self.client = client

    def _extract_skus_from_items(self, parsed_items: List[ParsedItem]) -> Set[str]:
        """Extract all SKUs from parsed items."""
        ingested_skus: Set[str] = set()
        for item in parsed_items:
            assert item.is_interpreted, "Parsed item is not interpreted"

            for element in item.elements:
                if element.column_name == 'sku' and element.value:
                    ingested_skus.add(element.value)
                    break
        return ingested_skus

    def ingest_data(self, parser_config: ParserConfig, client_data: bytes, full_update: bool = False) -> IngestionReport:
        try:
            parser = self.config_broker.get_parser(parser_config.parser_id)
            parsed_items = parser.process_client_data(client_data, parser_config.column_mapping)

            ingested_skus = self._extract_skus_from_items(parsed_items) if full_update else None
            
            processed_count, deactivated_count = self._apply_to_database(parsed_items, full_update, ingested_skus)
            
            stats = {"processed_count": processed_count}
            if full_update:
                stats.update({
                    "deactivated_count": deactivated_count,
                    "total_ingested_skus": len(ingested_skus)
                })

            if full_update:
                message = f"Full update completed. {processed_count} products processed, {deactivated_count} products deactivated."
            else:
                message = "Success"
            
            return IngestionReport(
                success=True,
                message=message,
                processed_items=processed_count,
                report=[],
                stats=stats
            )

        except Exception as e:
            error_type = "full update" if full_update else "data"
            return IngestionReport(
                success=False,
                message=f"Error processing {error_type}: {str(e)}",
                processed_items=0,
                report=[],
                stats={}
            )
    
    def _apply_to_database(self, parsed_items: List[ParsedItem], full_update: bool = False, ingested_skus: Set[str] = None) -> tuple[int, int]:
        if full_update and ingested_skus is None:
            raise ValueError("ingested_skus must be provided when full_update=True")
        
        processed_count = 0
        deactivated_count = 0
        
        if full_update:
            deactivated_count = self.db.query(ClientProduct).filter(
                ClientProduct.client_id == self.client.id,
                ClientProduct.sku.isnot(None),
                ~ClientProduct.sku.in_(ingested_skus)
            ).update({
                'active': False,
                'last_changed_on': func.current_timestamp()
            })
        
        for item in parsed_items:
            assert item.is_interpreted, "Parsed item is not interpreted"
            
            record_data = {element.column_name: element.value for element in item.elements}
            if not record_data:
                continue
            
            sku = record_data.get('sku')
            if sku:
                existing_record = self.db.query(ClientProduct).filter_by(
                    sku=sku, client_id=self.client.id
                ).first()
                
                if existing_record:
                    for key, value in record_data.items():
                        if key != 'sku' and value is not None:
                            setattr(existing_record, key, value)
                    existing_record.last_changed_on = func.current_timestamp()
                    processed_count += 1
                    continue
            
            db_record = ClientProduct(**(record_data | {'client_id': self.client.id}))
            self.db.add(db_record)
            processed_count += 1
        
        self.db.commit()
        return processed_count, deactivated_count
