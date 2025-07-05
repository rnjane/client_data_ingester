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

    def ingest_data_full_update(self, parser_config: ParserConfig, client_data: bytes) -> IngestionReport:
        """
        Full update mode: Any product ingested is assumed to be active,
        any product that was absent is assumed to be inactive.
        """
        def do_full_update():
            parser = self.config_broker.get_parser(parser_config.parser_id)
            parsed_items = parser.process_client_data(client_data, parser_config.column_mapping)

            # Get all SKUs from the parsed items
            ingested_skus: Set[str] = set()
            for item in parsed_items:
                assert item.is_interpreted, "Parsed item is not interpreted"
                
                # Extract SKU from parsed elements
                for element in item.elements:
                    if element.column_name == 'sku' and element.value:
                        ingested_skus.add(element.value)
                        break

            # Apply the full update logic
            processed_count, deactivated_count = self._apply_full_update_to_database(parsed_items, ingested_skus)
            
            stats = {
                "processed_count": processed_count,
                "deactivated_count": deactivated_count,
                "total_ingested_skus": len(ingested_skus)
            }
            
            return IngestionReport(
                success=True,
                message=f"Full update completed. {processed_count} products processed, {deactivated_count} products deactivated.",
                processed_items=processed_count,
                report=[],  # TODO: Add a sample of the data that was ingested
                stats=stats
            )

        try:
            return do_full_update()
        except Exception as e:
            raise
            return IngestionReport(
                success=False,
                message=f"Error processing full update: {str(e)}",
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
                # Handle existing record update or new record creation
                if record_data.get('sku'):
                    existing_record = self.db.query(ClientProduct).filter_by(
                        sku=record_data['sku'],
                        client_id=self.client.id
                    ).first()
                    
                    if existing_record:
                        for key, value in record_data.items():
                            if key != 'sku' and value is not None:
                                setattr(existing_record, key, value)
                        existing_record.last_changed_on = func.current_timestamp()
                        processed_count += 1
                        continue
                
                # Create new record (no SKU, empty SKU, or SKU not found)
                db_record = ClientProduct(**(record_data | {'client_id': self.client.id}))
                self.db.add(db_record)
                processed_count += 1
        
        self.db.commit()
        return processed_count

    def _apply_full_update_to_database(self, parsed_items: List[ParsedItem], ingested_skus: Set[str]) -> tuple[int, int]:
        """
        Apply full update logic: mark all ingested products as active, 
        mark all absent products as inactive.
        
        Returns: (processed_count, deactivated_count)
        """
        processed_count = 0
        
        # First, mark all existing products that are not in the ingested set as inactive
        # Note: We only deactivate products with non-empty SKUs, as empty SKUs are treated specially
        existing_products = self.db.query(ClientProduct).filter_by(client_id=self.client.id).all()
        deactivated_count = 0
        
        for existing_product in existing_products:
            # Only deactivate products with non-empty SKUs that are not in the ingested set
            if existing_product.sku and existing_product.sku not in ingested_skus:
                existing_product.active = False
                existing_product.last_changed_on = func.current_timestamp()
                deactivated_count += 1
            # For products with empty SKUs, we don't deactivate them based on the ingested set
            # They will be handled by the normal ingestion logic (create new or update existing)
        
        # Then process the ingested items, ensuring they are marked as active
        for item in parsed_items:
            assert item.is_interpreted, "Parsed item is not interpreted"
            
            # Convert parsed elements to database record
            record_data = {}
            for element in item.elements:
                record_data[element.column_name] = element.value
            
            if record_data:
                # Ensure all ingested products are marked as active
                record_data['active'] = True
                
                # Handle existing record update or new record creation
                if record_data.get('sku'):
                    existing_record = self.db.query(ClientProduct).filter_by(
                        sku=record_data['sku'],
                        client_id=self.client.id
                    ).first()
                    
                    if existing_record:
                        for key, value in record_data.items():
                            if key != 'sku' and value is not None:
                                setattr(existing_record, key, value)
                        existing_record.last_changed_on = func.current_timestamp()
                        processed_count += 1
                        continue
                
                # Create new record (no SKU, empty SKU, or SKU not found)
                db_record = ClientProduct(**(record_data | {'client_id': self.client.id}))
                self.db.add(db_record)
                processed_count += 1
        
        self.db.commit()
        return processed_count, deactivated_count