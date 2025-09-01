from .base_client import DatabaseClient
from .arxiv import ArxivClient
from .springer import SpringerClient
from .ieeexplore import IeeeXploreClient
from .core import CoreClient
from .elsevier import ElsevierClient
from .semantic_scholar import SemanticScholarClient
from util.error_standards import ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory, get_standard_error_info
from util.logging_standards import LogCategory

class DatabaseClientFactory:
    """
    Factory class for creating database clients.
    
    This provides a clean interface for creating the right client
    based on the database name, and makes it easy to add new databases.
    """
    
    def __init__(self):
        self._clients = {
            'arxiv': ArxivClient,
            'springer': SpringerClient,
            'ieeexplore': IeeeXploreClient,
            'core': CoreClient,
            'elsevier': ElsevierClient,
            'semantic_scholar': SemanticScholarClient,
        }
    
    def create_client(self, database_name: str) -> DatabaseClient:
        """
        Create a client for the specified database.
        
        Args:
            database_name: Name of the database (e.g., 'arxiv', 'springer')
            
        Returns:
            DatabaseClient instance or None if database not supported
        """
        if database_name not in self._clients:
            return None
            
        try:
            return self._clients[database_name]()
        except (ValueError, TypeError) as e:
            # Log the error but don't crash the system
            import logging
            logger = logging.getLogger('logger')
            context = create_error_context(
                "client_factory", "create_client", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Failed to create client for {database_name} due to data type error: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, logger)
            return None
        except Exception as ex:
            # Log the error but don't crash the system
            import logging
            logger = logging.getLogger('logger')
            context = create_error_context(
                "client_factory", "create_client", 
                ErrorSeverity.ERROR, 
                ErrorCategory.SYSTEM,
                f"Failed to create client for {database_name} due to unexpected error: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, logger)
            return None
    
    def get_supported_databases(self) -> list:
        """Get list of supported database names."""
        return list(self._clients.keys())
    
    def is_supported(self, database_name: str) -> bool:
        """Check if a database is supported."""
        return database_name in self._clients
    
    def register_client(self, database_name: str, client_class):
        """
        Register a new client class for a database.
        
        This allows for dynamic registration of new clients
        without modifying the factory code.
        """
        if not issubclass(client_class, DatabaseClient):
            raise ValueError(f"Client class must inherit from DatabaseClient")
        
        self._clients[database_name] = client_class
