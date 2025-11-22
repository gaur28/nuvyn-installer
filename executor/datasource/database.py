"""
Database data source connector
"""

import asyncio
from typing import List, Dict, Any, Optional
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from executor.datasource.base import DataSourceBase
from executor.logger import get_logger

logger = get_logger(__name__)


class DatabaseDataSource(DataSourceBase):
    """Database data source connector"""
    
    def __init__(self, credentials: Dict[str, str]):
        super().__init__(credentials)
        self.engine = None
        self.db_type = None
    
    def can_handle(self, path: str) -> bool:
        """Check if this is a database connection string"""
        return (
            path.startswith('mysql://') or
            path.startswith('postgresql://') or
            path.startswith('postgres://') or
            path.startswith('snowflake://') or
            path.startswith('mssql://') or
            path.startswith('oracle://')
        )
    
    def get_source_type(self) -> str:
        """Get the data source type"""
        return f"database_{self.db_type}" if self.db_type else "database"
    
    def validate_credentials(self) -> bool:
        """Validate database credentials"""
        required_fields = ['host', 'username', 'password', 'database']
        return all(self.credentials.get(field) for field in required_fields)
    
    async def connect(self) -> bool:
        """Establish connection to database"""
        try:
            logger.info("🔗 Connecting to database...")
            
            # Determine database type and build connection string
            connection_string = self._build_connection_string()
            self.db_type = self._detect_db_type(connection_string)
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                connection_string,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test the connection
            await self.test_connection()
            logger.info(f"✅ Database connection established ({self.db_type})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            return False
    
    async def disconnect(self):
        """Close connection to database"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("🔌 Database connection closed")
    
    async def list_files(self, path: str) -> List[str]:
        """List tables in the database (database equivalent of files)"""
        try:
            if not self.engine:
                await self.connect()
            
            # Get list of tables
            query = self._get_tables_query()
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                tables = [row[0] for row in result.fetchall()]
            
            logger.info(f"📁 Found {len(tables)} tables in database")
            return tables
            
        except Exception as e:
            logger.error(f"❌ Error listing database tables: {e}")
            return []
    
    async def get_file_size(self, file_path: str) -> int:
        """Get row count for a table (database equivalent of file size)"""
        try:
            if not self.engine:
                await self.connect()
            
            # Get row count for the table
            query = f"SELECT COUNT(*) FROM {file_path}"
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                row_count = result.fetchone()[0]
            
            logger.debug(f"📏 Row count for table {file_path}: {row_count}")
            return row_count
            
        except Exception as e:
            logger.error(f"❌ Error getting table row count: {e}")
            return 0
    
    async def read_file_sample(self, file_path: str, max_bytes: int = 1024*1024) -> bytes:
        """Read a sample of the table data (database equivalent of file sample)"""
        try:
            if not self.engine:
                await self.connect()
            
            # Get sample data from the table
            query = f"SELECT * FROM {file_path} LIMIT 1000"
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                rows = result.fetchall()
            
            # Convert to CSV-like format
            if rows:
                # Get column names
                columns = list(rows[0].keys())
                csv_data = ','.join(columns) + '\n'
                
                # Add sample rows
                for row in rows:
                    csv_data += ','.join(str(value) for value in row) + '\n'
                
                sample_data = csv_data.encode('utf-8')
            else:
                sample_data = b''
            
            logger.debug(f"📖 Read {len(sample_data)} bytes from table {file_path}")
            return sample_data
            
        except Exception as e:
            logger.error(f"❌ Error reading table sample: {e}")
            return b''
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test the database connection"""
        try:
            if not self.engine:
                return {"success": False, "error": "Not connected"}
            
            # Test connection with a simple query
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                result.fetchone()
            
            return {
                "success": True,
                "source_type": f"database_{self.db_type}",
                "connection_status": "connected"
            }
            
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "source_type": f"database_{self.db_type}",
                "connection_status": "failed"
            }
    
    def _build_connection_string(self) -> str:
        """Build database connection string from credentials"""
        host = self.credentials.get('host', 'localhost')
        port = self.credentials.get('port', '')
        username = self.credentials.get('username', '')
        password = self.credentials.get('password', '')
        database = self.credentials.get('database', '')
        
        # Determine database type
        db_type = self.credentials.get('type', 'mysql').lower()
        
        if db_type == 'mysql':
            port = port or '3306'
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type in ['postgresql', 'postgres']:
            port = port or '5432'
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == 'snowflake':
            return f"snowflake://{username}:{password}@{host}/{database}"
        
        elif db_type in ['mssql', 'sqlserver']:
            port = port or '1433'
            return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == 'oracle':
            port = port or '1521'
            return f"oracle://{username}:{password}@{host}:{port}/{database}"
        
        else:
            # Default to MySQL
            port = port or '3306'
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    
    def _detect_db_type(self, connection_string: str) -> str:
        """Detect database type from connection string"""
        if 'mysql' in connection_string:
            return 'mysql'
        elif 'postgresql' in connection_string or 'postgres' in connection_string:
            return 'postgresql'
        elif 'snowflake' in connection_string:
            return 'snowflake'
        elif 'mssql' in connection_string:
            return 'mssql'
        elif 'oracle' in connection_string:
            return 'oracle'
        else:
            return 'unknown'
    
    def _get_tables_query(self) -> str:
        """Get appropriate query to list tables based on database type"""
        if self.db_type == 'mysql':
            return "SHOW TABLES"
        elif self.db_type in ['postgresql', 'postgres']:
            return "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        elif self.db_type == 'snowflake':
            return "SHOW TABLES"
        elif self.db_type == 'mssql':
            return "SELECT name FROM sys.tables"
        elif self.db_type == 'oracle':
            return "SELECT table_name FROM user_tables"
        else:
            # Default to MySQL
            return "SHOW TABLES"
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table"""
        try:
            if not self.engine:
                await self.connect()
            
            if self.db_type == 'mysql':
                query = f"DESCRIBE {table_name}"
            elif self.db_type in ['postgresql', 'postgres']:
                query = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
                """
            elif self.db_type == 'snowflake':
                query = f"DESCRIBE TABLE {table_name}"
            else:
                query = f"DESCRIBE {table_name}"
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                columns = []
                for row in result.fetchall():
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] if len(row) > 2 else None,
                        "default": row[3] if len(row) > 3 else None
                    })
            
            return {
                "table_name": table_name,
                "columns": columns,
                "column_count": len(columns)
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting table schema: {e}")
            return {"error": str(e)}
    
    async def get_database_info(self) -> Dict[str, Any]:
        """Get general database information"""
        try:
            if not self.engine:
                await self.connect()
            
            # Get database version and basic info
            version_query = self._get_version_query()
            
            with self.engine.connect() as connection:
                result = connection.execute(text(version_query))
                version_info = result.fetchone()[0]
            
            return {
                "database_type": self.db_type,
                "version": version_info,
                "connection_status": "connected"
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting database info: {e}")
            return {"error": str(e)}
    
    def _get_version_query(self) -> str:
        """Get appropriate query to get database version"""
        if self.db_type == 'mysql':
            return "SELECT VERSION()"
        elif self.db_type in ['postgresql', 'postgres']:
            return "SELECT version()"
        elif self.db_type == 'snowflake':
            return "SELECT CURRENT_VERSION()"
        elif self.db_type == 'mssql':
            return "SELECT @@VERSION"
        elif self.db_type == 'oracle':
            return "SELECT * FROM v$version WHERE rownum = 1"
        else:
            return "SELECT 1"  # Default fallback
