import sqlite3
import json
import logging
from typing import Optional, Generator, Dict, Any, List

class ReasoningDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.connect()
        self.ensure_schema()

    def connect(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()

    def ensure_schema(self):
        """Ensures the database schema exists and has necessary columns."""
        # Tables should already be created by create_db.py, but we can verify or add columns
        self.add_column("responses", "verification_status", "TEXT DEFAULT 'pending'")
        self.add_column("responses", "verification_details", "JSON")
        
        # Table for mapping custom_id (from requests) to problem_id
        self.create_table_if_not_exists("request_mappings", """
            custom_id TEXT PRIMARY KEY,
            problem_id TEXT
        """)
        
        # Add indices for performance
        self.create_index("idx_responses_verification_status", "responses", "verification_status")
        self.create_index("idx_responses_problem_id", "responses", "problem_id")
        self.create_index("idx_responses_model", "responses", "model")
        self.create_index("idx_problems_source", "problems", "source")

    def create_table_if_not_exists(self, table_name: str, schema: str):
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
        self.conn.commit()

    def create_index(self, index_name: str, table: str, column: str):
        """Safely creates an index if it doesn't exist."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
            self.conn.commit()
            logging.info(f"Created index {index_name} on {table}({column})")
        except Exception as e:
            logging.error(f"Failed to create index {index_name}: {e}")

    def add_column(self, table: str, column: str, col_type: str):
        """Safely adds a column to a table if it doesn't exist."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT {column} FROM {table} LIMIT 1")
        except sqlite3.OperationalError:
            # Column doesn't exist
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                self.conn.commit()
                logging.info(f"Added column {column} to table {table}")
            except Exception as e:
                logging.error(f"Failed to add column {column} to {table}: {e}")

    def get_problems(self, limit: Optional[int] = None, offset: int = 0) -> Generator[sqlite3.Row, None, None]:
        query = "SELECT * FROM problems"
        params = []
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield row

    def get_problem(self, problem_id: str) -> Optional[sqlite3.Row]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM problems WHERE id = ?", (problem_id,))
        return cursor.fetchone()

    def get_unverified_responses(self, limit: Optional[int] = None) -> Generator[sqlite3.Row, None, None]:
        """Get responses that need verification (pending or error status)."""
        return self.get_responses_by_status(['pending', 'error', None], limit)
    
    def get_responses_by_status(self, statuses: List[Optional[str]], limit: Optional[int] = None) -> Generator[sqlite3.Row, None, None]:
        """Get responses filtered by verification status.
        
        Args:
            statuses: List of statuses to include (e.g., ['pending', 'failed', 'error'])
                     Use None to include null values
            limit: Optional limit on number of responses to return
        """
        # Build WHERE clause for multiple statuses
        conditions = []
        for status in statuses:
            if status is None:
                conditions.append("verification_status IS NULL")
            else:
                conditions.append(f"verification_status = '{status}'")
        
        where_clause = " OR ".join(conditions)
        query = f"SELECT * FROM responses WHERE {where_clause}"
        
        params = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
            
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield row

    def update_response(self, response_id: str, **kwargs):
        if not kwargs:
            return
        
        set_clauses = []
        params = []
        for key, value in kwargs.items():
            set_clauses.append(f"{key} = ?")
            if isinstance(value, (dict, list)):
                params.append(json.dumps(value))
            else:
                params.append(value)
        
        params.append(response_id)
        query = f"UPDATE responses SET {', '.join(set_clauses)} WHERE id = ?"
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self.conn.commit()

    def insert_response(self, response_data: Dict[str, Any]):
        keys = list(response_data.keys())
        placeholders = ",".join(["?"] * len(keys))
        columns = ",".join(keys)
        values = [response_data[k] for k in keys]
        
        query = f"INSERT OR IGNORE INTO responses ({columns}) VALUES ({placeholders})"
        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()

    def insert_request_mapping(self, custom_id: str, problem_id: str):
        query = "INSERT OR IGNORE INTO request_mappings (custom_id, problem_id) VALUES (?, ?)"
        cursor = self.conn.cursor()
        cursor.execute(query, (custom_id, problem_id))
        self.conn.commit()

    def get_problem_id_by_custom_id(self, custom_id: str) -> Optional[str]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT problem_id FROM request_mappings WHERE custom_id = ?", (custom_id,))
        row = cursor.fetchone()
        return row['problem_id'] if row else None
