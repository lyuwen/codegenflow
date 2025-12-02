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
        # Create tables if they don't exist
        self.create_table_if_not_exists("problems", """
            id TEXT PRIMARY KEY,
            source TEXT,
            original_id TEXT,
            problem_content JSON,
            origin JSON,
            test_cases JSON
        """)
        
        self.create_table_if_not_exists("responses", """
            id TEXT PRIMARY KEY,
            problem_id TEXT,
            model TEXT,
            full_response_text TEXT,
            full_response_json JSON,
            reasoning_trace TEXT,
            extracted_code TEXT,
            completion_tokens INTEGER,
            verifiable BOOLEAN,
            FOREIGN KEY (problem_id) REFERENCES problems (id)
        """)

        # Add columns if missing (for migrations)
        self.add_column("responses", "verification_status", "TEXT DEFAULT 'pending'")
        self.add_column("responses", "verification_details", "JSON")
        
        # Table for mapping custom_id (from requests) to problem_id
        self.create_table_if_not_exists("request_mappings", """
            custom_id TEXT PRIMARY KEY,
            problem_id TEXT
        """)
        
        # Add difficulty column to problems
        self.add_column("problems", "difficulty", "TEXT")

        # Add indices for performance
        self.create_index("idx_responses_verification_status", "responses", "verification_status")
        self.create_index("idx_responses_problem_id", "responses", "problem_id")
        self.create_index("idx_responses_model", "responses", "model")
        self.create_index("idx_problems_source", "problems", "source")
        self.create_index("idx_problems_difficulty", "problems", "difficulty")
        # Covering index for status reporting
        self.create_index("idx_responses_stats", "responses", "verification_status, problem_id, model, completion_tokens")

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

    def get_problems_by_ids(self, problem_ids: List[str]) -> Dict[str, sqlite3.Row]:
        """Fetch multiple problems by their IDs."""
        if not problem_ids:
            return {}
        
        placeholders = ",".join(["?"] * len(problem_ids))
        query = f"SELECT * FROM problems WHERE id IN ({placeholders})"
        
        cursor = self.conn.cursor()
        cursor.execute(query, problem_ids)
        
        problems = {}
        for row in cursor.fetchall():
            problems[row['id']] = row
        return problems

    def get_unverified_responses(self, limit: Optional[int] = None, offset: int = 0) -> Generator[sqlite3.Row, None, None]:
        """Get responses that need verification (pending or error status)."""
        return self.get_responses_by_status(['pending', 'error', None], limit, offset)
    
    def get_responses_by_status(self, statuses: List[Optional[str]], limit: Optional[int] = None, offset: int = 0) -> Generator[sqlite3.Row, None, None]:
        """Get responses filtered by verification status.
        
        Args:
            statuses: List of statuses to include (e.g., ['pending', 'failed', 'error'])
                     Use None to include null values
            limit: Optional limit on number of responses to return
            offset: Number of responses to skip (default 0)
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
        
        if offset > 0:
            if limit is None:
                # SQLite requires LIMIT if OFFSET is present. Use -1 for no limit.
                query += " LIMIT -1"
            query += " OFFSET ?"
            params.append(offset)
            
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            for row in rows:
                yield row

    def get_responses_with_problems(self, statuses: List[Optional[str]], limit: Optional[int] = None, offset: int = 0) -> Generator[sqlite3.Row, None, None]:
        """Get responses joined with their corresponding problem data.
        
        Args:
            statuses: List of verification statuses to filter by.
            limit: Optional limit.
            offset: Optional offset.
        """
        conditions = []
        for status in statuses:
            if status is None:
                conditions.append("r.verification_status IS NULL")
            else:
                conditions.append(f"r.verification_status = '{status}'")
        
        where_clause = " OR ".join(conditions)
        
        query = f"""
            SELECT r.id, r.problem_id, r.extracted_code, p.test_cases 
            FROM responses r
            JOIN problems p ON r.problem_id = p.id
            WHERE {where_clause}
        """
        
        params = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        
        if offset > 0:
            if limit is None:
                query += " LIMIT -1"
            query += " OFFSET ?"
            params.append(offset)
            
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            for row in rows:
                yield row

            for row in rows:
                yield row

    async def get_responses_with_problems_async(self, statuses: List[Optional[str]], limit: Optional[int] = None, offset: int = 0, num_workers: int = 1):
        """Get responses joined with their corresponding problem data asynchronously.
        
        This runs the query in separate threads to avoid blocking the asyncio loop,
        and yields results as they are fetched.
        """
        import asyncio
        import threading
        import math
        
        loop = asyncio.get_running_loop()
        q = asyncio.Queue(maxsize=1000)
        
        conditions = []
        for status in statuses:
            if status is None:
                conditions.append("r.verification_status IS NULL")
            else:
                conditions.append(f"r.verification_status = '{status}'")
        
        where_clause = " OR ".join(conditions)
        
        base_query = f"""
            SELECT r.id, r.problem_id, r.extracted_code, p.test_cases 
            FROM responses r
            JOIN problems p ON r.problem_id = p.id
            WHERE {where_clause}
        """
        
        def producer(worker_limit, worker_offset):
            try:
                # Open a new connection for this thread
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = base_query
                params = []
                
                if worker_limit is not None:
                    query += " LIMIT ?"
                    params.append(worker_limit)
                
                if worker_offset > 0:
                    if worker_limit is None:
                        query += " LIMIT -1"
                    query += " OFFSET ?"
                    params.append(worker_offset)
                
                cursor.execute(query, params)
                
                while True:
                    rows = cursor.fetchmany(1000)
                    if not rows:
                        break
                    for row in rows:
                        # Convert to dict to be safe across threads and consistent
                        asyncio.run_coroutine_threadsafe(q.put(dict(row)), loop).result()
                
                conn.close()
            except Exception as e:
                logging.error(f"Async fetch error: {e}")

        # Calculate chunks
        threads = []
        if limit is not None and num_workers > 1:
            chunk_size = math.ceil(limit / num_workers)
            for i in range(num_workers):
                w_limit = min(chunk_size, limit - i * chunk_size)
                if w_limit <= 0:
                    break
                w_offset = offset + i * chunk_size
                t = threading.Thread(target=producer, args=(w_limit, w_offset), daemon=True)
                threads.append(t)
        else:
            # Single worker
            t = threading.Thread(target=producer, args=(limit, offset), daemon=True)
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()
            
        # Waiter thread to signal completion
        def waiter():
            for t in threads:
                t.join()
            asyncio.run_coroutine_threadsafe(q.put(None), loop).result()
            
        threading.Thread(target=waiter, daemon=True).start()
        
        while True:
            item = await q.get()
            if item is None:
                break
            yield item

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

    def update_responses_batch(self, updates: List[Dict[str, Any]]):
        """
        Batch update responses.
        updates: List of dicts, each must contain 'id' and fields to update.
        """
        if not updates:
            return

        cursor = self.conn.cursor()
        try:
            self.conn.execute("BEGIN TRANSACTION")
            for update in updates:
                # Create a copy to avoid modifying the original dict if we pop
                data = update.copy()
                if 'id' not in data:
                    logging.warning(f"Skipping update without id: {data}")
                    continue
                    
                response_id = data.pop('id')
                if not data:
                    continue
                    
                set_clauses = []
                params = []
                for key, value in data.items():
                    set_clauses.append(f"{key} = ?")
                    if isinstance(value, (dict, list)):
                        params.append(json.dumps(value))
                    else:
                        params.append(value)
                
                params.append(response_id)
                query = f"UPDATE responses SET {', '.join(set_clauses)} WHERE id = ?"
                cursor.execute(query, params)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Batch update failed: {e}")
            raise

    def insert_response(self, response_data: Dict[str, Any]):
        keys = list(response_data.keys())
        placeholders = ",".join(["?"] * len(keys))
        columns = ",".join(keys)
        values = [response_data[k] for k in keys]
        
        query = f"INSERT OR IGNORE INTO responses ({columns}) VALUES ({placeholders})"
        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()

    def insert_problem(self, problem_data: Dict[str, Any]):
        keys = list(problem_data.keys())
        placeholders = ",".join(["?"] * len(keys))
        columns = ",".join(keys)
        values = [problem_data[k] for k in keys]
        
        query = f"INSERT OR IGNORE INTO problems ({columns}) VALUES ({placeholders})"
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
