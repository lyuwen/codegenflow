import os
import json
import logging
import math
import asyncio
import threading
from typing import Optional, Generator, Dict, Any, List, Union
from datetime import datetime

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Boolean, 
    Text, TIMESTAMP, JSON, select, text, and_, or_, func, inspect
)
from sqlalchemy.engine import Engine, Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReasoningDatabase:
    def __init__(self, db_url: str):
        """
        Initialize the database connection.
        
        Args:
            db_url: Database connection URL (e.g., 'postgresql://user:pass@host:port/dbname' 
                   or 'sqlite:///path/to/db.sqlite').
                   If a file path is provided, it's treated as a SQLite database.
        """
        if not db_url:
            raise ValueError("Database URL cannot be empty")
            
        logger.info(f"ReasoningDatabase initialized with: {db_url}")

        # Backward compatibility: if it looks like a file path (doesn't start with scheme://), treat as sqlite
        if "://" not in db_url:
            self.db_url = f"sqlite:///{db_url}"
        else:
            self.db_url = db_url
            
        logger.info(f"Resolved database URL: {self.db_url}")
        
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self._define_schema()
        self.ensure_schema()

    def _create_engine(self) -> Engine:
        """Create and configure the SQLAlchemy engine."""
        # For SQLite, we need to handle concurrent access if using multiple threads
        if self.db_url.startswith('sqlite'):
            return create_engine(
                self.db_url, 
                connect_args={'check_same_thread': False},
                poolclass=QueuePool,
                pool_size=20,
                max_overflow=30
            )
        else:
            # PostgreSQL and others
            return create_engine(
                self.db_url,
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True
            )

    def _define_schema(self):
        """Define the database schema using SQLAlchemy Core."""
        self.problems = Table(
            'problems', self.metadata,
            Column('id', String, primary_key=True),
            Column('source', String),
            Column('original_id', String),
            Column('problem_content', JSON), # SQLAlchemy handles JSON serialization
            Column('origin', JSON),
            Column('test_cases', JSON),
            Column('difficulty', String)
        )

        self.responses = Table(
            'responses', self.metadata,
            Column('id', String, primary_key=True),
            Column('problem_id', String, index=True),
            Column('model', String, index=True),
            Column('full_response_text', Text),
            Column('full_response_json', JSON),
            Column('reasoning_trace', Text),
            Column('extracted_code', Text),
            Column('completion_tokens', Integer),
            Column('verifiable', Boolean),
            Column('timestamp', TIMESTAMP, index=True),
            Column('verification_status', String, server_default='pending', index=True),
            Column('verification_details', JSON)
        )

        self.request_mappings = Table(
            'request_mappings', self.metadata,
            Column('custom_id', String, primary_key=True),
            Column('problem_id', String)
        )

        self.response_annotations = Table(
            'response_annotations', self.metadata,
            Column('response_id', String, primary_key=True),
            Column('cr', JSON),
            Column('lrr', JSON),
            Column('max_line_len', Integer),
            Column('token_repetition', Boolean),
            Column('lang_bad', Boolean),
            Column('lang_reasons', JSON),
            Column('safe_cjk', Boolean),
            Column('flaw_backtracking', Integer),
            Column('flaw_uncertainty', Integer),
            Column('high_paragraph_count', Boolean),
            Column('sequential_paragraph_repeat', Boolean),
            Column('intra_paragraph_repetition', Boolean),
            Column('high_ngram_repetition', JSON)
        )

    def ensure_schema(self):
        """Create tables if they don't exist."""
        try:
            self.metadata.create_all(self.engine)
            # Verify columns and add if missing (migration support)
            inspector = inspect(self.engine)
            
            # Check responses table columns
            existing_cols = {c['name'] for c in inspector.get_columns('responses')}
            
            # Migration logic for columns that might be missing in older schemas
            # Note: SQLAlchemy create_all skips existing tables, so we check manually
            with self.engine.connect() as conn:
                if 'verification_status' not in existing_cols:
                    conn.execute(text("ALTER TABLE responses ADD COLUMN verification_status TEXT DEFAULT 'pending'"))
                    conn.execute(text("CREATE INDEX idx_responses_verification_status ON responses (verification_status)"))
                
                if 'verification_details' not in existing_cols:
                    # Syntax depends on DB. Postgres: JSON, SQLite: JSON/TEXT
                    col_type = "JSONB" if "postgresql" in self.db_url else "JSON"
                    conn.execute(text(f"ALTER TABLE responses ADD COLUMN verification_details {col_type}"))

                if 'timestamp' not in existing_cols:
                    col_type = "TIMESTAMP"
                    conn.execute(text(f"ALTER TABLE responses ADD COLUMN timestamp {col_type}"))
                    conn.execute(text("CREATE INDEX idx_responses_timestamp ON responses (timestamp)"))
            
            # Check response_annotations table
            if not inspector.has_table('response_annotations'):
                try:
                    self.response_annotations.create(self.engine)
                except Exception as e:
                    logger.warning(f"Could not create response_annotations table (might exist): {e}")

            logger.info("Schema ensured.")
        except SQLAlchemyError as e:
            logger.error(f"Schema creation failed: {e}")
            raise

    def get_problems(self, limit: Optional[int] = None, offset: int = 0) -> Generator[Dict[str, Any], None, None]:
        query = select(self.problems)
        if limit is not None:
            query = query.limit(limit)
        if offset > 0:
            query = query.offset(offset)
        
        with self.engine.connect() as conn:
            # Stream results for large datasets
            result = conn.execution_options(stream_results=True).execute(query)
            for row in result:
                yield row._mapping

    def get_problem(self, problem_id: str) -> Optional[Dict[str, Any]]:
        query = select(self.problems).where(self.problems.c.id == problem_id)
        with self.engine.connect() as conn:
            row = conn.execute(query).fetchone()
            return row._mapping if row else None

    def get_problems_by_ids(self, problem_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        if not problem_ids:
            return {}
        
        query = select(self.problems).where(self.problems.c.id.in_(problem_ids))
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return {row.id: row._mapping for row in result}

    def get_unverified_responses(self, limit: Optional[int] = None, offset: int = 0) -> Generator[Dict[str, Any], None, None]:
        return self.get_responses_by_status(['pending', 'error', None], limit, offset)

    def get_responses_by_status(self, statuses: List[Optional[str]], limit: Optional[int] = None, offset: int = 0) -> Generator[Dict[str, Any], None, None]:
        conditions = []
        for status in statuses:
            if status is None:
                conditions.append(self.responses.c.verification_status.is_(None))
            else:
                conditions.append(self.responses.c.verification_status == status)
        
        query = select(self.responses).where(or_(*conditions))
        
        if limit is not None:
            query = query.limit(limit)
        if offset > 0:
            query = query.offset(offset)
            
        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(query)
            for row in result:
                yield row._mapping

    def get_responses_with_problems(self, statuses: List[Optional[str]], limit: Optional[int] = None, offset: int = 0) -> Generator[Dict[str, Any], None, None]:
        conditions = []
        for status in statuses:
            if status is None:
                conditions.append(self.responses.c.verification_status.is_(None))
            else:
                conditions.append(self.responses.c.verification_status == status)
        
        query = select(
            self.responses.c.id,
            self.responses.c.problem_id,
            self.responses.c.extracted_code,
            self.problems.c.test_cases
        ).select_from(
            self.responses.join(self.problems, self.responses.c.problem_id == self.problems.c.id)
        ).where(or_(*conditions))
        
        if limit is not None:
            query = query.limit(limit)
        if offset > 0:
            query = query.offset(offset)
            
        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(query)
            for row in result:
                yield row._mapping

    async def get_responses_with_problems_async(self, statuses: List[Optional[str]], limit: Optional[int] = None, offset: int = 0, num_workers: int = 1):
        """
        Async fetcher using threads and SQLAlchemy engine pool.
        """
        loop = asyncio.get_running_loop()
        q = asyncio.Queue(maxsize=1000)
        
        def producer(worker_limit, worker_offset):
            try:
                # Use a separate connection for each thread
                with self.engine.connect() as conn:
                    conditions = []
                    for status in statuses:
                        if status is None:
                            conditions.append(self.responses.c.verification_status.is_(None))
                        else:
                            conditions.append(self.responses.c.verification_status == status)
                    
                    query = select(
                        self.responses.c.id,
                        self.responses.c.problem_id,
                        self.responses.c.extracted_code,
                        self.problems.c.test_cases
                    ).select_from(
                        self.responses.join(self.problems, self.responses.c.problem_id == self.problems.c.id)
                    ).where(or_(*conditions))
                    
                    if worker_limit is not None:
                        query = query.limit(worker_limit)
                    if worker_offset > 0:
                        query = query.offset(worker_offset)
                        
                    result = conn.execution_options(stream_results=True).execute(query)
                    for row in result:
                        # Convert to dict
                        asyncio.run_coroutine_threadsafe(q.put(dict(row._mapping)), loop).result()
            except Exception as e:
                logger.error(f"Async fetch error: {e}")

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
            t = threading.Thread(target=producer, args=(limit, offset), daemon=True)
            threads.append(t)

        for t in threads:
            t.start()
            
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
        
        stmt = self.responses.update().where(self.responses.c.id == response_id).values(**kwargs)
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def update_responses_batch(self, updates: List[Dict[str, Any]]):
        if not updates:
            return
            
        # SQLAlchemy Core execute(stmt, params) does batch updates if params is a list of dicts
        # But we need to ensure the statement uses bind parameters for values
        # AND the where clause matches.
        # Standard bulk update in SQLAlchemy is often done via:
        # conn.execute(table.update().where(table.c.id == bindparam('b_id')), values)
        # But here 'values' keys must match bindparams.
        
        # Simpler approach: Group by keys or just loop (transactional)
        # For maximum performance with Postgres, we might want to use specific dialect features,
        # but for generic SQL, a loop inside a transaction is safe.
        
        with self.engine.begin() as conn:
            for update_data in updates:
                data = update_data.copy()
                if 'id' not in data:
                    continue
                r_id = data.pop('id')
                if not data:
                    continue
                
                stmt = self.responses.update().where(self.responses.c.id == r_id).values(**data)
                conn.execute(stmt)


    def insert_responses_batch(self, responses: List[Dict[str, Any]]):
        if not responses:
            return

        if self.engine.dialect.name == 'sqlite':
            # For SQLite, prefix_with('OR IGNORE') logic
            stmt = self.responses.insert().prefix_with('OR IGNORE')
        elif self.engine.dialect.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert
            # For Postgres, ON CONFLICT DO NOTHING
            stmt = insert(self.responses).on_conflict_do_nothing()
        else:
             # Fallback
            stmt = self.responses.insert()

        with self.engine.begin() as conn:
            conn.execute(stmt, responses)

    def insert_response(self, response_data: Dict[str, Any]):
        self.insert_responses_batch([response_data])

    def insert_problem(self, problem_data: Dict[str, Any]):
        stmt = self.problems.insert().values(**problem_data)
        
        if self.engine.dialect.name == 'sqlite':
            stmt = stmt.prefix_with('OR IGNORE')
        elif self.engine.dialect.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert
            stmt = insert(self.problems).values(**problem_data).on_conflict_do_nothing()
            
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def insert_request_mapping(self, custom_id: str, problem_id: str):
        data = {'custom_id': custom_id, 'problem_id': problem_id}
        stmt = self.request_mappings.insert().values(**data)
        
        if self.engine.dialect.name == 'sqlite':
            stmt = stmt.prefix_with('OR IGNORE')
        elif self.engine.dialect.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert
            stmt = insert(self.request_mappings).values(**data).on_conflict_do_nothing()
            
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def get_problem_id_by_custom_id(self, custom_id: str) -> Optional[str]:
        query = select(self.request_mappings.c.problem_id).where(self.request_mappings.c.custom_id == custom_id)
        with self.engine.connect() as conn:
            return conn.execute(query).scalar()

    def insert_annotations_batch(self, annotations: List[Dict[str, Any]]):
        if not annotations:
            return
            
        if self.engine.dialect.name == 'sqlite':
            stmt = self.response_annotations.insert().prefix_with('OR REPLACE')
        elif self.engine.dialect.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert
            stmt = insert(self.response_annotations).values(annotations)
            stmt = stmt.on_conflict_do_update(
                index_elements=['response_id'],
                set_={c.name: c for c in stmt.excluded if c.name != 'response_id'}
            )
        else:
            stmt = self.response_annotations.insert()

        with self.engine.begin() as conn:
            if self.engine.dialect.name == 'postgresql':
                conn.execute(stmt)
            elif self.engine.dialect.name == 'sqlite':
                 conn.execute(stmt, annotations)
            else:
                 conn.execute(stmt, annotations)
