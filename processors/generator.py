import json
import logging
from typing import List, Dict, Optional, Any
from sqlalchemy import select, func
from database import ReasoningDatabase

logger = logging.getLogger(__name__)

class PromptGenerator:
    def __init__(self, database: ReasoningDatabase):
        self.database = database

    def generate(self, 
                 output_file: str, 
                 model: str, 
                 difficulty: Optional[str] = None, 
                 source: Optional[str] = None, 
                 limit: Optional[int] = None, 
                 offset: int = 0):
        """
        Generates prompts for problems matching the criteria.
        """
        logger.info(f"Starting prompt generation. Output: {output_file}, Model: {model}")
        
        try:
            # Build query
            query = select(self.database.problems)
            
            if difficulty:
                query = query.where(self.database.problems.c.difficulty == difficulty)
            
            if source:
                query = query.where(self.database.problems.c.source == source)

            query = query.order_by(self.database.problems.c.id)

            if limit is not None:
                query = query.limit(limit)
            
            if offset > 0:
                query = query.offset(offset)

            # Count total for progress bar
            count_query = select(func.count()).select_from(self.database.problems)
            if difficulty:
                count_query = count_query.where(self.database.problems.c.difficulty == difficulty)
            if source:
                count_query = count_query.where(self.database.problems.c.source == source)
            
            with self.database.engine.connect() as conn:
                total_count = conn.execute(count_query).scalar()
            
            if limit is not None:
                total_count = min(total_count, limit)
                if offset > 0:
                    total_count = max(0, total_count - offset)

            count = 0
            from tqdm import tqdm
            
            with open(output_file, 'w') as f, tqdm(total=total_count, desc="Generating prompts") as pbar:
                # Stream results
                with self.database.engine.connect() as conn:
                    result = conn.execution_options(stream_results=True).execute(query)
                    
                    for row in result:
                        try:
                            # row is a SQLAlchemy Row object, access by key works
                            request_json = self._create_request(row._mapping, model)
                            if request_json:
                                f.write(json.dumps(request_json) + "\n")
                                count += 1
                                pbar.update(1)
                        except Exception as e:
                            logger.error(f"Error processing problem {row.id}: {e}")

            logger.info(f"Generated {count} prompts.")

        except Exception as e:
            logger.error(f"Generation failed: {e}")
            raise

    def _create_request(self, row: Dict[str, Any], model: str) -> Optional[Dict[str, Any]]:
        """Creates the JSON request object for a single problem."""
        problem_id = row['id']
        source = row['source']
        
        # SQLAlchemy handles JSON deserialization automatically for JSON columns
        content = row['problem_content']
        # If it's still a string (e.g. SQLite sometimes), try to parse
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in problem_content for {problem_id}")
                return None
        
        if not content:
             return None

        prompt_text = self._extract_prompt_text(content, source)
        if not prompt_text:
            logger.warning(f"Could not extract prompt text for {problem_id}")
            return None

        return {
            "custom_id": f"request-{problem_id}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt_text
                    }
                ],
                "temperature": 0.6,
                "top_p": 0.95,
                "thinking_budget": 32768,
                "max_tokens": 32768
            }
        }

    def _extract_prompt_text(self, content: Dict[str, Any], source: str) -> str:
        """Extracts the problem description/prompt based on source."""
        if source == 'apps':
            return content.get('problem', '')
        elif source == 'taco':
            return content.get('problem', '')
        elif source == 'code_generation_lite':
            return content.get('question_content', '')
        else:
            return content.get('description') or content.get('prompt') or content.get('question') or content.get('problem') or ''
