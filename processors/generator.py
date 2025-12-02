import json
import sqlite3
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

class PromptGenerator:
    def __init__(self, db_path: str):
        self.db_path = db_path

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
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            # Build query
            query = "SELECT id, source, problem_content FROM problems WHERE 1=1"
            params = []

            if difficulty:
                query += " AND difficulty = ?"
                params.append(difficulty)
            
            if source:
                query += " AND source = ?"
                params.append(source)

            query += " ORDER BY id"

            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            
            if offset > 0:
                query += " OFFSET ?"
                params.append(offset)

            # Count total for progress bar
            count_query = "SELECT COUNT(*) FROM problems WHERE 1=1"
            count_params = []
            if difficulty:
                count_query += " AND difficulty = ?"
                count_params.append(difficulty)
            if source:
                count_query += " AND source = ?"
                count_params.append(source)
            
            cursor.execute(count_query, count_params)
            total_count = cursor.fetchone()[0]
            
            if limit is not None:
                total_count = min(total_count, limit)
                if offset > 0:
                    total_count = max(0, total_count - offset) # Adjust logic if offset applies to limit or total

            # Re-execute main query
            cursor.execute(query, params)
            
            count = 0
            from tqdm import tqdm
            with open(output_file, 'w') as f, tqdm(total=total_count, desc="Generating prompts") as pbar:
                while True:
                    row = cursor.fetchone()
                    if not row:
                        break
                    
                    try:
                        request_json = self._create_request(row, model)
                        if request_json:
                            f.write(json.dumps(request_json) + "\n")
                            count += 1
                            pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing problem {row['id']}: {e}")

            logger.info(f"Generated {count} prompts.")

        finally:
            conn.close()

    def _create_request(self, row: sqlite3.Row, model: str) -> Optional[Dict[str, Any]]:
        """Creates the JSON request object for a single problem."""
        problem_id = row['id']
        source = row['source']
        
        try:
            content = json.loads(row['problem_content'])
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in problem_content for {problem_id}")
            return None

        prompt_text = self._extract_prompt_text(content, source)
        if not prompt_text:
            logger.warning(f"Could not extract prompt text for {problem_id}")
            return None

        # Construct the request object matching the example format
        # {"custom_id": "request-code-apps-0", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "...", "messages": [...]}}
        
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
            # codeforces, code_contests, and others usually use 'description'
            # Some might use 'prompt' or 'question'
            return content.get('description') or content.get('prompt') or content.get('question') or ''
