import time
import json
import logging
import glob
import os
from typing import List
from tqdm import tqdm
from database import ReasoningDatabase
from processors.base import Processor

class ResponseImporter(Processor):
    def __init__(self, file_pattern: str):
        super().__init__("ResponseImporter")
        self.file_pattern = file_pattern

    def process(self, database: ReasoningDatabase):
        files = glob.glob(self.file_pattern, recursive=True)
        if not files:
            logging.warning(f"No files found matching pattern: {self.file_pattern}")
            return

        logging.info(f"Found {len(files)} files to import.")
        
        total_imported = 0
        for file_path in tqdm(files, desc="Importing files"):
            total_imported += self._import_file(database, file_path)
            
        logging.info(f"Import complete. Total records processed: {total_imported}")

    def _import_file(self, database: ReasoningDatabase, file_path: str) -> int:
        count = 0
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        custom_id = data.get('custom_id')
                        
                        if not custom_id:
                            continue
                            
                        # Check if response already exists (optimization to avoid parsing if not needed)
                        # But INSERT OR IGNORE handles it. 
                        # However, we need problem_id.
                        
                        # Fix: Strip 'request-' prefix to get the correct problem ID directly
                        # This avoids relying on request_mappings for the problem_id itself,
                        # but we still need to know if the problem exists in the DB?
                        # Actually, importer relies on database.get_problem_id_by_custom_id
                        # which queries request_mappings. 
                        # Since we are rebuilding request_mappings with the new ID format,
                        # this lookup should work fine if mappings are populated.
                        # BUT, if we want to be robust, we can also derive it here.
                        
                        problem_id = database.get_problem_id_by_custom_id(custom_id)
                        if not problem_id:
                            # Fallback: try to derive it
                            if custom_id.startswith('request-'):
                                problem_id = custom_id[len('request-'):]
                            else:
                                problem_id = custom_id
                            # logging.debug(f" inferred problem_id {problem_id} for {custom_id}")

                        response_obj = data.get('response', {})
                        if not response_obj or 'body' not in response_obj:
                            continue
                            
                        body = response_obj['body']
                        choices = body.get('choices', [])
                        if not choices:
                            continue
                            
                        full_response_text = choices[0]['message']['content']
                        model = body.get('model', 'unknown')
                        completion_tokens = body.get('usage', {}).get('completion_tokens', 0)
                        created = body.get('created', time.time())
                        
                        # Extract reasoning and code (reusing logic or duplicating simple logic)
                        # Ideally we move extraction logic to a util
                        reasoning_trace = self._extract_reasoning(full_response_text, body)
                        extracted_code = self._extract_code(full_response_text)
                        
                        response_id = body.get('id', custom_id)

                        record = {
                            "id": response_id,
                            "problem_id": problem_id,
                            "model": model,
                            "full_response_text": full_response_text,
                            "full_response_json": json.dumps(response_obj),
                            "reasoning_trace": reasoning_trace,
                            "extracted_code": extracted_code,
                            "completion_tokens": completion_tokens,
                            "verifiable": True, # Default
                            "verification_status": "pending",
                            "timestamp": created
                        }
                        
                        database.insert_response(record)
                        count += 1
                        
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            
        return count

    def _extract_code(self, text):
        import re
        matches = re.findall(r'```[^\n]*\n(.*?)```', text, re.DOTALL)
        if matches:
            return "\n\n".join(matches)
        return ""

    def _extract_reasoning(self, text, json_body=None):
        import re
        # Check for <think> tags
        think_match = re.search(r'<think>(.*?)</think>', text, re.DOTALL)
        if think_match:
            return think_match.group(1).strip()
        
        # Check for reasoning_content in json body
        if json_body and 'choices' in json_body and len(json_body['choices']) > 0:
            choice = json_body['choices'][0]
            if 'message' in choice and 'reasoning_content' in choice['message']:
                 return choice['message']['reasoning_content']

        # Fallback
        code_start = text.find('```')
        if code_start != -1:
            return text[:code_start].strip()
        
        return ""
