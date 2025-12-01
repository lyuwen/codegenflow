import json
import logging
import glob
import os
from tqdm import tqdm
from database import ReasoningDatabase
from processors.base import Processor

class RequestMapper(Processor):
    def __init__(self, file_pattern: str):
        super().__init__("RequestMapper")
        self.file_pattern = file_pattern

    def process(self, database: ReasoningDatabase):
        files = glob.glob(self.file_pattern, recursive=True)
        if not files:
            logging.warning(f"No files found matching pattern: {self.file_pattern}")
            return

        logging.info(f"Found {len(files)} req-meta files to process.")
        
        count = 0
        for file_path in tqdm(files, desc="Mapping requests"):
            count += self._process_file(database, file_path)
            
        logging.info(f"Mapping complete. Total mappings processed: {count}")

    def _process_file(self, database: ReasoningDatabase, file_path: str) -> int:
        count = 0
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        custom_id = data.get('custom_id')
                        
                        if not custom_id:
                            continue

                        # Fix: Strip 'request-' prefix to get the correct problem ID
                        if custom_id.startswith('request-'):
                            problem_id = custom_id[len('request-'):]
                        else:
                            problem_id = custom_id
                        
                        database.insert_request_mapping(custom_id, problem_id)
                        count += 1
                        
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            
        return count
