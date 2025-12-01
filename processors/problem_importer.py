import json
import logging
import glob
import os
import zlib
import pickle
import base64
from tqdm import tqdm
from database import ReasoningDatabase
from processors.base import Processor

class ProblemImporter(Processor):
    def __init__(self, file_pattern: str):
        super().__init__("ProblemImporter")
        self.file_pattern = file_pattern

    def process(self, database: ReasoningDatabase):
        files = glob.glob(self.file_pattern, recursive=True)
        if not files:
            logging.warning(f"No files found matching pattern: {self.file_pattern}")
            return

        logging.info(f"Found {len(files)} req-meta files to import.")
        
        total_imported = 0
        for file_path in tqdm(files, desc="Importing problems"):
            total_imported += self._import_file(database, file_path)
            
        logging.info(f"Import complete. Total problems processed: {total_imported}")

    def _decode_lcb_test_cases(self, encoded_str):
        try:
            return json.loads(encoded_str)
        except:
            try:
                return json.loads(pickle.loads(zlib.decompress(base64.b64decode(encoded_str.encode("utf-8")))))
            except Exception as e:
                logging.error(f"Failed to decode LCB test cases: {e}")
                return []

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

                        # Determine source
                        source = 'unknown'
                        if 'apps' in file_path:
                            source = 'apps'
                        elif 'code_contests' in file_path:
                            source = 'code_contests'
                        elif 'taco' in file_path:
                            source = 'taco'
                        elif 'codeforce' in file_path:
                            source = 'codeforces'
                        elif 'lcb' in file_path:
                            source = 'code_generation_lite'
                        
                        if 'source' in data:
                            origin = data['source']
                        else:
                            origin = source

                        # Derive problem_id
                        if custom_id.startswith('request-'):
                            problem_id = custom_id[len('request-'):]
                        else:
                            problem_id = custom_id

                        # Determine original_id
                        if 'id' in data:
                            original_id = str(data['id'])
                        elif 'cf_contest_id' in data and 'cf_index' in data:
                            original_id = f"{data['cf_contest_id']}_{data['cf_index']}"
                        elif 'name' in data:
                             original_id = data['name']
                        else:
                             original_id = problem_id

                        # Prepare test_cases
                        test_cases_raw = None
                        
                        if source == 'codeforces':
                            test_cases_raw = data.get('test_cases', [])
                            if not test_cases_raw and 'official_tests' in data:
                                official_tests = data['official_tests']
                                if official_tests:
                                    inputs = [t.get('input', '') for t in official_tests]
                                    outputs = [t.get('output', '') for t in official_tests]
                                    test_cases_raw = {"inputs": inputs, "outputs": outputs}
                        
                        elif source == 'code_contests':
                            # Code Contests: merge public, private, and generated tests
                            inputs = []
                            outputs = []
                            
                            for key in ['public_tests', 'private_tests', 'generated_tests']:
                                if key in data and data[key]:
                                    key_inputs = data[key].get('input', [])
                                    key_outputs = data[key].get('output', [])
                                    if key_inputs and key_outputs:
                                        inputs.extend(key_inputs)
                                        outputs.extend(key_outputs)
                            
                            if inputs:
                                test_cases_raw = {"inputs": inputs, "outputs": outputs}
                        
                        elif source == 'code_generation_lite':
                            public_tests = []
                            private_tests = []
                            
                            if 'public_test_cases' in data:
                                public_tests = self._decode_lcb_test_cases(data['public_test_cases'])
                                
                            if 'private_test_cases' in data:
                                private_tests = self._decode_lcb_test_cases(data['private_test_cases'])
                                
                            all_tests = public_tests + private_tests
                            if all_tests:
                                inputs = [t.get('input', '') for t in all_tests]
                                outputs = [t.get('output', '') for t in all_tests]
                                test_cases_raw = {"inputs": inputs, "outputs": outputs}
                        
                        if not test_cases_raw:
                             test_cases_raw = data.get('test_cases', [])

                        test_cases = json.dumps(test_cases_raw)
                        problem_content = json.dumps(data)

                        record = {
                            "id": problem_id,
                            "source": source,
                            "original_id": original_id,
                            "problem_content": problem_content,
                            "origin": origin,
                            "test_cases": test_cases
                        }
                        
                        database.insert_problem(record)
                        count += 1
                        
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            
        return count
