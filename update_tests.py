import sqlite3
import json
import glob
import os
import logging
import zlib
import pickle
import base64
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_PATH = "problems.db"
DATA_DIR = "/mnt/huawei/users/lfu/datasets/reasoning/lfu-code-r1-v2"

def decode_lcb_test_cases(encoded_str):
    try:
        return json.loads(encoded_str)
    except:
        try:
            return json.loads(pickle.loads(zlib.decompress(base64.b64decode(encoded_str.encode("utf-8")))))
        except Exception as e:
            logging.error(f"Failed to decode LCB test cases: {e}")
            return []

def update_tests():
    if not os.path.exists(DB_PATH):
        logging.error(f"Database {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # files = glob.glob(os.path.join(DATA_DIR, "**/req-meta-*.jsonl"), recursive=True)
    files = glob.glob(os.path.join(DATA_DIR, "code_contests/r1-0528/req-meta-gen-code-contests.jsonl"), recursive=True)
    logging.info(f"Found {len(files)} req-meta files.")

    updated_count = 0
    
    for file_path in tqdm(files, desc="Processing req-meta files"):
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    custom_id = data.get('custom_id')
                    
                    if not custom_id:
                        continue

                    # Derive problem_id
                    if custom_id.startswith('request-'):
                        problem_id = custom_id[len('request-'):]
                    else:
                        problem_id = custom_id
                    
                    source = None
                    if 'codeforce' in file_path:
                        source = 'codeforces'
                    elif 'lcb' in file_path:
                        source = 'code_generation_lite'
                    elif 'code_contests' in file_path:
                        source = 'code_contests'
                    
                    if not source:
                        # Fallback to prefix check if needed, or just continue
                        if problem_id.startswith('codeforces'):
                            source = 'codeforces'
                        elif problem_id.startswith('lcb'):
                            source = 'code_generation_lite'
                        elif problem_id.startswith('code-contests'):
                            source = 'code_contests'
                    
                    if not source:
                        continue

                    test_cases_raw = None
                    
                    if source == 'codeforces':
                        # Codeforces: use official_tests
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
                        # LCB: use public_test_cases + private_test_cases
                        public_tests = []
                        private_tests = []
                        
                        if 'public_test_cases' in data:
                            public_tests = decode_lcb_test_cases(data['public_test_cases'])
                            
                        if 'private_test_cases' in data:
                            private_tests = decode_lcb_test_cases(data['private_test_cases'])
                            
                        all_tests = public_tests + private_tests
                        if all_tests:
                            inputs = [t.get('input', '') for t in all_tests]
                            outputs = [t.get('output', '') for t in all_tests]
                            test_cases_raw = {"inputs": inputs, "outputs": outputs}

                    if test_cases_raw:
                        test_cases = json.dumps(test_cases_raw)
                        
                        # Update database
                        cursor.execute("""
                            UPDATE problems 
                            SET test_cases = ?, source = ?
                            WHERE id = ?
                        """, (test_cases, source, problem_id))
                        
                        if cursor.rowcount > 0:
                            updated_count += 1
                        
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logging.error(f"Error processing line in {file_path}: {e}")
                    continue
    
    conn.commit()
    conn.close()
    logging.info(f"Update complete. Updated {updated_count} problems.")

if __name__ == "__main__":
    update_tests()
