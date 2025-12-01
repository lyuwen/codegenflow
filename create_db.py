import sqlite3
import json
import os
import glob
import re
from tqdm import tqdm

DB_PATH = "problems.db"
DATA_DIR = "/mnt/huawei/users/lfu/datasets/reasoning/lfu-code-r1-v2"

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS problems (
            id TEXT PRIMARY KEY,
            source TEXT,
            original_id TEXT,
            problem_content JSON,
            origin JSON,
            test_cases JSON
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS responses (
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
        )
    """)
    conn.commit()

def get_problem_id(source, original_id):
    return f"{source}-{original_id}"

def process_req_meta(conn):
    cursor = conn.cursor()
    files = glob.glob(os.path.join(DATA_DIR, "**/req-meta-*.jsonl"), recursive=True)
    
    custom_id_map = {} # custom_id -> problem_id

    for file_path in tqdm(files, desc="Processing req-meta files"):
        source_dir = os.path.basename(os.path.dirname(os.path.dirname(file_path))) # e.g., apps, code_contests
        # Adjust source extraction if needed, but 'apps', 'code_contests' seems consistent
        # Actually, let's rely on the filename or content 'source' field if available
        
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    custom_id = data.get('custom_id')
                    
                    # Determine source and original_id
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
                    else:
                        source = 'unknown'

                    if 'source' in data:
                        origin = data['source']
                    else:
                        origin = source

                    # Strip 'request-' prefix to get the correct problem ID
                    if custom_id.startswith('request-'):
                        problem_id = custom_id[len('request-'):]
                    else:
                        problem_id = custom_id
                    if 'id' in data:
                        original_id = str(data['id'])
                    elif 'cf_contest_id' in data and 'cf_index' in data:
                        original_id = f"{data['cf_contest_id']}_{data['cf_index']}"
                    elif 'name' in data:
                         original_id = data['name']
                    else:
                        # Fallback to custom_id suffix if no other ID found
                        # custom_id format: request-{task}-{i}
                         original_id = problem_id

                    # Problem ID should be the original_id (which now includes source like 'apps-0')
                    # The previous logic was f"{source}-{original_id}", but if original_id is 'apps-0', we don't want 'apps-apps-0'.
                    # Let's check if original_id already contains the source.
                    
                    # Actually, the user wants to "extract the custom_id and remove the prefix 'requests-'".
                    # So if custom_id is 'request-code-apps-0', the problem_id should be 'code-apps-0'.
                    # This seems to be what the user implies.
                    
                    # problem_id = original_id
                    
                    # Update custom_id_map
                    custom_id_map[custom_id] = problem_id
                    
                    # Prepare data for insertion
                    test_cases_raw = data.get('test_cases', [])
                    if not test_cases_raw and 'official_tests' in data:
                        # Normalize official_tests (list of dicts) to expected format (dict of lists)
                        official_tests = data['official_tests']
                        if official_tests:
                            inputs = [t.get('input', '') for t in official_tests]
                            outputs = [t.get('output', '') for t in official_tests]
                            test_cases_raw = {"inputs": inputs, "outputs": outputs}
                    
                    test_cases = json.dumps(test_cases_raw)
                    problem_content = json.dumps(data)
                    
                    cursor.execute("""
                        INSERT OR IGNORE INTO problems (id, source, original_id, problem_content, origin, test_cases)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (problem_id, source, original_id, problem_content, origin, test_cases))
                    
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line in {file_path}: {e}")
                    continue
    
    conn.commit()
    return custom_id_map

def extract_code(text):
    # Extract code from ``` blocks
    # If multiple blocks, join them or take the last one? 
    # Usually the solution is in a python block.
    # Let's extract all code blocks.
    matches = re.findall(r'```[^\n]*\n(.*?)```', text, re.DOTALL)
    if matches:
        return "\n\n".join(matches)
    return ""

def extract_reasoning(text, json_body=None):
    # Check for <think> tags
    think_match = re.search(r'<think>(.*?)</think>', text, re.DOTALL)
    if think_match:
        return think_match.group(1).strip()
    
    # Check for reasoning_content in json body (DeepSeek style)
    if json_body and 'choices' in json_body and len(json_body['choices']) > 0:
        choice = json_body['choices'][0]
        if 'message' in choice and 'reasoning_content' in choice['message']:
             return choice['message']['reasoning_content']

    # Fallback: if code block exists, take text before it
    code_start = text.find('```')
    if code_start != -1:
        return text[:code_start].strip()
    
    return ""

def process_responses(conn, custom_id_map):
    cursor = conn.cursor()
    files = glob.glob(os.path.join(DATA_DIR, "**/responses-*.jsonl"), recursive=True)
    
    for file_path in tqdm(files, desc="Processing response files"):
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    custom_id = data.get('custom_id')
                    
                    if custom_id not in custom_id_map:
                        # Try to infer problem_id if missing from map (maybe partial run)
                        # But for now, skip if not linked
                        continue
                        
                    problem_id = custom_id_map[custom_id]
                    
                    response_obj = data.get('response', {})
                    if not response_obj:
                         continue

                    # Handle case where response might be an error or different structure
                    if 'body' not in response_obj:
                        continue
                        
                    body = response_obj['body']
                    choices = body.get('choices', [])
                    if not choices:
                        continue
                        
                    full_response_text = choices[0]['message']['content']
                    model = body.get('model', 'unknown')
                    completion_tokens = body.get('usage', {}).get('completion_tokens', 0)
                    
                    full_response_json = json.dumps(response_obj)
                    
                    reasoning_trace = extract_reasoning(full_response_text, body)
                    extracted_code = extract_code(full_response_text)
                    
                    # Determine verifiable (placeholder logic, can be refined)
                    verifiable = True 
                    
                    # Response ID from the API response, or generate one
                    response_id = body.get('id', custom_id) # Use API ID if available

                    cursor.execute("""
                        INSERT OR IGNORE INTO responses (
                            id, problem_id, model, full_response_text, full_response_json,
                            reasoning_trace, extracted_code, completion_tokens, verifiable
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (response_id, problem_id, model, full_response_text, full_response_json,
                          reasoning_trace, extracted_code, completion_tokens, verifiable))
                          
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line in {file_path}: {e}")
                    continue
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    
    print("Processing req-meta files...")
    custom_id_map = process_req_meta(conn)
    print(f"Loaded {len(custom_id_map)} problem mappings.")
    
    print("Processing response files...")
    process_responses(conn, custom_id_map)
    
    conn.close()
    print("Database population complete.")

if __name__ == "__main__":
    main()
