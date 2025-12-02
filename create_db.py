import sqlite3
import json
import os
import glob
import re
from tqdm import tqdm
from database import ReasoningDatabase

DB_PATH = "problems.db"
DATA_DIR = "/mnt/huawei/users/lfu/datasets/reasoning/lfu-code-r1-v2"

def get_problem_id(source, original_id):
    return f"{source}-{original_id}"

def process_req_meta(db: ReasoningDatabase):
    files = glob.glob(os.path.join(DATA_DIR, "**/req-meta-*.jsonl"), recursive=True)
    
    custom_id_map = {} # custom_id -> problem_id

    for file_path in tqdm(files, desc="Processing req-meta files"):
        source_dir = os.path.basename(os.path.dirname(os.path.dirname(file_path))) # e.g., apps, code_contests
        
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

                    # Update custom_id_map
                    custom_id_map[custom_id] = problem_id
                    
                    # Persist mapping to DB
                    db.insert_request_mapping(custom_id, problem_id)
                    
                    # Prepare data for insertion
                    test_cases_raw = data.get('test_cases', [])
                    if not test_cases_raw and 'official_tests' in data:
                        # Normalize official_tests (list of dicts) to expected format (dict of lists)
                        official_tests = data['official_tests']
                        if official_tests:
                            inputs = [t.get('input', '') for t in official_tests]
                            outputs = [t.get('output', '') for t in official_tests]
                            test_cases_raw = {"inputs": inputs, "outputs": outputs}
                    
                    # Extract difficulty if available (for code_generation_lite or others)
                    raw_difficulty = None
                    if source == 'codeforces':
                        raw_difficulty = data.get('rating')
                    elif source == 'code_generation_lite':
                        raw_difficulty = data.get('difficulty')
                    elif 'difficulty' in data:
                        raw_difficulty = data['difficulty']
                    
                    difficulty = get_difficulty(raw_difficulty)
                    
                    problem_data = {
                        "id": problem_id,
                        "source": source,
                        "original_id": original_id,
                        "problem_content": json.dumps(data),
                        "origin": json.dumps(origin),
                        "test_cases": json.dumps(test_cases_raw),
                        "difficulty": difficulty
                    }
                    
                    db.insert_problem(problem_data)
                    
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line in {file_path}: {e}")
                    continue
    
    return custom_id_map

def get_difficulty(val):
    """Normalize difficulty rating."""
    if val is None or val == "":
        return 'unknown'
    
    val_str = str(val).lower().strip()
    if val_str in ['easy', 'medium', 'hard']:
        return val_str
        
    # Try numeric (Codeforces ratings etc)
    try:
        rating = float(val)
        if rating < 1200:
            return 'easy'
        elif rating < 1600:
            return 'medium'
        else:
            return 'hard'
    except ValueError:
        pass
        
    return 'unknown'

def extract_code(text):
    # Extract code from ``` blocks
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

def process_responses(db: ReasoningDatabase, custom_id_map):
    files = glob.glob(os.path.join(DATA_DIR, "**/responses-*.jsonl"), recursive=True)
    
    for file_path in tqdm(files, desc="Processing response files"):
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    custom_id = data.get('custom_id')
                    
                    if custom_id not in custom_id_map:
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

                    response_data = {
                        "id": response_id,
                        "problem_id": problem_id,
                        "model": model,
                        "full_response_text": full_response_text,
                        "full_response_json": full_response_json,
                        "reasoning_trace": reasoning_trace,
                        "extracted_code": extracted_code,
                        "completion_tokens": completion_tokens,
                        "verifiable": verifiable,
                        "verification_status": "pending" # Default status
                    }
                    
                    db.insert_response(response_data)
                          
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line in {file_path}: {e}")
                    continue

def main():
    db = ReasoningDatabase(DB_PATH)
    # ensure_schema is called in __init__
    
    print("Processing req-meta files...")
    custom_id_map = process_req_meta(db)
    print(f"Loaded {len(custom_id_map)} problem mappings.")
    
    print("Processing response files...")
    process_responses(db, custom_id_map)
    
    db.close()
    print("Database population complete.")

if __name__ == "__main__":
    main()
