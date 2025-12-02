import sqlite3
import json
import os
import multiprocessing
from tqdm import tqdm
import time

DB_PATH = "problems.db"
OUTPUT_FILE = "passed_responses-1.jsonl"
NUM_WORKERS = max(1, os.cpu_count() - 1)

def get_problem_text(content, source):
    """Extract problem description based on source."""
    if source == 'apps':
        return content.get('problem', '')
    elif source == 'taco':
        return content.get('problem', '')
    elif source == 'code_generation_lite':
        return content.get('question_content', '')
    else:
        # codeforces, code_contests, and others usually use 'description'
        return content.get('description', '')

def worker(input_queue, output_queue):
    """Worker process to fetch data and format it."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        while True:
            problem_id = input_queue.get()
            if problem_id is None:
                break
            
            try:
                # Fetch problem details
                cursor.execute("SELECT source, original_id, origin, difficulty, problem_content FROM problems WHERE id = ?", (problem_id,))
                prob_row = cursor.fetchone()
                
                if not prob_row:
                    continue
                
                # Fetch responses
                cursor.execute("""
                    SELECT id, model, full_response_text, reasoning_trace, completion_tokens 
                    FROM responses 
                    WHERE problem_id = ? AND verification_status = 'passed'
                """, (problem_id,))
                resp_rows = cursor.fetchall()
                
                if not resp_rows:
                    continue

                # Parse problem content
                try:
                    problem_content = json.loads(prob_row['problem_content'])
                except:
                    problem_content = {}
                    
                try:
                    origin = json.loads(prob_row['origin'])
                except:
                    origin = prob_row['origin']
                
                problem_text = get_problem_text(problem_content, prob_row['source'])
                
                # Format responses
                responses_list = []
                for r in resp_rows:
                    responses_list.append({
                        "role": "assistant",
                        "content": r['full_response_text'],
                        "reasoning_content": r['reasoning_trace'],
                        "id": r['id'],
                        "model": r['model'],
                        "completion_tokens": r['completion_tokens']
                    })
                
                output_data = {
                    "problem_id": problem_id,
                    "problem": problem_text,
                    "source": prob_row['source'],
                    "original_id": prob_row['original_id'],
                    "origin": origin,
                    "difficulty": prob_row['difficulty'],
                    "responses": responses_list
                }
                
                output_queue.put(json.dumps(output_data))
                
            except Exception as e:
                print(f"Error processing problem {problem_id}: {e}")
                
    except Exception as e:
        print(f"Worker initialization failed: {e}")
    finally:
        if conn:
            conn.close()

def writer(output_queue, total_count):
    """Writer process to write results to file."""
    with open(OUTPUT_FILE, 'w') as f:
        pbar = tqdm(total=total_count, desc="Exporting")
        count = 0
        while count < total_count:
            line = output_queue.get()
            f.write(line + "\n")
            pbar.update(1)
            count += 1
        pbar.close()

def main():
    # Get list of problems with passed responses
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    print("Fetching problem IDs...")
    cursor.execute("SELECT DISTINCT problem_id FROM responses WHERE verification_status = 'passed'")
    problem_ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    total_problems = len(problem_ids)
    print(f"Found {total_problems} problems with passed responses.")
    
    if total_problems == 0:
        return

    # Set up queues
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    
    # Fill input queue
    for pid in problem_ids:
        input_queue.put(pid)
        
    # Add poison pills for workers
    for _ in range(NUM_WORKERS):
        input_queue.put(None)
        
    # Start workers
    print(f"Starting {NUM_WORKERS} workers...")
    workers = []
    for _ in range(NUM_WORKERS):
        p = multiprocessing.Process(target=worker, args=(input_queue, output_queue))
        p.start()
        workers.append(p)
        
    # Start writer
    writer_process = multiprocessing.Process(target=writer, args=(output_queue, total_problems))
    writer_process.start()
    
    # Wait for workers
    for p in workers:
        p.join()
        
    # Wait for writer
    writer_process.join()
    
    print("Export complete.")

if __name__ == "__main__":
    main()
