import json
import os
import multiprocessing
import argparse
from datetime import datetime
from tqdm import tqdm
import time
from dotenv import load_dotenv
from sqlalchemy import select, and_
from database import ReasoningDatabase

# Load environment variables
load_dotenv()
DB_URL = os.environ.get("DB_URL")
OUTPUT_FILE = "passed_responses-1.jsonl"
NUM_WORKERS = min(4, max(1, os.cpu_count() - 1))

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

def worker(db_url, input_queue, output_queue, filters):
    """Worker process to fetch data and format it."""
    try:
        # Initialize database connection in worker
        db = ReasoningDatabase(db_url)
        
        while True:
            problem_id = input_queue.get()
            if problem_id is None:
                break
            
            try:
                # Fetch problem details
                query = select(db.problems).where(db.problems.c.id == problem_id)
                
                with db.engine.connect() as conn:
                    prob_row = conn.execute(query).fetchone()
                
                if not prob_row:
                    continue
                
                # Build response query with filters
                conditions = [
                    db.responses.c.problem_id == problem_id,
                    db.responses.c.verification_status == 'passed'
                ]
                
                if filters.get('after'):
                    conditions.append(db.responses.c.timestamp >= filters['after'])
                if filters.get('before'):
                    conditions.append(db.responses.c.timestamp <= filters['before'])
                
                resp_query = select(
                    db.responses.c.id, 
                    db.responses.c.model, 
                    db.responses.c.full_response_text, 
                    db.responses.c.reasoning_trace, 
                    db.responses.c.completion_tokens,
                    db.responses.c.timestamp
                ).where(and_(*conditions))
                
                with db.engine.connect() as conn:
                    resp_rows = conn.execute(resp_query).fetchall()
                
                if not resp_rows:
                    continue

                # Parse problem content
                problem_content = prob_row.problem_content
                if isinstance(problem_content, str):
                    try:
                        problem_content = json.loads(problem_content)
                    except:
                        problem_content = {}
                elif problem_content is None:
                    problem_content = {}
                    
                origin = prob_row.origin
                if isinstance(origin, str):
                    try:
                        origin = json.loads(origin)
                    except:
                        pass 
                
                problem_text = get_problem_text(problem_content, prob_row.source)
                
                # Format responses
                responses_list = []
                for r in resp_rows:
                    responses_list.append({
                        "role": "assistant",
                        "content": r.full_response_text,
                        "reasoning_content": r.reasoning_trace,
                        "id": r.id,
                        "model": r.model,
                        "completion_tokens": r.completion_tokens,
                        "timestamp": r.timestamp.isoformat() if r.timestamp else None
                    })
                
                output_data = {
                    "problem_id": problem_id,
                    "problem": problem_text,
                    "source": prob_row.source,
                    "original_id": prob_row.original_id,
                    "origin": origin,
                    "difficulty": prob_row.difficulty,
                    "responses": responses_list
                }
                
                output_queue.put(json.dumps(output_data))
                
            except Exception as e:
                print(f"Error processing problem {problem_id}: {e}")
                
    except Exception as e:
        print(f"Worker initialization failed: {e}")

def writer(output_queue, total_count, output_file):
    """Writer process to write results to file."""
    with open(output_file, 'w') as f:
        pbar = tqdm(total=total_count, desc="Exporting")
        count = 0
        while count < total_count:
            line = output_queue.get()
            f.write(line + "\n")
            pbar.update(1)
            count += 1
        pbar.close()

def parse_datetime(s):
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError(f"Not a valid date: {s}")

def main():
    parser = argparse.ArgumentParser(description="Export passed responses")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Output JSONL file")
    parser.add_argument("--after", type=parse_datetime, help="Filter responses after this timestamp (ISO format or YYYY-MM-DD)")
    parser.add_argument("--before", type=parse_datetime, help="Filter responses before this timestamp (ISO format or YYYY-MM-DD)")
    parser.add_argument("--difficulty", help="Filter by difficulty (comma-separated)")
    
    parser.add_argument("--db", help="Database URL or path (default: from .env)")
    
    args = parser.parse_args()

    db_url = args.db if args.db else DB_URL
    if not db_url:
        print("Error: DB_URL not found in .env and --db not provided")
        return

    # Get list of problems with passed responses
    db = ReasoningDatabase(db_url)
    print("Fetching problem IDs...")
    
    # Build query to select problem IDs
    # We join responses and problems to filter by difficulty
    # We also filter responses by status and timestamp
    
    j = db.responses.join(db.problems, db.responses.c.problem_id == db.problems.c.id)
    
    conditions = [db.responses.c.verification_status == 'passed']
    
    if args.after:
        conditions.append(db.responses.c.timestamp >= args.after)
    if args.before:
        conditions.append(db.responses.c.timestamp <= args.before)
    
    if args.difficulty:
        diffs = [d.strip() for d in args.difficulty.split(',')]
        conditions.append(db.problems.c.difficulty.in_(diffs))
        
    query = select(db.responses.c.problem_id).select_from(j).where(and_(*conditions)).distinct()
    
    with db.engine.connect() as conn:
        problem_ids = [row[0] for row in conn.execute(query).fetchall()]
    
    total_problems = len(problem_ids)
    print(f"Found {total_problems} problems matching criteria.")
    
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
        
    # Prepare filters for workers
    worker_filters = {
        'after': args.after,
        'before': args.before
    }
        
    # Start workers
    print(f"Starting {NUM_WORKERS} workers...")
    workers = []
    for _ in range(NUM_WORKERS):
        p = multiprocessing.Process(target=worker, args=(db_url, input_queue, output_queue, worker_filters))
        p.start()
        workers.append(p)
        
    # Start writer
    writer_process = multiprocessing.Process(target=writer, args=(output_queue, total_problems, args.output))
    writer_process.start()
    
    # Wait for workers
    for p in workers:
        p.join()
        
    # Wait for writer
    writer_process.join()
    
    print("Export complete.")

if __name__ == "__main__":
    main()
