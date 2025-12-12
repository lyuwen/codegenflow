import os
import json
from tqdm import tqdm
from sqlalchemy import create_engine
from dotenv import load_dotenv
from database import ReasoningDatabase

load_dotenv()
DB_URL = os.environ.get("DB_URL") 
INPUT_FILE = "all_problems_contam.jsonl"

def main():
    print(f"Connecting to database at {DB_URL}...")
    db = ReasoningDatabase(DB_URL)
    
    print(f"Reading {INPUT_FILE}...")
    
    batch_size = 5000
    batch = []
    total_inserted = 0
    
    with open(INPUT_FILE, 'r') as f:
        # Count lines first for tqdm? Or just iterate? 
        # Tqdm file iteration is good if we don't know total.
        for line in tqdm(f, desc="Importing Annotations"):
            if not line.strip(): continue
            try:
                data = json.loads(line)
                
                # Mapping
                # _matched_benchmark -> matched_benchmark_problem
                # _matched_benchmark_dataset -> matched_benchmark_dataset
                # _similarity -> matched_similarity
                
                # Check required fields?
                # problem_id is required for FK logic (though strict constraint might not exist in SQLite/some configs, logically required)
                pid = data.get('problem_id')
                if not pid:
                     continue
                     
                annot = {
                    "problem_id": pid,
                    "matched_benchmark_problem": data.get('_matched_benchmark'),
                    "matched_benchmark_dataset": data.get('_matched_benchmark_dataset'),
                    "matched_similarity": data.get('_similarity')
                }
                
                batch.append(annot)
                
                if len(batch) >= batch_size:
                    db.insert_problem_annotations_batch(batch)
                    total_inserted += len(batch)
                    batch = []
                    
            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"Error processing line: {e}")

    # Insert remaining
    if batch:
        db.insert_problem_annotations_batch(batch)
        total_inserted += len(batch)
        
    print(f"Done. Inserted {total_inserted} annotations.")

if __name__ == "__main__":
    main()
