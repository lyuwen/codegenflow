import sqlite3
import json
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

DB_PATH = "problems.db"

def get_difficulty(mark):
    try:
        # Try to convert to int first (handles floats like 2500.0 and strings like "6")
        mark_str = str(int(float(mark)))
    except (ValueError, TypeError):
        # If not a number, use the string as is
        mark_str = str(mark)

    hard_cats = 'competition HARD VERY_HARD 3 4 5 6 1600 1700 1800 1900 2000 2100 2200 2300 2400 2500 2600 2700 2800 2900 3000 3100 3200 3300 3400 3500 3600'.split()
    easy_cats = '1 800 1000 1100 1200 1300 introductory EASY'.split()
    
    if mark_str in hard_cats:
        return "hard"
    if mark_str in easy_cats:
        return "easy"
    if mark_str in "UNKNOWN_DIFFICULTY 0".split():
        return "unknown"
    
    return "medium"

def normalize_difficulty(source, content):
    if source == 'code_generation_lite':
        # Taken as is
        return content.get('difficulty', 'unknown')
    
    raw_val = None
    if source == 'codeforces':
        raw_val = content.get('rating')
    else:
        raw_val = content.get('difficulty')
        
    if raw_val is None:
        return 'unknown'
        
    return get_difficulty(raw_val)

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    logging.info("Fetching problems...")
    cursor.execute("SELECT id, source, problem_content FROM problems")
    problems = cursor.fetchall()
    
    logging.info(f"Found {len(problems)} problems. Starting migration...")
    
    updates = []
    BATCH_SIZE = 1000
    
    for row in tqdm(problems, desc="Processing problems"):
        try:
            content = json.loads(row['problem_content'])
            difficulty = normalize_difficulty(row['source'], content)
            
            updates.append((difficulty, row['id']))
            
            if len(updates) >= BATCH_SIZE:
                cursor.executemany("UPDATE problems SET difficulty = ? WHERE id = ?", updates)
                conn.commit()
                updates = []
                
        except json.JSONDecodeError:
            logging.warning(f"Invalid JSON for problem {row['id']}")
            continue
        except Exception as e:
            logging.error(f"Error processing problem {row['id']}: {e}")
            continue
            
    if updates:
        cursor.executemany("UPDATE problems SET difficulty = ? WHERE id = ?", updates)
        conn.commit()
        
    logging.info("Migration complete.")
    conn.close()

if __name__ == "__main__":
    main()
