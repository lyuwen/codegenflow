import sqlite3
import shutil
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_PATH = "problems.db"
BACKUP_PATH = "problems.db.bak"

def migrate():
    if not os.path.exists(DB_PATH):
        logging.error(f"Database {DB_PATH} not found.")
        return

    # 1. Backup
    logging.info(f"Backing up {DB_PATH} to {BACKUP_PATH}...")
    shutil.copy2(DB_PATH, BACKUP_PATH)
    logging.info("Backup complete.")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN TRANSACTION")

        # 2. Update Problems
        logging.info("Migrating 'problems' table...")
        cursor.execute("SELECT * FROM problems")
        problems = cursor.fetchall()
        
        migrated_problems = 0
        deleted_problems = 0
        
        for p in problems:
            old_id = p['id']
            
            # Handle codeforces- edge case
            if old_id == 'codeforces-' or old_id.startswith('request-codeforces-'):
                 # Check if it's malformed (no suffix)
                 if old_id.endswith('-'):
                     logging.warning(f"Deleting malformed problem ID: {old_id}")
                     cursor.execute("DELETE FROM problems WHERE id = ?", (old_id,))
                     deleted_problems += 1
                     continue

            if old_id.startswith('request-'):
                new_id = old_id[len('request-'):] # Strip 'request-'
                
                # Check if new_id already exists (shouldn't, but safety first)
                cursor.execute("SELECT 1 FROM problems WHERE id = ?", (new_id,))
                if cursor.fetchone():
                    logging.warning(f"Target ID {new_id} already exists. Skipping migration for {old_id}.")
                    continue
                
                # Create new record with new ID
                # We need to get all columns dynamically to be safe
                keys = p.keys()
                values = [p[k] for k in keys]
                # Replace ID in values
                id_idx = keys.index('id')
                values = list(values)
                values[id_idx] = new_id
                
                placeholders = ",".join(["?"] * len(keys))
                columns = ",".join(keys)
                
                cursor.execute(f"INSERT INTO problems ({columns}) VALUES ({placeholders})", values)
                cursor.execute("DELETE FROM problems WHERE id = ?", (old_id,))
                migrated_problems += 1
        
        logging.info(f"Problems migrated: {migrated_problems}, Deleted: {deleted_problems}")

        # 3. Update Request Mappings
        logging.info("Migrating 'request_mappings' table...")
        cursor.execute("SELECT * FROM request_mappings")
        mappings = cursor.fetchall()
        
        migrated_mappings = 0
        
        for m in mappings:
            custom_id = m['custom_id']
            old_problem_id = m['problem_id']
            
            if old_problem_id.startswith('request-'):
                new_problem_id = old_problem_id[len('request-'):]
                cursor.execute("UPDATE request_mappings SET problem_id = ? WHERE custom_id = ?", (new_problem_id, custom_id))
                migrated_mappings += 1
            elif old_problem_id == 'codeforces-' or old_problem_id.startswith('request-codeforces-'):
                 if old_problem_id.endswith('-'):
                     # Delete mapping if it points to a deleted problem
                     cursor.execute("DELETE FROM request_mappings WHERE custom_id = ?", (custom_id,))

        logging.info(f"Mappings migrated: {migrated_mappings}")

        # 4. Update Responses
        logging.info("Migrating 'responses' table...")
        cursor.execute("SELECT id, problem_id FROM responses")
        responses = cursor.fetchall()
        
        migrated_responses = 0
        
        for r in responses:
            response_id = r['id']
            old_problem_id = r['problem_id']
            
            if old_problem_id and old_problem_id.startswith('request-'):
                new_problem_id = old_problem_id[len('request-'):]
                cursor.execute("UPDATE responses SET problem_id = ? WHERE id = ?", (new_problem_id, response_id))
                migrated_responses += 1
            elif old_problem_id == 'codeforces-' or (old_problem_id and old_problem_id.startswith('request-codeforces-') and old_problem_id.endswith('-')):
                 # Delete response or set problem_id to null? Deleting seems safer if problem is gone.
                 # Let's just set problem_id to NULL for now to preserve the response data
                 cursor.execute("UPDATE responses SET problem_id = NULL WHERE id = ?", (response_id,))

        logging.info(f"Responses migrated: {migrated_responses}")

        conn.commit()
        logging.info("Migration completed successfully.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Migration failed: {e}")
        # Restore backup? User can do it manually.
        logging.info(f"Rolled back changes. Database is in state before transaction. Backup available at {BACKUP_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
