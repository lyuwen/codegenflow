import argparse
import logging
from database import ReasoningDatabase
from processors.verifier import ResponseVerifier
from processors.importer import ResponseImporter
from processors.mapper import RequestMapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

DB_PATH = "problems.db"
DEFAULT_SANDBOX_ENDPOINT = "http://10.200.92.162:8081"

def main():
    parser = argparse.ArgumentParser(description="Reasoning Pipeline")
    parser.add_argument("--task", choices=["verify", "import", "map"], required=True, help="Task to perform")
    parser.add_argument("--db", default=DB_PATH, help="Path to SQLite database")
    parser.add_argument("--endpoint", default=DEFAULT_SANDBOX_ENDPOINT, help="Sandbox endpoint")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrency for verification")
    parser.add_argument("--limit", type=int, default=10000, help="Limit number of responses to verify")
    parser.add_argument("--retry-status", help="Comma-separated list of statuses to retry (e.g., 'failed,error'). Use 'all' for everything not passed. Default: pending,error")
    parser.add_argument("--pattern", help="File pattern for import/map task")
    
    args = parser.parse_args()
    
    db = ReasoningDatabase(args.db)
    
    if args.task == "verify":
        # Parse retry statuses
        retry_statuses = None
        if args.retry_status:
            if args.retry_status.lower() == 'all':
                # Everything except passed
                retry_statuses = ['pending', 'failed', 'error', 'skipped', None]
            else:
                # Parse comma-separated list
                retry_statuses = [s.strip() for s in args.retry_status.split(',')]
                # Handle 'null' or 'none' as None
                retry_statuses = [None if s.lower() in ['null', 'none'] else s for s in retry_statuses]
        
        verifier = ResponseVerifier(args.endpoint, concurrency=args.concurrency)
        verifier.process(db, limit=args.limit, retry_statuses=retry_statuses)
    elif args.task == "import":
        if not args.pattern:
            print("Error: --pattern is required for import task")
            return
        importer = ResponseImporter(args.pattern)
        importer.process(db)
    elif args.task == "map":
        if not args.pattern:
            print("Error: --pattern is required for map task")
            return
        mapper = RequestMapper(args.pattern)
        mapper.process(db)
    
    db.close()

if __name__ == "__main__":
    main()
