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
DEFAULT_SANDBOX_ENDPOINT = "http://127.0.0.1:8080"
# DEFAULT_SANDBOX_ENDPOINT = "http://127.0.0.1:8880"

def main():
    parser = argparse.ArgumentParser(description="Reasoning Pipeline")
    parser.add_argument("--task", choices=["verify", "import", "map", "import-problems"], required=True, help="Task to perform")
    parser.add_argument("--db", default=DB_PATH, help="Path to SQLite database")
    parser.add_argument("--endpoint", default=DEFAULT_SANDBOX_ENDPOINT, help="Sandbox endpoint")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrency for verification")
    parser.add_argument("--limit", type=int, default=10000, help="Limit number of responses to verify")
    parser.add_argument("--offset", type=int, default=0, help="Offset for verification (skip first N)")
    parser.add_argument("--retry-status", help="Comma-separated list of statuses to retry (e.g., 'failed,error'). Use 'all' for everything not passed. Default: pending,error")
    parser.add_argument("--pattern", help="File pattern for import/map task")
    
    parser.add_argument("--dryrun", action="store_true", help="Run verification without updating the database")
    parser.add_argument("--dump-tasks", help="Path to output JSONL file for offline verification tasks")
    parser.add_argument("--ingest-results", nargs="*", help="Path to input JSONL file with verification results")
    parser.add_argument("--failure-log", help="Path to log file for verification failures")
    
    args = parser.parse_args()
    
    db = ReasoningDatabase(args.db)
    
    if args.task == "verify":
        verifier = ResponseVerifier(args.endpoint, concurrency=args.concurrency)
        
        if args.dump_tasks:
            # Parse retry statuses
            retry_statuses = None
            if args.retry_status:
                if args.retry_status.lower() == 'all':
                    retry_statuses = ['pending', 'failed', 'error', 'skipped', None]
                else:
                    retry_statuses = [s.strip() for s in args.retry_status.split(',')]
                    retry_statuses = [None if s.lower() in ['null', 'none'] else s for s in retry_statuses]
            
            verifier.dump_tasks(db, args.dump_tasks, limit=args.limit, offset=args.offset, retry_statuses=retry_statuses)
            
        elif args.ingest_results:
            for ingest_file in args.ingest_results:
                verifier.ingest_results(db, ingest_file, dryrun=args.dryrun)
            
        else:
            # Normal online verification
            # Parse retry statuses
            retry_statuses = None
            if args.retry_status:
                if args.retry_status.lower() == 'all':
                    retry_statuses = ['pending', 'failed', 'error', 'skipped', None]
                else:
                    retry_statuses = [s.strip() for s in args.retry_status.split(',')]
                    retry_statuses = [None if s.lower() in ['null', 'none'] else s for s in retry_statuses]
            
            verifier.process(db, limit=args.limit, offset=args.offset, retry_statuses=retry_statuses, dryrun=args.dryrun, failure_log=args.failure_log)
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
    elif args.task == "import-problems":
        from processors.problem_importer import ProblemImporter
        if not args.pattern:
            print("Error: --pattern is required for import-problems task")
            return
        importer = ProblemImporter(args.pattern)
        importer.process(db)
    
    db.close()

if __name__ == "__main__":
    main()
