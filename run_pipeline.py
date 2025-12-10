from datetime import datetime
import argparse
import logging
import os
from dotenv import load_dotenv
from database import ReasoningDatabase
from processors.verifier import ResponseVerifier
from processors.importer import ResponseImporter
from processors.mapper import RequestMapper
from processors.generator import PromptGenerator
from processors.annotator import ResponseAnnotator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Load environment variables from .env file
load_dotenv()

# Default to Postgres from env, or fallback to a safe default (e.g. sqlite or error)
# User requested NOT to write raw db url in code.
DEFAULT_DB_URL = os.environ.get("DB_URL")
DEFAULT_SANDBOX_ENDPOINT = "http://127.0.0.1:8080"
# DEFAULT_SANDBOX_ENDPOINT = "http://127.0.0.1:8880"


def parse_datetime(s):
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError(f"Not a valid date: {s}")

def main():
    parser = argparse.ArgumentParser(description="Reasoning Pipeline")
    # parser.add_argument("--task", choices=["verify", "import", "map", "import-problems"], required=True, help="Task to perform")
    parser.add_argument("--db", default=DEFAULT_DB_URL, help="Database URL (Postgres or SQLite). Defaults to DB_URL in .env")
    parser.add_argument("--endpoint", default=DEFAULT_SANDBOX_ENDPOINT, help="Sandbox endpoint")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrency for verification")
    parser.add_argument("--limit", type=int, default=10000, help="Limit number of responses to verify")
    parser.add_argument("--offset", type=int, default=0, help="Offset for verification (skip first N)")
    parser.add_argument("--retry-status", help="Comma-separated list of statuses to retry (e.g., 'failed,error'). Use 'all' for everything not passed. Default: pending,error")
    parser.add_argument("--pattern", help="File pattern for import/map task")

    subparsers = parser.add_subparsers(dest="command", help="Task to perform", required=True)

    # Verify subparser
    verify_parser = subparsers.add_parser("verify", help="Verify responses")
    verify_parser.add_argument("--endpoint", default=DEFAULT_SANDBOX_ENDPOINT, help="Sandbox endpoint")
    verify_parser.add_argument("--concurrency", type=int, default=8, help="Concurrency for verification")
    verify_parser.add_argument("--limit", type=int, default=10000, help="Limit number of responses to verify")
    verify_parser.add_argument("--offset", type=int, default=0, help="Offset for verification (skip first N)")
    verify_parser.add_argument("--retry-status", help="Comma-separated list of statuses to retry (e.g., 'failed,error'). Use 'all' for everything not passed. Default: pending,error")
    verify_parser.add_argument("--dryrun", action="store_true", help="Run verification without updating the database")
    verify_parser.add_argument("--dump-tasks", help="Path to output JSONL file for offline verification tasks")
    verify_parser.add_argument("--ingest-results", nargs="*", help="Path to input JSONL file with verification results")
    verify_parser.add_argument("--failure-log", help="Path to log file for verification failures")
    
    # Import subparser
    import_parser = subparsers.add_parser("import", help="Import responses")
    import_parser.add_argument("--pattern", help="File pattern for import task")

    # Map subparser
    map_parser = subparsers.add_parser("map", help="Map requests")
    map_parser.add_argument("--pattern", help="File pattern for map task")

    # Import problems subparser
    import_problems_parser = subparsers.add_parser("import-problems", help="Import problems")
    import_problems_parser.add_argument("--pattern", help="File pattern for import-problems task")

    # Annotate subparser
    annotate_parser = subparsers.add_parser("annotate", help="Annotate passed responses with metrics")
    annotate_parser.add_argument("--concurrency", type=int, default=8, help="Concurrency for annotation")
    annotate_parser.add_argument("--limit", type=int, default=10000, help="Limit number of responses")
    annotate_parser.add_argument("--offset", type=int, default=0, help="Offset")

    # Generate subparser
    gen_parser = subparsers.add_parser("generate", help="Generate prompts for new runs")
    gen_parser.add_argument("--output", required=True, help="Output JSONL file")
    gen_parser.add_argument("--model", required=True, help="Target model name")
    gen_parser.add_argument("--difficulty", help="Filter by difficulty")
    gen_parser.add_argument("--source", help="Filter by source")
    gen_parser.add_argument("--limit", type=int, help="Limit number of prompts")
    gen_parser.add_argument("--offset", type=int, default=0, help="Offset for pagination")
    

    # Export subparser
    export_parser = subparsers.add_parser("export", help="Export passed responses")
    export_parser.add_argument("--output", required=True, help="Output JSONL file")
    export_parser.add_argument("--after", type=parse_datetime, help="Filter responses after this timestamp (ISO format or YYYY-MM-DD)")
    export_parser.add_argument("--before", type=parse_datetime, help="Filter responses before this timestamp (ISO format or YYYY-MM-DD)")
    export_parser.add_argument("--difficulty", help="Filter by difficulty (comma-separated)")
    export_parser.add_argument("--status", default="passed", help="Filter by verification status (default: passed)")

    args = parser.parse_args()
    
    if not args.db:
        print("Error: Database URL must be provided via --db or DB_URL environment variable (.env)")
        return

    # Initialize database with URL
    db = ReasoningDatabase(args.db)
    
    if args.command == "verify":
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
    elif args.command == "import":
        if not args.pattern:
            print("Error: --pattern is required for import task")
            return
        importer = ResponseImporter(args.pattern)
        importer.process(db)
    elif args.command == "map":
        if not args.pattern:
            print("Error: --pattern is required for map task")
            return
        mapper = RequestMapper(args.pattern)
        mapper.process(db)
    elif args.command == "import-problems":
        from processors.problem_importer import ProblemImporter
        if not args.pattern:
            print("Error: --pattern is required for import-problems task")
            return
        importer = ProblemImporter(args.pattern)
        importer.process(db)
    elif args.command == "annotate":
        annotator = ResponseAnnotator(db)
        annotator.process(limit=args.limit, offset=args.offset, concurrency=args.concurrency)
    elif args.command == "generate":
        # Generator needs the DB instance or URL. 
        # The original generator took db_path. We should update it to take the db instance or url.
        # Let's pass the db instance.
        generator = PromptGenerator(db)
        generator.generate(
            output_file=args.output,
            model=args.model,
            difficulty=args.difficulty,
            source=args.source,
            limit=args.limit,
            offset=args.offset
        )
    elif args.command == "export":
        from processors.exporter import ResponseExporter
        exporter = ResponseExporter(db)
        exporter.process(
            output_file=args.output,
            after=args.after,
            before=args.before,
            difficulty=args.difficulty,
            status=args.status
        )

    
    # db.close() # SQLAlchemy engine doesn't strictly need explicit close, but good practice if we want to dispose pool
    # db.engine.dispose() 

if __name__ == "__main__":
    main()
