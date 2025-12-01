import json
import logging
import asyncio
import logging
import asyncio
import argparse
import sys
import os
from typing import Dict, Any, List, Optional
from tqdm import tqdm
from database import ReasoningDatabase
from processors.base import Processor
from sandbox_fusion import RunCodeRequest, RunCodeResponse, run_code, set_endpoint
from sandbox_fusion.models import RunStatus
import re
import ast

# Increase the limit for integer string conversion to handle edge cases
sys.set_int_max_str_digits(100000)

import_string = "from string import *\nfrom re import *\nfrom datetime import *\nfrom collections import *\nfrom heapq import *\nfrom bisect import *\nfrom copy import *\nfrom math import *\nfrom random import *\nfrom statistics import *\nfrom itertools import *\nfrom functools import *\nfrom operator import *\nfrom io import *\nfrom sys import *\nfrom json import *\nfrom builtins import *\nfrom typing import *\nimport string\nimport re\nimport datetime\nimport collections\nimport heapq\nimport bisect\nimport copy\nimport math\nimport random\nimport statistics\nimport itertools\nimport functools\nimport operator\nimport io\nimport sys\nimport json\nsys.setrecursionlimit(50000)\n"

# Reusing logic from run_sandbox_tests_parallel.py where appropriate

def inside_solution_class(code: str, fn_name: str) -> bool:
    pattern = rf"class\s+Solution\s*:\s*(?:.|\n)*?def\s+{re.escape(fn_name)}\s*\("
    return re.search(pattern, code) is not None

def clean_sandbox_output(output: str) -> str:
    """Remove known sandbox messages from output."""
    if not output:
        return output
    
    # Remove known sandbox initialization messages
    lines = output.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Skip known sandbox messages
        if line.strip() == "User customization module loaded!":
            continue
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def normalize_stdio_output(output: str) -> str:
    """Normalize stdio output by removing leading/trailing whitespace from each line.
    
    This handles test case formatting inconsistencies where expected outputs
    may have trailing or leading spaces that don't affect correctness.
    """
    if not output:
        return output
    
    # Strip leading and trailing whitespace from each line
    lines = output.split('\n')
    normalized_lines = [line.strip() for line in lines]
    # Join back and strip overall leading/trailing newlines
    return '\n'.join(normalized_lines).strip()

def extract_function_output(stdout: str) -> str:
    """Extract the function's return value from sandbox stdout.
    
    The sandbox may prepend class or function definitions (e.g.,
    "class Solution:" and "def foo(...):") before the actual result.
    This helper removes any lines that start with "class " or "def "
    (ignoring leading whitespace) and returns the last remaining line
    stripped of surrounding whitespace. If no such line exists, it returns
    an empty string.
    """
    if not stdout:
        return ""
    lines = [ln for ln in stdout.split('\n') if ln.strip()]
    # Filter out definition lines
    filtered = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('class ') or stripped.startswith('def '):
            continue
        filtered.append(line)
    return filtered[-1].strip() if filtered else ""

def _extract_function_name(code: str) -> Optional[str]:
    """Extracts the function name from the provided Python code."""
    match = re.search(r"def\s+(\w+)\s*\(", code)
    if match:
        return match.group(1)
    return None

def check_match(expected: Any, actual: Any) -> bool:
    """
    Check if actual matches expected using fuzzy matching rules:
    1. Exact match
    2. Floating point tolerance (1e-6)
    3. Case-insensitive string match
    4. Token-based match (ignore whitespace differences)
    """
    # 1. Exact match
    if expected == actual:
        return True

    # Normalize strings
    str_expected = str(expected).strip()
    str_actual = str(actual).strip()

    if str_expected == str_actual:
        return True

    # 2. Case-insensitive match
    if str_expected.lower() == str_actual.lower():
        return True

    # 3. Token-based match
    tokens_expected = str_expected.split()
    tokens_actual = str_actual.split()

    if len(tokens_expected) != len(tokens_actual):
        return False

    for t_exp, t_act in zip(tokens_expected, tokens_actual):
        # Exact token match
        if t_exp == t_act:
            continue
        
        # Case-insensitive token match
        if t_exp.lower() == t_act.lower():
            continue

        # Floating point match
        try:
            f_exp = float(t_exp)
            f_act = float(t_act)
            if abs(f_exp - f_act) < 1e-6:
                continue
        except ValueError:
            pass
            
        return False

    return True

class ResponseVerifier(Processor):
    def __init__(self, endpoint: str, concurrency: int = 8, language: str = "python"):
        super().__init__("ResponseVerifier")
        self.endpoint = endpoint
        self.concurrency = concurrency
        self.language = language
        set_endpoint(endpoint)
        
    def process(self, database: ReasoningDatabase, limit: int = 10000, offset: int = 0, retry_statuses: list = None, dryrun: bool = False, failure_log: str = None):
        """Process responses for verification.
        
        Args:
            database: Database instance
            limit: Maximum number of responses to verify
            offset: Number of responses to skip
            retry_statuses: List of statuses to retry (e.g., ['failed', 'error'])
                           If None, defaults to ['pending', 'error', None]
            dryrun: If True, run verification but do not update database
            failure_log: Path to file to log failures
        """
        # Default to pending and error if not specified
        if retry_statuses is None:
            retry_statuses = ['pending', 'error', None]
        
        logging.info(f"Fetching responses with statuses: {retry_statuses} (limit={limit}, offset={offset})...")
        if dryrun:
            logging.info("DRYRUN MODE: Database will NOT be updated.")

        # Use async generator for streaming
        # We need to run this within the asyncio loop
        asyncio.run(self._verify_batch(database, limit, offset, retry_statuses, dryrun=dryrun, failure_log=failure_log))

    async def _verify_batch(self, database: ReasoningDatabase, limit: int, offset: int, retry_statuses: list, dryrun: bool = False, failure_log: str = None):
        semaphore = asyncio.Semaphore(self.concurrency)
        
        # Statistics tracking
        stats = {
            'passed': 0,
            'failed': 0,
            'error': 0,
            'skipped': 0
        }
        stats_lock = asyncio.Lock()
        
        # Setup failure logging
        log_file_handle = None
        log_lock = asyncio.Lock()
        if failure_log:
            try:
                log_file_handle = open(failure_log, "w")
            except Exception as e:
                logging.error(f"Failed to open failure log {failure_log}: {e}")

        async def worker(row):
            async with semaphore:
                try:
                    # row contains both response info and problem info (test_cases)
                    update_data, status = await self._verify_single(database, row, row)
                    
                    # Log failure to file if enabled
                    if failure_log and log_file_handle:
                         if status == 'failed':
                            error_msg = update_data.get('verification_details', [{}])[0].get('error', 'Unknown error')
                            async with log_lock:
                                log_file_handle.write(f"FAIL {row['id']}: {error_msg}\n")
                                log_file_handle.flush()
                         elif status == 'error':
                            error_msg = update_data.get('verification_details', {}).get('error', 'Unknown error')
                            async with log_lock:
                                log_file_handle.write(f"ERROR {row['id']}: {error_msg}\n")
                                log_file_handle.flush()
                            
                    return update_data, status
                except Exception as e:
                    error_msg = str(e)
                    logging.error(f"Error verifying response {row['id']}: {error_msg}")
                    return None, 'error'

        # Streaming execution
        pending_tasks = set()
        batch_updates = []
        BATCH_SIZE = 50
        
        # Use tqdm for progress
        pbar = tqdm(desc="Verifying", total=limit if limit else None)
        
        async for row in database.get_responses_with_problems_async(retry_statuses, limit=limit, offset=offset, num_workers=4):
            task = asyncio.create_task(worker(row))
            pending_tasks.add(task)
            
            # Callback to remove from set is tricky with async loop, better to manage manually
            # or just prune finished tasks periodically
            
            # Flow control: don't let pending tasks grow indefinitely
            if len(pending_tasks) >= self.concurrency * 2:
                done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                
                # Process finished tasks
                for t in done:
                    try:
                        update_data, result_status = await t
                        if update_data:
                            batch_updates.append(update_data)
                            if dryrun and result_status == 'failed':
                                logging.info(f"DRYRUN FAIL {update_data['id']}: {update_data.get('verification_details', [{}])[0].get('error', 'Unknown error')}")
                        
                        # Update stats
                        if result_status in stats:
                            stats[result_status] += 1
                        
                        pbar.update(1)
                        total_done = sum(stats.values())
                        if total_done > 0:
                            pass_rate = (stats['passed'] / total_done) * 100
                            fail_rate = (stats['failed'] / total_done) * 100
                            error_rate = (stats['error'] / total_done) * 100
                            pbar.set_postfix({
                                'Pass': f"{stats['passed']} ({pass_rate:.1f}%)",
                                'Fail': f"{stats['failed']} ({fail_rate:.1f}%)",
                                'Err': f"{stats['error']} ({error_rate:.1f}%)"
                            })

                    except Exception as e:
                        logging.error(f"Task error: {e}")

                # Flush updates if needed
                if len(batch_updates) >= BATCH_SIZE:
                    if not dryrun:
                        database.update_responses_batch(batch_updates)
                    batch_updates = []

        # Wait for remaining tasks
        if pending_tasks:
            done, _ = await asyncio.wait(pending_tasks)
            for t in done:
                try:
                    update_data, result_status = await t
                    if update_data:
                        batch_updates.append(update_data)
                        if dryrun and result_status == 'failed':
                            logging.info(f"DRYRUN FAIL {update_data['id']}: {update_data.get('verification_details', [{}])[0].get('error', 'Unknown error')}")
                    
                    if result_status in stats:
                        stats[result_status] += 1
                    pbar.update(1)
                except Exception as e:
                    logging.error(f"Task error: {e}")
        
        pbar.close()
        
        # Final flush
        if batch_updates and not dryrun:
            database.update_responses_batch(batch_updates)
        elif batch_updates and dryrun:
            logging.info(f"DRYRUN: Would have updated {len(batch_updates)} records.")
        
        # Flush remaining updates
        if batch_updates and not dryrun:
            database.update_responses_batch(batch_updates)
        elif batch_updates and dryrun:
            logging.info(f"DRYRUN: Would have updated {len(batch_updates)} records.")
        
        # Log final statistics
        total = sum(stats.values())
        logging.info(f"Verification complete: {stats['passed']} passed, {stats['failed']} failed, "
                    f"{stats['error']} errors, {stats['skipped']} skipped (total: {total})")

    async def _verify_single(self, database: ReasoningDatabase, response_row: Any, problem: Any = None):
        problem_id = response_row['problem_id']
        
        if not problem:
            # Fallback if not provided (though batch fetch should handle it)
            problem = database.get_problem(problem_id)
        
        if not problem:
            logging.warning(f"Problem {problem_id} not found for response {response_row['id']}")
            return {'id': response_row['id'], 'verification_status': 'error', 'verification_details': {"error": "Problem not found"}}, 'error'

        code = response_row['extracted_code']
        if not code:
             # If no code, we can't verify. Mark as skipped or failed-no-code.
             # User said "only mark the reponse pass if all tests passed".
             # But if there is no code, it's not even a candidate for verification.
             # Let's mark it as 'skipped' to distinguish from execution errors.
             return {'id': response_row['id'], 'verification_status': 'skipped', 'verification_details': {"reason": "No extracted code"}}, 'skipped'

        test_cases_json = problem['test_cases']
        # Normalize test cases structure
        # The structure might vary (inputs/outputs lists vs list of dicts)
        # Based on previous inspection: {"inputs": [...], "outputs": [...]}
        
        # Handle potential double-encoding or string format
        if isinstance(test_cases_json, str):
            try:
                test_cases_json = json.loads(test_cases_json)
                # Check if it's still a string (double encoded)
                if isinstance(test_cases_json, str):
                    test_cases_json = json.loads(test_cases_json)
            except json.JSONDecodeError:
                return {'id': response_row['id'], 'verification_status': 'error', 'verification_details': {"error": "Failed to parse test_cases JSON"}}, 'error'

        test_cases = test_cases_json # Assign the potentially parsed object to test_cases

        inputs = []
        outputs = []
        fn_name = None
        
        if isinstance(test_cases, dict):
            fn_name = test_cases.get("fn_name")
            if "inputs" in test_cases and "outputs" in test_cases:
                inputs = test_cases["inputs"]
                outputs = test_cases["outputs"]

                # Additional validation and cleanup of inputs/outputs
                if inputs and outputs:
                    # Ensure inputs and outputs have the same length
                    min_len = min(len(inputs), len(outputs))
                    if len(inputs) != len(outputs):
                        logging.warning(f"Mismatched test case lengths for {response_row['id']}: inputs={len(inputs)}, outputs={len(outputs)}")
                        inputs = inputs[:min_len]
                        outputs = outputs[:min_len]
        
        if not inputs:
             return {'id': response_row['id'], 'verification_status': 'error', 'verification_details': {"error": "No test cases found or unrecognized format"}}, 'error'

        results = []
        all_passed = True
        
        for i, (inp, expected) in enumerate(zip(inputs, outputs)):
            # Prepare request
            request = None
            
            if fn_name:
                # Function-based (e.g., apps)
                # Construct driver code
                # inp is a list of arguments, e.g. [[1, 2, 3]] or [1, "a"]
                # expected is the return value

                # Handle the case where inp is wrapped in an extra list
                # Sometimes test cases have structure like [[[actual_args]]]
                if isinstance(inp, list) and len(inp) == 1 and isinstance(inp[0], list):
                    # Check if this looks like over-wrapped args
                    # If inp[0] contains empty lists at the end, it's likely malformed
                    if len(inp[0]) >= 2 and all(x == [] for x in inp[0][-2:]):
                        # Strip trailing empty lists and unwrap one level
                        inp = [x for x in inp[0] if x != []]
                    else:
                        inp = inp[0]

                # We need to be careful with json serialization of arguments
                try:
                    args_json = repr(inp)
                    expected_json = repr(expected)
                except Exception as e:
                    results.append({
                        "index": i,
                        "passed": False,
                        "error": f"Serialization error: {e}"
                    })
                    all_passed = False
                    continue

                if inside_solution_class(code, fn_name):
                    fn_name = f"Solution().{fn_name}"
                driver_code = f"""
{import_string}
import sys
import json

# Solution Code
{code}

# Test Driver
try:
    args = {args_json}
    expected = {expected_json}
    
    result = {fn_name}(*args)
    
    # Simple equality check. For float or complex types, might need tolerance.
    # For now, exact match.
    if result == expected:
        print("PASSED")
    elif isinstance(expected, list) and len(expected) == 1 and expected[0] == result:
        # Handle wrapped scalar case (e.g. expected=[0], result=0)
        print("PASSED")
    else:
        print(f"FAILED")
        print(f"FAILED: Expected {{expected}}, got {{result}}", file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f"RUNTIME ERROR")
    print(f"RUNTIME ERROR: {{e}}", file=sys.stderr)
    sys.exit(1)
"""
                request = RunCodeRequest(
                    code=driver_code,
                    stdin="", # No stdin for function calls usually
                    language=self.language,
                    compile_timeout=10.0,
                    run_timeout=10.0,
                    files={},
                    fetch_files=[]
                )
                
            else:
                # Stdio-based (e.g., codeforces)
                # inp is the stdin string
                # expected is the stdout string
                
                if not isinstance(inp, str):
                    # Try to convert to string if it's not (though it should be for stdio)
                    if isinstance(inp, list):
                        inp = "\n".join(inp)
                    else:
                        inp = str(inp)
                
                request = RunCodeRequest(
                    code=import_string + "\n" + code,
                    stdin=inp,
                    language=self.language,
                    compile_timeout=10.0,
                    run_timeout=10.0,
                    files={},
                    fetch_files=[]
                )
            
            # Run in thread to avoid blocking async loop
            # max_attempts=3 to handle transient sandbox errors.
            response = await asyncio.to_thread(run_code, request, max_attempts=3)
            
            run_result = response.run_result
            
            passed = False
            actual_stdout = ""
            error_msg = ""
            
            if run_result is not None:
                # Execution completed (successfully or with error)
                # Clean sandbox output to remove known messages
                raw_stdout = run_result.stdout if run_result.stdout else ""
                raw_stderr = run_result.stderr if run_result.stderr else ""
                actual_stdout = clean_sandbox_output(raw_stdout).strip()



# ... (inside ResponseVerifier class) ...

                if fn_name:
                    # For function based, compare the extracted result with expected.
                    # Clean and normalize the sandbox output first.
                    actual_result = extract_function_output(actual_stdout)
                    
                    # Use check_match for comparison
                    if (run_result.return_code == 0 and
                        (check_match(expected, actual_result) or "PASSED" in actual_stdout)):
                        passed = True
                    else:
                        passed = False
                        # Include stderr in error message for better debugging
                        stderr_info = f" | stderr: {raw_stderr[:100]}" if raw_stderr else ""
                        error_msg = f"Expected: {str(expected)[:100]}..., Got: {actual_result[:100]}...{stderr_info}"
                        
                        # Check for specific runtime errors in stdout/stderr
                        if "RUNTIME ERROR" in raw_stdout or run_result.return_code != 0:
                             error_msg = f"Runtime Error: {raw_stderr.strip()[:200] or raw_stdout.strip()[:200]}"
                else:
                    # For stdio, we compare stdout with expected
                    # Apply whitespace normalization to handle formatting inconsistencies
                    raw_expected = clean_sandbox_output(expected) if isinstance(expected, str) else str(expected)
                    
                    # Check if expected output is a string representation of a list
                    # e.g. "['50', '200']" -> should be treated as "50\n200"
                    if isinstance(raw_expected, str) and raw_expected.strip().startswith('[') and raw_expected.strip().endswith(']'):
                        try:
                            # Use ast.literal_eval to handle Python list syntax (single quotes)
                            parsed = ast.literal_eval(raw_expected)
                            if isinstance(parsed, list):
                                # Convert list elements to string and join with newlines
                                raw_expected = "\n".join(str(x) for x in parsed)
                        except (ValueError, SyntaxError):
                            # Not a valid list literal, treat as literal string
                            pass

                    expected_stdout = normalize_stdio_output(raw_expected)
                    actual_stdout_normalized = normalize_stdio_output(actual_stdout)
                    
                    if not isinstance(expected_stdout, str):
                        # Try to convert to string if it's not (though it should be for stdio)
                        if isinstance(expected_stdout, list):
                            expected_stdout = "\n".join(expected_stdout)
                        else:
                            expected_stdout = str(expected_stdout)

                    # Use check_match for comparison
                    if run_result.return_code == 0 and check_match(expected_stdout, actual_stdout_normalized):
                        passed = True
                    else:
                        passed = False
                        # Include stderr in error message for better debugging
                        stderr_info = f" | stderr: {raw_stderr[:100]}" if raw_stderr else ""
                        error_msg = f"Expected: {expected_stdout[:100]}..., Got: {actual_stdout_normalized[:100]}...{stderr_info}"
            else:
                # No run result, implies sandbox infrastructure failure
                passed = False
                error_msg = f"Sandbox error: {response.message or 'Unknown'}"

            if not passed:
                # Failure details are collected in results
                all_passed = False
            
            results.append({
                "index": i,
                "passed": passed,
                "expected": str(expected)[:100],
                "actual": actual_stdout[:100],
                "status": response.status.value if response.status else "unknown",
                "return_code": run_result.return_code if run_result else None,
                "error": error_msg[:300] if error_msg else "",
                "stderr": (run_result.stderr[:200] if run_result and run_result.stderr else "")
            })
            
        status = "passed" if all_passed else "failed"
        return {
            'id': response_row['id'], 
            'verification_status': status, 
            'verification_details': results
        }, status

    def dump_tasks(self, database: ReasoningDatabase, output_file: str, limit: int = 10000, offset: int = 0, retry_statuses: list = None):
        """Dump verification tasks to a JSONL file for offline execution."""
        if retry_statuses is None:
            retry_statuses = ['pending', 'error', None]
        
        logging.info(f"Fetching responses for dump with statuses: {retry_statuses} (limit={limit}, offset={offset})...")
        
        async def _dump():
            count = 0
            with open(output_file, 'w') as f:
                pbar = tqdm(desc="Dumping tasks", total=limit if limit else None)
                async for row in database.get_responses_with_problems_async(retry_statuses, limit=limit, offset=offset, num_workers=4):
                    code = row['extracted_code']
                    if not code:
                        pbar.update(1)
                        continue

                    test_cases_json = row['test_cases']
                    # Normalize test cases
                    if isinstance(test_cases_json, str):
                        try:
                            test_cases_json = json.loads(test_cases_json)
                            if isinstance(test_cases_json, str):
                                test_cases_json = json.loads(test_cases_json)
                        except json.JSONDecodeError:
                            pbar.update(1)
                            continue
                    
                    task = {
                        "id": row['id'],
                        "problem_id": row['problem_id'],
                        "code": code,
                        "language": self.language,
                        "test_cases": test_cases_json,
                        "import_string": import_string
                    }
                    
                    f.write(json.dumps(task) + "\n")
                    count += 1
                    pbar.update(1)
                pbar.close()
            return count

        count = asyncio.run(_dump())
        logging.info(f"Dumped {count} tasks to {output_file}")

    def ingest_results(self, database: ReasoningDatabase, results_file: str, dryrun: bool = False):
        """Ingest verification results from a JSONL file and update the database."""
        if not os.path.exists(results_file):
            logging.error(f"Results file {results_file} not found.")
            return

        logging.info(f"Ingesting results from {results_file} (dryrun={dryrun})...")
        
        batch_updates = []
        BATCH_SIZE = 100
        count = 0
        
        with open(results_file, 'r') as f:
            for line in tqdm(f, desc="Ingesting results"):
                try:
                    result = json.loads(line)
                    response_id = result.get('id')
                    status = result.get('verification_status')
                    details = result.get('verification_details')
                    
                    if not response_id or not status:
                        continue
                        
                    update_data = {
                        'id': response_id,
                        'verification_status': status,
                        'verification_details': details
                    }
                    
                    batch_updates.append(update_data)
                    count += 1
                    
                    if len(batch_updates) >= BATCH_SIZE:
                        if not dryrun:
                            database.update_responses_batch(batch_updates)
                        batch_updates = []
                        
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logging.error(f"Error processing line: {e}")
        
        if batch_updates:
            if not dryrun:
                database.update_responses_batch(batch_updates)
            
        logging.info(f"Ingested {count} results (dryrun={dryrun}).")
