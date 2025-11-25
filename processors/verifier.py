import json
import logging
import asyncio
import argparse
import sys
from typing import Dict, Any, List, Optional
from tqdm import tqdm
from database import ReasoningDatabase
from processors.base import Processor
from sandbox_fusion import RunCodeRequest, RunCodeResponse, run_code, set_endpoint
from sandbox_fusion.models import RunStatus
import re

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

class ResponseVerifier(Processor):
    def __init__(self, endpoint: str, concurrency: int = 8, language: str = "python"):
        super().__init__("ResponseVerifier")
        self.endpoint = endpoint
        self.concurrency = concurrency
        self.language = language
        set_endpoint(endpoint)
        
    def process(self, database: ReasoningDatabase, limit: int = 10000, retry_statuses: list = None):
        """Process responses for verification.
        
        Args:
            database: Database instance
            limit: Maximum number of responses to verify
            retry_statuses: List of statuses to retry (e.g., ['failed', 'error'])
                           If None, defaults to ['pending', 'error', None]
        """
        # Default to pending and error if not specified
        if retry_statuses is None:
            retry_statuses = ['pending', 'error', None]
        
        logging.info(f"Fetching responses with statuses: {retry_statuses} (limit={limit})...")
        # Fetch responses with specified statuses
        responses = list(database.get_responses_by_status(retry_statuses, limit=limit)) 
        
        if not responses:
            logging.info("No unverified responses found.")
            return

        logging.info(f"Found {len(responses)} responses to verify.")
        
        asyncio.run(self._verify_batch(database, responses))

    async def _verify_batch(self, database: ReasoningDatabase, responses: List[Any]):
        semaphore = asyncio.Semaphore(self.concurrency)
        
        # Statistics tracking
        stats = {
            'passed': 0,
            'failed': 0,
            'error': 0,
            'skipped': 0
        }
        stats_lock = asyncio.Lock()
        
        async def worker(row):
            async with semaphore:
                result_status = 'error'  # Default
                try:
                    result_status = await self._verify_single(database, row)
                except Exception as e:
                    error_msg = str(e)
                    logging.error(f"Error verifying response {row['id']}: {error_msg}")
                    
                    # Categorize specific error types
                    if "413" in error_msg or "Request Entity Too Large" in error_msg:
                        # Request too large for sandbox - mark as skipped
                        database.update_response(
                            row['id'], 
                            verification_status='skipped', 
                            verification_details=json.dumps({"reason": "request_too_large", "error": error_msg[:500]})
                        )
                        result_status = 'skipped'
                    elif "database is locked" in error_msg:
                        # Database locking issue - leave as pending to retry later
                        logging.warning(f"Database locked for {row['id']}, will retry later")
                        result_status = 'pending'
                    else:
                        # Other errors
                        database.update_response(
                            row['id'], 
                            verification_status='error', 
                            verification_details=json.dumps({"error": error_msg[:500]})
                        )
                        result_status = 'error'
                
                # Update statistics
                async with stats_lock:
                    if result_status in stats:
                        stats[result_status] += 1

        tasks = [asyncio.create_task(worker(row)) for row in responses]
        
        # Use tqdm with custom formatting to show live statistics
        with tqdm(total=len(tasks), desc="Verifying") as pbar:
            for f in asyncio.as_completed(tasks):
                await f
                # Update progress bar with current statistics
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
                pbar.update(1)
        
        # Log final statistics
        total = sum(stats.values())
        logging.info(f"Verification complete: {stats['passed']} passed, {stats['failed']} failed, "
                    f"{stats['error']} errors, {stats['skipped']} skipped (total: {total})")

    async def _verify_single(self, database: ReasoningDatabase, response_row: Any):
        problem_id = response_row['problem_id']
        problem = database.get_problem(problem_id)
        
        if not problem:
            logging.warning(f"Problem {problem_id} not found for response {response_row['id']}")
            database.update_response(response_row['id'], verification_status='error', verification_details=json.dumps({"error": "Problem not found"}))
            return 'error'

        code = response_row['extracted_code']
        if not code:
             # If no code, we can't verify. Mark as skipped or failed-no-code.
             # User said "only mark the reponse pass if all tests passed".
             # But if there is no code, it's not even a candidate for verification.
             # Let's mark it as 'skipped' to distinguish from execution errors.
             database.update_response(response_row['id'], verification_status='skipped', verification_details=json.dumps({"reason": "No extracted code"}))
             return 'skipped'

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
                database.update_response(response_row['id'], verification_status='error', verification_details=json.dumps({"error": "Failed to parse test_cases JSON"}))
                return 'error'

        test_cases = test_cases_json # Assign the potentially parsed object to test_cases

        inputs = []
        outputs = []
        fn_name = None
        
        if isinstance(test_cases, dict):
            fn_name = test_cases.get("fn_name")
            if "inputs" in test_cases and "outputs" in test_cases:
                inputs = test_cases["inputs"]
                outputs = test_cases["outputs"]
        
        if not inputs:
             database.update_response(response_row['id'], verification_status='error', verification_details=json.dumps({"error": "No test cases found or unrecognized format"}))
             return 'error'

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
                
                # We need to be careful with json serialization of arguments
                try:
                    # print(f"{inp=!r}")
                    # print(f"{expected=!r}")
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
            response = await asyncio.to_thread(run_code, request, max_attempts=3)
            
            run_result = response.run_result
            
            passed = False
            actual_stdout = ""
            error_msg = ""
            
            if response.status == RunStatus.Success and run_result is not None:
                # Clean sandbox output to remove known messages
                raw_stdout = run_result.stdout if run_result.stdout else ""
                actual_stdout = clean_sandbox_output(raw_stdout).strip()
                
                if fn_name:
                    # For function based, compare the extracted result with expected.
                    # Clean and normalize the sandbox output first.
                    actual_result = extract_function_output(actual_stdout)
                    expected_norm = normalize_stdio_output(str(expected))
                    if (run_result.return_code == 0 and
                        (actual_result == expected_norm or "PASSED" in actual_stdout)):
                        passed = True
                    else:
                        passed = False
                        error_msg = f"Expected: {expected_norm[:100]}..., Got: {actual_result[:100]}..."
                else:
                    # For stdio, we compare stdout with expected
                    # Apply whitespace normalization to handle formatting inconsistencies
                    raw_expected = clean_sandbox_output(expected) if isinstance(expected, str) else str(expected)
                    expected_stdout = normalize_stdio_output(raw_expected)
                    actual_stdout_normalized = normalize_stdio_output(actual_stdout)
                    if not isinstance(expected_stdout, str):
                        # Try to convert to string if it's not (though it should be for stdio)
                        if isinstance(expected_stdout, list):
                            expected_stdout = "\n".join(expected_stdout)
                        else:
                            expected_stdout = str(expected_stdout)
                    
                    if run_result.return_code == 0 and actual_stdout_normalized == expected_stdout:
                        passed = True
                    else:
                        passed = False
                        error_msg = f"Expected: {expected_stdout[:100]}..., Got: {actual_stdout_normalized[:100]}..."
            else:
                passed = False
                error_msg = response.message or (run_result.stderr if run_result else "Unknown error")

            if not passed:
                # Store failure details for debugging
                database.update_response(response_row['id'], verification_status='failed', verification_details=json.dumps({'error': error_msg}))
                all_passed = False
            
            results.append({
                "index": i,
                "passed": passed,
                "expected": str(expected)[:100],
                "actual": actual_stdout[:100],
                "status": response.status.value if response.status else "unknown",
                "return_code": run_result.return_code if run_result else None,
                "error": error_msg[:200]
            })
            
        status = "passed" if all_passed else "failed"
        database.update_response(
            response_row['id'], 
            verification_status=status, 
            verification_details=json.dumps(results)
        )
        return status
