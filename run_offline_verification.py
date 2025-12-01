import json
import argparse
import logging
import sys
import os
import ast
import re
from tqdm import tqdm
from python_sandbox import run_sandbox
sys.set_int_max_str_digits(100000)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def check_match(expected, actual):
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

def normalize_stdio_output(output: str) -> str:
    """Normalize stdio output by removing leading/trailing whitespace from each line."""
    if not output:
        return output
    lines = output.split('\n')
    normalized_lines = [line.strip() for line in lines]
    return '\n'.join(normalized_lines).strip()

def extract_function_output(stdout: str) -> str:
    """Extract the function's return value from sandbox stdout."""
    if not stdout:
        return ""
    lines = [ln for ln in stdout.split('\n') if ln.strip()]
    filtered = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('class ') or stripped.startswith('def '):
            continue
        filtered.append(line)
    return filtered[-1].strip() if filtered else ""

def inside_solution_class(code: str, fn_name: str) -> bool:
    pattern = rf"class\s+Solution\s*:\s*(?:.|\n)*?def\s+{re.escape(fn_name)}\s*\("
    return re.search(pattern, code) is not None

def process_task(task):
    code = task['code']
    test_cases = task['test_cases']
    import_string = task.get('import_string', '')
    
    inputs = []
    outputs = []
    fn_name = None
    
    if isinstance(test_cases, dict):
        fn_name = test_cases.get("fn_name")
        if "inputs" in test_cases and "outputs" in test_cases:
            inputs = test_cases["inputs"]
            outputs = test_cases["outputs"]
            
            # Ensure inputs and outputs have the same length
            min_len = min(len(inputs), len(outputs))
            inputs = inputs[:min_len]
            outputs = outputs[:min_len]

    if not inputs:
        return {
            "id": task['id'],
            "verification_status": "error",
            "verification_details": {"error": "No test cases found"}
        }

    results = []
    all_passed = True
    
    for i, (inp, expected) in enumerate(zip(inputs, outputs)):
        driver_code = ""
        stdin_input = None
        
        if fn_name:
            # Function-based
            if isinstance(inp, list) and len(inp) == 1 and isinstance(inp[0], list):
                if len(inp[0]) >= 2 and all(x == [] for x in inp[0][-2:]):
                    inp = [x for x in inp[0] if x != []]
                else:
                    inp = inp[0]

            try:
                args_json = repr(inp)
                expected_json = repr(expected)
            except Exception as e:
                results.append({"index": i, "passed": False, "error": f"Serialization error: {e}"})
                all_passed = False
                continue

            call_fn_name = fn_name
            if inside_solution_class(code, fn_name):
                call_fn_name = f"Solution().{fn_name}"
                
            driver_code = f"""
{import_string}
import sys
import json
sys.set_int_max_str_digits(100000)

# Solution Code
{code}

# Test Driver
try:
    args = {args_json}
    expected = {expected_json}
    
    result = {call_fn_name}(*args)
    
    if result == expected:
        print("PASSED")
    elif isinstance(expected, list) and len(expected) == 1 and expected[0] == result:
        print("PASSED")
    else:
        print(f"FAILED")
        print(f"FAILED: Expected {{expected}}, got {{result}}", file=sys.stderr)
        # sys.exit(1)
except Exception as e:
    print(f"RUNTIME ERROR")
    print(f"RUNTIME ERROR: {{e}}", file=sys.stderr)
    # sys.exit(1)
"""
        else:
            # Stdio-based
            if not isinstance(inp, str):
                if isinstance(inp, list):
                    inp = "\n".join(inp)
                else:
                    inp = str(inp)
            
            stdin_input = inp
            driver_code = import_string + "\nsys.set_int_max_str_digits(100000)\n" + "\n" + code

        # Run sandbox
        result = run_sandbox(
            code=driver_code,
            stdin=stdin_input,
            timeout=2.0,
            max_memory_mb=128
        )
        
        passed = False
        error_msg = ""
        actual_stdout = ""
        
        if result['success']:
            actual_stdout = result['stdout']
            
            if fn_name:
                actual_result = extract_function_output(actual_stdout)
                if check_match(expected, actual_result) or "PASSED" in actual_stdout:
                    passed = True
                else:
                    passed = False
                    error_msg = f"Expected: {str(expected)[:100]}..., Got: {actual_result[:100]}..."
            else:
                raw_expected = expected
                if isinstance(raw_expected, str) and raw_expected.strip().startswith('[') and raw_expected.strip().endswith(']'):
                    try:
                        parsed = ast.literal_eval(raw_expected)
                        if isinstance(parsed, list):
                            raw_expected = "\n".join(str(x) for x in parsed)
                    except:
                        pass
                
                expected_stdout = normalize_stdio_output(str(raw_expected))
                actual_stdout_normalized = normalize_stdio_output(actual_stdout)
                
                if check_match(expected_stdout, actual_stdout_normalized):
                    passed = True
                else:
                    passed = False
                    error_msg = f"Expected: {expected_stdout[:100]}..., Got: {actual_stdout_normalized[:100]}..."
        else:
            passed = False
            error_msg = f"Runtime Error: {result['stderr'][:200]}"

        if not passed:
            all_passed = False
            
        results.append({
            "index": i,
            "passed": passed,
            "expected": str(expected)[:100],
            "actual": actual_stdout[:100],
            "error": error_msg,
            "stderr": result['stderr'][:2000]
        })

    status = "passed" if all_passed else "failed"
    return {
        "id": task['id'],
        "verification_status": status,
        "verification_details": results
    }

def main():
    parser = argparse.ArgumentParser(description="Run offline verification tasks")
    parser.add_argument("--tasks", required=True, help="Path to tasks JSONL file")
    parser.add_argument("--rank", type=int, default=0, help="Worker rank (0-indexed)")
    parser.add_argument("--world-size", type=int, default=1, help="Total number of workers")
    parser.add_argument("--total-lines", type=int, default=None, help="Total number of lines")
    args = parser.parse_args()

    if args.rank < 0 or args.rank >= args.world_size:
        print(f"Error: Rank {args.rank} must be between 0 and {args.world_size - 1}")
        return

    output_file = f"results-{args.rank:06d}-{args.world_size:06d}.jsonl"
    
    # Count total lines for progress bar (optional, but good for UX)
    if args.total_lines:
        total_lines = args.total_lines
    else:
        total_lines = 0
        if os.path.exists(args.tasks):
            # Quick line count
            with open(args.tasks, 'rb') as f:
                for _ in tqdm(f, desc="Count lines"):
                    total_lines += 1
    
    estimated_my_tasks = total_lines // args.world_size
    logging.info(f"Worker {args.rank}/{args.world_size} starting. Total tasks: {total_lines}. Estimated my tasks: {estimated_my_tasks}")
    
    with open(args.tasks, 'r') as f_in, open(output_file, 'w') as f_out:
        # Create progress bar
        pbar = tqdm(total=estimated_my_tasks, desc=f"Worker {args.rank}")
        
        for i, line in enumerate(f_in):
            # Check if this task belongs to this worker
            if i % args.world_size == args.rank:
                try:
                    task = json.loads(line)
                    result = process_task(task)
                    f_out.write(json.dumps(result) + "\n")
                    f_out.flush()
                    pbar.update(1)
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logging.error(f"Error processing line {i}: {e}")
        
        pbar.close()

    logging.info(f"Worker {args.rank} finished. Results written to {output_file}")

if __name__ == "__main__":
    main()
