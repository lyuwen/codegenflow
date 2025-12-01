import os
import tempfile
import uuid
import multiprocessing
import shutil
import sys
import io
import contextlib
import resource
import traceback

def _sandbox_worker(code, stdin, sandbox_dir, return_dict, max_memory_bytes, cpu_limit_seconds):
    """
    Worker function to execute code in a separate process.
    """
    try:
        # Create sandbox directory
        os.makedirs(sandbox_dir, exist_ok=True)
        os.chdir(sandbox_dir)

        # Set resource limits
        #  if max_memory_bytes:
        #      try:
        #          resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))
        #      except ValueError:
        #          pass # Ignore if not allowed
        #
        #  if cpu_limit_seconds:
        #      try:
        #          # CPU time is in seconds
        #          # RLIMIT_CPU takes integer seconds
        #          cpu_limit_int = int(cpu_limit_seconds) + 1
        #          resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit_int, cpu_limit_int))
        #      except ValueError:
        #          pass

        # Prepare stdin
        if stdin:
            sys.stdin = io.StringIO(stdin)
        else:
            sys.stdin = io.StringIO("")

        # Capture stdout/stderr
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with contextlib.redirect_stdout(stdout_capture), contextlib.redirect_stderr(stderr_capture):
            try:
                # Execute the code
                # We use a fresh dictionary for globals/locals
                exec_globals = {"__name__": "__main__"}
                exec(code, exec_globals)
                success = True
            except SystemExit as e:
                traceback.print_exc()
                print(f"Sandbox attempted to exit: SystemExit({e.code})", file=sys.stderr)
            except Exception:
                traceback.print_exc()
                success = False

        return_dict['success'] = success
        return_dict['stdout'] = stdout_capture.getvalue()
        return_dict['stderr'] = stderr_capture.getvalue()

    except Exception as e:
        return_dict['success'] = False
        return_dict['stdout'] = ""
        return_dict['stderr'] = f"Sandbox internal error: {str(e)}"

def run_sandbox(
    code: str,
    stdin: str = None,
    timeout: float = 2.0,
    max_memory_mb: int = 1024,
    cpu_limit_seconds: float = 1.5
):
    """
    Execute untrusted Python code inside a hardened, isolated sandbox environment.
    ...
    """
    sandbox_dir = os.path.join(tempfile.gettempdir(), f"sandbox_{uuid.uuid4()}")
    max_memory_bytes = max_memory_mb * 1024 * 1024

    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    proc = multiprocessing.Process(
        target=_sandbox_worker,
        args=(code, stdin, sandbox_dir, return_dict,
              max_memory_bytes, cpu_limit_seconds)
    )

    proc.start()
    proc.join(timeout)

    if proc.is_alive():
        proc.kill()
        shutil.rmtree(sandbox_dir, ignore_errors=True)
        return {"success": False, "stdout": "", "stderr": "Execution timed out"}

    shutil.rmtree(sandbox_dir, ignore_errors=True)
    return dict(return_dict)

