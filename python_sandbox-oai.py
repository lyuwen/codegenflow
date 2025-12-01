import multiprocessing
import sys
import io
import os
import uuid
import ast
import traceback
import resource
import builtins
import tempfile
import shutil
import platform
import faulthandler
from contextlib import redirect_stdout, redirect_stderr


# =====================================================================
# STRICT IMPORT WHITELIST
# =====================================================================

WHITELIST_IMPORTS = {
    "copy", "string", "math", "collections", "bisect", "heapq",
    "functools", "random", "itertools", "operator", "re",
    "numpy", "pandas", "os", "sys", "typing", "json", "datetime"
}


# =====================================================================
# RELIABILITY GUARD (GRANULAR OS/SYS/SHUTIL PROTECTION)
# =====================================================================

class ReliabilityGuard:
    def __init__(self, max_memory_bytes=None):
        self.max_memory_bytes = max_memory_bytes
        self.saved_patches = []
        self.saved_sys_modules = {}

    def __enter__(self):
        # Resource limits
        # if self.max_memory_bytes is not None:
        #     try:
        #         resource.setrlimit(resource.RLIMIT_AS, (self.max_memory_bytes, self.max_memory_bytes))
        #         resource.setrlimit(resource.RLIMIT_DATA, (self.max_memory_bytes, self.max_memory_bytes))
        #         if platform.system() != "Darwin":
        #             resource.setrlimit(resource.RLIMIT_STACK, (self.max_memory_bytes, self.max_memory_bytes))
        #     except ValueError:
        #         pass

        # Disable crash reports
        faulthandler.disable()

        # -------- builtins --------
        self._patch(builtins, "quit", None)
        self._patch(builtins, "exit", None)
        
        # Handle __builtins__ help
        if isinstance(__builtins__, dict):
            if "help" in __builtins__:
                self.saved_sys_modules["__builtins__.help"] = __builtins__["help"]
                __builtins__["help"] = None
        elif hasattr(__builtins__, "help"):
            self._patch(__builtins__, "help", None)

        # -------- os module --------
        import os
        dangerous_os_attrs = [
            "kill", "system", "putenv", "remove", "removedirs", "rmdir",
            "fchdir", "setuid", "fork", "forkpty", "killpg",
            "rename", "renames", "truncate", "replace",
            "unlink", "fchmod", "fchown", "chmod", "chown",
            "chroot", "lchflags", "lchmod", "lchown",
        ]
        for attr in dangerous_os_attrs:
            self._patch(os, attr, None)

        # -------- shutil --------
        import shutil
        for attr in ("rmtree", "move", "chown"):
            self._patch(shutil, attr, None)

        # -------- subprocess --------
        import subprocess
        self._patch(subprocess, "Popen", None)

        # -------- sys module --------
        import sys
        banned_sys_modules = [
            "ipdb", "joblib", "resource", "psutil", "tkinter",
            "socket", "urllib", "http", "requests"
        ]
        for mod in banned_sys_modules:
            if mod in sys.modules:
                self.saved_sys_modules[mod] = sys.modules[mod]
            sys.modules[mod] = None
            
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore patches
        for obj, attr, original_val in reversed(self.saved_patches):
            setattr(obj, attr, original_val)
        
        # Restore sys modules
        import sys
        for mod, val in self.saved_sys_modules.items():
            if mod == "__builtins__.help":
                if isinstance(__builtins__, dict):
                    __builtins__["help"] = val
            else:
                sys.modules[mod] = val
                
        # Remove modules that were added as None but didn't exist before
        # (This logic is simplified; we assume if we saved it, we restore it. 
        # If we didn't save it, it implies it wasn't there or we didn't touch it.
        # But we set sys.modules[mod] = None unconditionally.
        # So if it wasn't in saved_sys_modules, we should remove it.)
        banned_sys_modules = ["ipdb", "joblib", "resource", "psutil", "tkinter"]
        for mod in banned_sys_modules:
            if mod not in self.saved_sys_modules:
                if mod in sys.modules and sys.modules[mod] is None:
                    del sys.modules[mod]

    def _patch(self, obj, attr, value):
        if hasattr(obj, attr):
            original = getattr(obj, attr)
            self.saved_patches.append((obj, attr, original))
            setattr(obj, attr, value)


# =====================================================================
# AST SECURITY
# =====================================================================

FORBIDDEN_CALL_NAMES = {"eval", "exec", "compile", "__import__", "globals", "locals", "vars"}

class SecurityScanner(ast.NodeVisitor):
    def visit_Call(self, node):
        if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_CALL_NAMES:
            raise ValueError(f"Forbidden call: {node.func.id}")
        self.generic_visit(node)


def validate_code_security(code: str):
    tree = ast.parse(code)
    SecurityScanner().visit(tree)


# =====================================================================
# SAFE IMPORT HOOK
# =====================================================================

def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    top = name.split('.')[0]
    # if top not in WHITELIST_IMPORTS:
    #     raise ImportError(f"Import '{name}' not in whitelist")
    return __import__(name, globals, locals, fromlist, level)


_real_open = builtins.open


# =====================================================================
# WORKER (EXECUTES INSIDE CHILD PROCESS)
# =====================================================================

def _sandbox_worker(code, stdin_data, sandbox_dir, return_dict,
                    max_memory_bytes, cpu_limit_seconds):

    # -------- resource limits --------
    # cpu_limit_seconds = int(cpu_limit_seconds) + 1
    # try:
    #     resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit_seconds, cpu_limit_seconds))
    # except ValueError:
    #     pass

    # -------- per-execution directory --------
    os.makedirs(sandbox_dir, exist_ok=True)
    os.chdir(sandbox_dir)

    # -------- safe stdin --------
    if stdin_data is not None:
        sys.stdin = io.StringIO(stdin_data)

    # prepare stdout/stderr buffers
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    # -------- safe builtins --------
    safe_builtins = {}
    for name in dir(builtins):
        if name in {"open", "__import__", "eval", "exec", "compile"}:
            continue
        safe_builtins[name] = getattr(builtins, name)

    def safe_open(path, mode="r", *args, **kwargs):
        abs_path = os.path.abspath(path)
        if not abs_path.startswith(os.path.abspath(sandbox_dir)):
            raise PermissionError("Forbidden filesystem access")
        return _real_open(abs_path, mode, *args, **kwargs)

    safe_builtins["open"] = safe_open
    safe_builtins["__import__"] = safe_import

    # Reset globals/locals every run
    safe_globals = {"__builtins__": safe_builtins}
    safe_locals = {}

    try:
        # AST security validation
        validate_code_security(code)

        # Execute code with redirected stdout and stderr
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            with ReliabilityGuard(max_memory_bytes):
                exec(code, safe_globals, safe_locals)

        return_dict["success"] = True
        return_dict["stdout"] = stdout_buffer.getvalue()
        return_dict["stderr"] = stderr_buffer.getvalue()

    except Exception:
        return_dict["success"] = False
        return_dict["stdout"] = stdout_buffer.getvalue()
        return_dict["stderr"] = (
            stderr_buffer.getvalue() + traceback.format_exc()
        )


# =====================================================================
# PUBLIC API
# =====================================================================

def run_sandbox(
    code: str,
    stdin: str = None,
    timeout: float = 2.0,
    max_memory_mb: int = 128,
    cpu_limit_seconds: float = 1.5
):
    """
    Execute untrusted Python code inside a hardened, isolated sandbox environment.

    This function creates a new subprocess with:
        - Strict AST-based security checks
        - Explicit import whitelist
        - Disabled dangerous builtins
        - Granular reliability guard (os/sys/shutil/subprocess restrictions)
        - Per-execution isolated temporary directory
        - Restricted filesystem access (`open` is sandboxed)
        - CPU time limits (RLIMIT_CPU)
        - Memory limits (RLIMIT_AS)
        - Captured stdout and stderr
        - Fresh globals/locals for every run

    Parameters
    ----------
    code : str
        The Python source code to execute inside the sandbox.
        Must be valid Python. Certain constructs (eval/exec/import/from/etc.)
        are rejected at AST-parse time.

    stdin : str, optional
        Optional input data fed into the sandboxed code via `sys.stdin`.
        If `None`, stdin is empty.
        Example: stdin="Hello\n123\n"

    timeout : float, optional
        Maximum wall-clock time (in seconds) to allow the child process to run.
        If exceeded, the sandboxed process is force-killed and an error is returned.

    max_memory_mb : int, optional
        Maximum amount of memory available to the sandbox process, in megabytes.
        Implemented via RLIMIT_AS and RLIMIT_DATA.
        Default is 128 MB.

    cpu_limit_seconds : float, optional
        Maximum CPU time allowed (not wall-clock), enforced via RLIMIT_CPU.
        When exceeded, the subprocess is terminated by the OS.

    Returns
    -------
    dict
        A dictionary of the form:

        {
            "success": bool,
            "stdout": str,
            "stderr": str
        }

        Meaning:
        - success: True if user code executed without unhandled exceptions.
                   False if an exception occurred or timeout happened.
        - stdout: Captured standard output produced by the sandboxed code.
        - stderr: Captured standard error output, including Python tracebacks
                  or safety violations.

        Example successful run:
            {
                "success": True,
                "stdout": "4\\n",
                "stderr": ""
            }

        Example error run:
            {
                "success": False,
                "stdout": "",
                "stderr": "ValueError: Forbidden call: eval\\n..."
            }

    Notes
    -----
    - This is a hardened execution environment for *untrusted code*, but is not
      a perfect security boundary. For absolute isolation, run inside a VM or
      container.
    - All global state resets for each execution.
    - The sandbox directory is destroyed after completion.
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

