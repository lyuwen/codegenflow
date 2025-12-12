import logging
import json
import zlib
import re
import multiprocessing
import traceback
from typing import List, Dict, Any, Tuple
from sqlalchemy import select, exists
from tqdm import tqdm
from database import ReasoningDatabase

logger = logging.getLogger(__name__)

# --- Logic extracted from analyze_paragraphs.py ---

def calculate_metrics(text):
    if not text:
        return 0.0, 1.0, 0
    
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if not lines:
        return 0.0, 1.0, 0
        
    total_lines = len(lines)
    unique_lines = len(set(lines))
    lrr = total_lines / unique_lines if unique_lines > 0 else 0.0
    
    original_size = len(text.encode('utf-8'))
    if original_size == 0:
        return 0.0, 1.0, 0
    compressed_size = len(zlib.compress(text.encode('utf-8')))
    cr = compressed_size / original_size
    
    return lrr, cr, total_lines

def check_heuristics(text, lines):
    if not lines:
        return 0, False
    
    max_line_len = max(len(line) for line in lines)
    
    token_bad = False
    if len(text) > 5000:
        tokens = text.replace(',', ' ').split()
        if len(tokens) > 1000:
            from collections import Counter
            counts = Counter(tokens)
            most_common = counts.most_common(1)
            if most_common and most_common[0][1] / len(tokens) > 0.4:
                token_bad = True
    
    return max_line_len, token_bad

def check_language(text):
    cjk_count = 0
    total_chars = len(text)
    if total_chars == 0:
        return False, []
    
    for char in text:
        if '\u4e00' <= char <= '\u9fff':
            cjk_count += 1
    
    reasons = []
    if cjk_count > 0:
        if "极" in text or "極" in text:
            for target_char in ["极", "極"]:
                if target_char in text:
                    idx = text.find(target_char)
                    if text.count(target_char) > 1:
                        reasons.append(f"Multiple '{target_char}' characters found")
                    else:
                        is_anomaly = False
                        if idx > 0:
                            prev_char = text[idx-1]
                            if not ('\u4e00' <= prev_char <= '\u9fff'):
                                 is_anomaly = True
                        if idx < len(text) - 1:
                            next_char = text[idx+1]
                            if not ('\u4e00' <= next_char <= '\u9fff'):
                                 is_anomaly = True
                        
                        if is_anomaly:
                             reasons.append(f"Anomaly: '{target_char}' surrounded by non-CJK")
        
        if not reasons and cjk_count > 5:
             reasons.append(f"Contains {cjk_count} CJK chars")
             
    return len(reasons) > 0, reasons

def detect_reasoning_flaws(text):
    backtracking_keywords = [
        "But wait", "Actually", "Hold on", "Let me re-read", "Let me check",
        "correction", "mistake", "incorrect", "Wait,", "However, note that",
        "On second thought", "Alternatively, we can", "Let's double check",
        "re-evaluating", "My previous assumption"
    ]
    uncertainty_keywords = [
        "I am confused", "Is it possible", "not sure if", "might be invalid",
        "Could it be that", "I don't understand", "What if", "unclear",
        "ambiguous", "Do I need to", "assuming that"
    ]
    
    backtracking_count = 0
    uncertainty_count = 0
    text_lower = text.lower()
    
    for kw in backtracking_keywords:
        backtracking_count += text_lower.count(kw.lower())
    for kw in uncertainty_keywords:
        uncertainty_count += text_lower.count(kw.lower())
        
    return backtracking_count, uncertainty_count

def get_max_consecutive_repetition(tokens, n):
    if not tokens:
        return 0, None
    if len(tokens) < n:
        return 1, None
    
    L = len(tokens)
    runs = [1] * L
    global_max = 1
    max_ngram = None
    
    for i in range(n, L - n + 1):
        if tokens[i:i+n] == tokens[i-n:i]:
            runs[i] = runs[i-n] + 1
            if runs[i] > global_max:
                global_max = runs[i]
                max_ngram = tokens[i:i+n]
    
    return global_max, max_ngram


def process_item_worker(item):
    try:
        response_id = item['id']
        reasoning_content = item.get('reasoning_trace') or ""
        problem_content = item.get('problem_content') or {}
        
        if isinstance(problem_content, str):
            try:
                problem_content = json.loads(problem_content)
            except:
                problem_content = {}
        
        problem_text = problem_content.get('problem', '') if isinstance(problem_content, dict) else ''

        lrr, cr, total_lines = calculate_metrics(reasoning_content)
        lines_list = [l.strip() for l in reasoning_content.split('\n') if l.strip()]
        max_line_len, token_bad = check_heuristics(reasoning_content, lines_list)
        
        is_safe_cjk = False
        for char in problem_text:
            if '\u4e00' <= char <= '\u9fff':
                is_safe_cjk = True
                break
        
        lang_bad = False
        lang_reasons = []
        if not is_safe_cjk:
            lang_bad, lang_reasons = check_language(reasoning_content)
            
        backtracking_count, uncertainty_count = detect_reasoning_flaws(reasoning_content)
        
        high_paragraph_count = False
        sequential_paragraph_repeat = False
        intra_paragraph_repetition = False
        high_ngram_repetition = {}
        
        if reasoning_content:
            paragraphs = reasoning_content.split('\n\n')
            non_empty_paragraphs = [p for p in paragraphs if p.strip()]
            count = len(non_empty_paragraphs)
            
            if count > 1600:
                high_paragraph_count = True
                
            prompt_content = problem_text

            if count > 5:
                repeats = 0
                last_p = None
                for p in non_empty_paragraphs:
                    if p == last_p:
                        repeats += 1
                    else:
                        repeats = 0
                    last_p = p
                    if repeats >= 5:
                        if prompt_content and p in prompt_content:
                            continue
                        sequential_paragraph_repeat = True
                        break
            
            for p in non_empty_paragraphs:
                check_p = p
                if len(check_p) > 5000:
                    check_p = check_p[:2500] + check_p[-2500:]
                
                for match in re.finditer(r'((.{2,20}?)\2{20,})', check_p):
                    repeated_unit = match.group(2)
                    if not repeated_unit.strip() or set(repeated_unit) == {'-'}:
                        continue
                    if prompt_content and repeated_unit in prompt_content:
                        continue
                    if len(match.group(1)) / len(check_p) > 0.5:
                        intra_paragraph_repetition = True
                        break
                if intra_paragraph_repetition:
                    break
            
            ngrams = [4, 6, 8, 10]
            tokens = reasoning_content.split()
            for n in ngrams:
                max_rep, max_ngram_tokens = get_max_consecutive_repetition(tokens, n)
                if max_rep > 50:
                    ngram_str = " ".join(max_ngram_tokens) if max_ngram_tokens else ""
                    if prompt_content and ngram_str in prompt_content:
                        continue
                    high_ngram_repetition[str(n)] = {
                        "count": max_rep,
                        "gram": ngram_str[:100] + "..." if len(ngram_str) > 100 else ngram_str
                    }

        return {
            "response_id": response_id,
            "cr": cr,
            "lrr": lrr,
            "max_line_len": max_line_len,
            "token_repetition": token_bad,
            "lang_bad": lang_bad,
            "lang_reasons": lang_reasons,
            "safe_cjk": is_safe_cjk,
            "flaw_backtracking": backtracking_count,
            "flaw_uncertainty": uncertainty_count,
            "high_paragraph_count": high_paragraph_count,
            "sequential_paragraph_repeat": sequential_paragraph_repeat,
            "intra_paragraph_repetition": intra_paragraph_repetition,
            "high_ngram_repetition": high_ngram_repetition
        }
        
    except Exception as e:
        logger.error(f"Error processing response {item.get('id')}: {e}")
        return None


class ResponseAnnotator:
    def __init__(self, db: ReasoningDatabase):
        self.db = db
        
    def process(self, limit: int = 10000, offset: int = 0, concurrency: int = 8, redo: bool = False):
        logger.info(f"Starting annotation with limit={limit}, offset={offset}, concurrency={concurrency}, redo={redo}")
        
        # Batch size for processing
        BATCH_SIZE = 5000
        
        # Use a streaming execution to avoid loading all rows into RAM
        with self.db.engine.connect() as conn:
            query = select(
                self.db.responses.c.id,
                self.db.responses.c.reasoning_trace,
                self.db.problems.c.problem_content
            ).select_from(
                self.db.responses.join(self.db.problems, self.db.responses.c.problem_id == self.db.problems.c.id)
            ).where(
                self.db.responses.c.verification_status == 'passed'
            )
            
            if not redo:
                query = query.where(
                    ~exists(select(1).where(self.db.response_annotations.c.response_id == self.db.responses.c.id))
                )

            if limit:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)
            
            # stream_results=True enables server-side cursor for some dialects (like psycopg2)
            # or streaming behavior.
            result_proxy = conn.execution_options(stream_results=True).execute(query)
            
            # We will read chunks from the cursor and feed them to the pool
            pool = multiprocessing.Pool(processes=concurrency)
            
            try:
                batch = []
                total_processed = 0
                
                # Progress bar total is estimated if limit is provided, else unknown
                pbar = tqdm(total=limit if limit else None, desc="Annotating", unit="resp")

                for row in result_proxy:
                    batch.append(dict(row._mapping))
                    
                    if len(batch) >= BATCH_SIZE:
                        # Process batch
                        self._process_and_insert_batch(pool, batch)
                        total_processed += len(batch)
                        pbar.update(len(batch))
                        batch = []
                
                # Process remaining
                if batch:
                    self._process_and_insert_batch(pool, batch)
                    total_processed += len(batch)
                    pbar.update(len(batch))
                    
                pbar.close()
                
            except Exception as e:
                logger.error(f"Error during streaming annotation: {e}")
                traceback.print_exc()
            finally:
                pool.close()
                pool.join()

        logger.info("Annotation complete.")

    def _process_and_insert_batch(self, pool, batch):
        """Helper to process a batch and insert results"""
        results = []
        # imap_unordered is good, but for small batches map might be faster or just fine.
        # using imap to keep it lazy-ish? No, we have the batch in memory.
        # map() blocks until all are done, which is fine for batching.
        batch_results = pool.map(process_item_worker, batch)
        
        valid_results = [r for r in batch_results if r is not None]
        
        if valid_results:
            self.db.insert_annotations_batch(valid_results)
