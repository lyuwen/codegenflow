#!/usr/bin/env python3
"""
Database Status Tool
Shows statistics and verification progress for the problems database.
"""

import os
import argparse
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from database import ReasoningDatabase

load_dotenv()
DB_URL = os.environ.get("DB_URL")

def get_stats(db):
    """Get comprehensive database statistics."""
    print("Gathering statistics...", flush=True)
    
    with db.engine.connect() as conn:
        # Problem statistics
        print("  - Counting problems...", end="\r", flush=True)
        total_problems = conn.execute(text("SELECT COUNT(*) FROM problems")).scalar()
        
        problems_by_source = conn.execute(text("SELECT source, COUNT(*) FROM problems GROUP BY source ORDER BY source")).fetchall()
        
        # Response statistics
        print("  - Counting responses...", end="\r", flush=True)
        total_responses = conn.execute(text("SELECT COUNT(*) FROM responses")).scalar()
        
        responses_by_status = conn.execute(text("SELECT verification_status, COUNT(*) FROM responses GROUP BY verification_status ORDER BY verification_status")).fetchall()
        
        responses_by_model = conn.execute(text("SELECT model, COUNT(*) FROM responses GROUP BY model ORDER BY COUNT(*) DESC")).fetchall()
        
        # Detailed error breakdown (top 5)
        print("  - Analyzing errors...", end="\r", flush=True)
        # Note: verification_details is JSON/JSONB. Grouping by it might be slow or behave differently across DBs.
        # Casting to text might be safer for grouping if it's a complex object.
        # For now, we assume it works or we cast if needed.
        # Postgres requires casting JSONB to TEXT to group by it easily if not using specific operators, 
        # but standard GROUP BY works for equality.
        top_errors = conn.execute(text("""
            SELECT verification_details, COUNT(*) as cnt 
            FROM responses 
            WHERE verification_status = 'error' 
            GROUP BY verification_details 
            ORDER BY cnt DESC 
            LIMIT 5
        """)).fetchall()
        
        # Pass rate statistics
        print("  - Calculating pass rates...", end="\r", flush=True)
        pass_stats = conn.execute(text("""
            SELECT 
                COUNT(CASE WHEN verification_status = 'passed' THEN 1 END) as passed,
                COUNT(CASE WHEN verification_status = 'failed' THEN 1 END) as failed,
                COUNT(CASE WHEN verification_status IN ('passed', 'failed') THEN 1 END) as total_verified
            FROM responses
        """)).fetchone()
    
        # Total problems per difficulty
        print("  - Counting problems per difficulty...", end="\r", flush=True)
        problems_per_difficulty = dict(conn.execute(text("SELECT difficulty, COUNT(*) FROM problems GROUP BY difficulty")).fetchall())

        # Annotation statistics
        print("  - Calculating annotation stats...", end="\r", flush=True)
        try:
             # Check if table exists first (optional, but good for safety) or just try-except block around it
             annotation_stats = conn.execute(text("""
                SELECT 
                    count(*) as total,
                    count(*) FILTER (WHERE token_repetition = true) as token_repetition,
                    count(*) FILTER (WHERE lang_bad = true) as lang_bad,
                    count(*) FILTER (WHERE safe_cjk = false) as unsafe_cjk,
                    count(*) FILTER (WHERE high_paragraph_count = true) as high_paragraph_count,
                    count(*) FILTER (WHERE sequential_paragraph_repeat = true) as sequential_paragraph_repeat,
                    count(*) FILTER (WHERE intra_paragraph_repetition = true) as intra_paragraph_repetition,
                    count(*) FILTER (WHERE CAST(high_ngram_repetition AS TEXT) != '{}') as high_ngram_repetition,

                    avg(CAST(CAST(cr AS TEXT) AS FLOAT)) as avg_cr,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY CAST(CAST(cr AS TEXT) AS FLOAT)) as p50_cr,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY CAST(CAST(cr AS TEXT) AS FLOAT)) as p95_cr,
                    
                    avg(CAST(CAST(lrr AS TEXT) AS FLOAT)) as avg_lrr,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY CAST(CAST(lrr AS TEXT) AS FLOAT)) as p50_lrr,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY CAST(CAST(lrr AS TEXT) AS FLOAT)) as p95_lrr,

                    avg(max_line_len) as avg_mll,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY max_line_len) as p50_mll,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY max_line_len) as p95_mll,

                    avg(flaw_backtracking) as avg_bt,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY flaw_backtracking) as p50_bt,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY flaw_backtracking) as p95_bt,

                    avg(flaw_uncertainty) as avg_uc,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY flaw_uncertainty) as p50_uc,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY flaw_uncertainty) as p95_uc
                FROM response_annotations
            """)).fetchone()
        except Exception as e:
            # Table might not exist
            annotation_stats = None
        
        # Matrix statistics (Model x Difficulty)
        print("  - Calculating matrix stats (this may take a moment)...", end="\r", flush=True)
        matrix_stats = conn.execute(text("""
            SELECT 
                r.model,
                p.difficulty,
                COUNT(*) as total,
                AVG(r.completion_tokens) as avg_tokens,
                SUM(CASE WHEN r.verification_status = 'passed' THEN 1 ELSE 0 END) as passed_count,
                AVG(CASE WHEN r.verification_status = 'passed' THEN r.completion_tokens END) as avg_passed_tokens,
                COUNT(DISTINCT CASE WHEN r.verification_status = 'passed' THEN r.problem_id END) as unique_passed_problems,
                COUNT(DISTINCT r.problem_id) as unique_attempted_problems
            FROM responses r
            JOIN problems p ON r.problem_id = p.id
            WHERE r.verification_status IN ('passed', 'failed')
            GROUP BY r.model, p.difficulty
            ORDER BY r.model, p.difficulty
        """)).fetchall()
        print(" " * 50, end="\r", flush=True) # Clear line
    
    return {
        'total_problems': total_problems,
        'problems_by_source': problems_by_source,
        'total_responses': total_responses,
        'responses_by_status': responses_by_status,
        'responses_by_model': responses_by_model,
        'top_errors': top_errors,
        'pass_stats': pass_stats,
        'matrix_stats': matrix_stats,
        'problems_per_difficulty': problems_per_difficulty,
        'annotation_stats': annotation_stats
    }

def format_number(num):
    """Format number with thousands separator."""
    if num is None:
        return "0"
    return f"{num:,}"

def print_status(stats):
    """Print formatted status report."""
    print("=" * 80)
    print("DATABASE STATUS REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Problems section
    print(f"\n📚 PROBLEMS")
    print(f"   Total: {format_number(stats['total_problems'])}")
    print(f"\n   By Source:")
    for source, count in stats['problems_by_source']:
        print(f"      {source:20s}: {format_number(count):>10s}")
    
    # Responses section
    print(f"\n💬 RESPONSES")
    print(f"   Total: {format_number(stats['total_responses'])}")
    
    print(f"\n   By Verification Status:")
    status_map = dict(stats['responses_by_status'])
    
    # Order: pending, passed, failed, error, skipped
    status_order = ['pending', 'passed', 'failed', 'error', 'skipped']
    status_icons = {
        'pending': '⏳',
        'passed': '✅',
        'failed': '❌',
        'error': '⚠️',
        'skipped': '⏭️'
    }
    
    for status in status_order:
        count = status_map.get(status, 0)
        if count > 0:
            icon = status_icons.get(status, '•')
            percentage = (count / stats['total_responses']) * 100
            print(f"      {icon} {status:12s}: {format_number(count):>10s} ({percentage:>5.1f}%)")
    
    # Pass rate
    passed, failed, total_verified = stats['pass_stats']
    if total_verified > 0:
        pass_rate = (passed / total_verified) * 100
        print(f"\n   📊 Pass Rate (among verified):")
        print(f"      Passed:  {format_number(passed):>10s}")
        print(f"      Failed:  {format_number(failed):>10s}")
        print(f"      Total:   {format_number(total_verified):>10s}")
        print(f"      Rate:    {pass_rate:>9.2f}%")
    
    # Progress
    verified = sum(count for status, count in stats['responses_by_status'] 
                   if status in ['passed', 'failed', 'skipped'])
    total = stats['total_responses']
    progress = (verified / total) * 100 if total > 0 else 0
    
    print(f"\n   🎯 Verification Progress:")
    print(f"      Verified: {format_number(verified):>10s} / {format_number(total)}")
    print(f"      Progress: {progress:>9.2f}%")
    
    # Matrix Statistics
    if stats['matrix_stats']:
        print(f"\n   📈 Model Performance by Difficulty:")
        # Header
        print(f"      {'Model':<40} {'Diff':<8} {'Pass%':<6} {'Passed':<8} {'AvgTok':<8} {'AvgPassTok':<10} {'Solved':<8} {'TotalProb':<9} {'AvgPass/Prob':<12}")
        print(f"      {'-'*40} {'-'*8} {'-'*6} {'-'*8} {'-'*8} {'-'*10} {'-'*8} {'-'*9} {'-'*12}")
        
        current_model = None
        problems_per_diff = stats.get('problems_per_difficulty', {})
        
        for model, difficulty, total, avg_tokens, passed_count, avg_passed_tokens, unique_passed, unique_attempted in stats['matrix_stats']:
            if model != current_model:
                if current_model is not None:
                    print(f"      {'-'*116}")
                current_model = model
            
            pass_rate = (passed_count / total) * 100 if total > 0 else 0
            avg_tok = avg_tokens if avg_tokens else 0
            avg_passed = avg_passed_tokens if avg_passed_tokens else 0
            
            # Calculate avg passed responses per solved problem
            avg_passed_per_prob = (passed_count / unique_passed) if unique_passed > 0 else 0
            
            # Total problems in this difficulty
            total_problems_in_diff = problems_per_diff.get(difficulty, 0)
            
            model_display = model if model else "Unknown"
            # Truncate model name if too long
            if len(model_display) > 38:
                model_display = model_display[:35] + "..."
                
            diff_display = difficulty if difficulty else "Unk"
            if len(diff_display) > 8:
                 diff_display = diff_display[:8]
            
            print(f"      {model_display:<40} {diff_display:<8} {pass_rate:>5.1f}% {passed_count:>8} {avg_tok:>8.0f} {avg_passed:>10.0f} {unique_passed:>8} {total_problems_in_diff:>9} {avg_passed_per_prob:>12.2f}")

    # Top errors
    if stats['top_errors']:
        print(f"\n   ⚠️  Top Error Types:")
        for i, (error_json, count) in enumerate(stats['top_errors'], 1):
            try:
                # error_json might be a dict (if SQLAlchemy decoded it) or string
                if isinstance(error_json, str):
                    error_data = json.loads(error_json)
                else:
                    error_data = error_json
                    
                error_msg = error_data.get('error', 'Unknown') if error_data else 'Unknown'
                # Truncate long error messages
                if len(error_msg) > 60:
                    error_msg = error_msg[:57] + "..."
                print(f"      {i}. {error_msg}")
                print(f"         Count: {format_number(count)}")
            except:
                print(f"      {i}. [Parse error]")
                print(f"         Count: {format_number(count)}")
    
    # Model breakdown (if multiple models)
    if len(stats['responses_by_model']) > 1:
        print(f"\n   🤖 By Model (Total Responses):")
        for model, count in stats['responses_by_model'][:5]:  # Top 5 models
            percentage = (count / stats['total_responses']) * 100
            model_name = model if model else "(unknown)"
            print(f"      {model_name:30s}: {format_number(count):>10s} ({percentage:>5.1f}%)")
    

    # Annotation Stats
    if stats.get('annotation_stats') and stats['annotation_stats'][0] > 0: # Check total > 0
        astats = stats['annotation_stats']
        # astats mapping: 
        # 0:total, 1:token_rep, 2:lang_bad, 3:unsafe_cjk, 4:high_para, 5:seq_para, 6:intra_para, 7:high_ngram
        # 8:avg_cr, 9:p50_cr, 10:p95_cr 
        # 11:avg_lrr, 12:p50_lrr, 13:p95_lrr
        # 14:avg_mll, 15:p50_mll, 16:p95_mll
        # 17:avg_bt, 18:p50_bt, 19:p95_bt
        # 20:avg_uc, 21:p50_uc, 22:p95_uc

        # Convert to dict for easier access if mapping is available, but tuple index is reliable with fixed query
        # Let's use indices based on the new query order.
        
        print(f"\n🔍 ANNOTATION STATS")
        print(f"   Total Annotated: {format_number(astats[0])}")
        
        print(f"\n   ⚠️  Anomalies:")
        print(f"      Token Repetition:       {format_number(astats[1]):>8}")
        print(f"      Language Bad:           {format_number(astats[2]):>8}")
        print(f"      Unsafe CJK:             {format_number(astats[3]):>8}")
        print(f"      High Para Count:        {format_number(astats[4]):>8}")
        print(f"      Seq Para Repeat:        {format_number(astats[5]):>8}")
        print(f"      Intra Para Repeat:      {format_number(astats[6]):>8}")
        print(f"      High N-gram Repeat:     {format_number(astats[7]):>8}")

        print(f"\n   📊 Metrics (Avg | P50 | P95):")
        print(f"      CR:                     {astats[8]:>8.4f} | {astats[9]:>8.4f} | {astats[10]:>8.4f}")
        print(f"      LRR:                    {astats[11]:>8.4f} | {astats[12]:>8.4f} | {astats[13]:>8.4f}")
        print(f"      Max Line Len:           {astats[14]:>8.1f} | {astats[15]:>8.1f} | {astats[16]:>8.1f}")
        print(f"      Backtracking:           {astats[17]:>8.1f} | {astats[18]:>8.1f} | {astats[19]:>8.1f}")
        print(f"      Uncertainty:            {astats[20]:>8.1f} | {astats[21]:>8.1f} | {astats[22]:>8.1f}")
        
    print("\n" + "=" * 80)

def main():
    parser = argparse.ArgumentParser(description="Show database status")
    parser.add_argument("--db", help="Database URL or path (default: from .env)")
    args = parser.parse_args()

    db_url = args.db if args.db else DB_URL

    if not db_url:
        print("Error: DB_URL not found in .env and --db not provided")
        return 1

    try:
        db = ReasoningDatabase(db_url)
        stats = get_stats(db)
        print_status(stats)
        # db.close()
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
