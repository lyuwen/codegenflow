#!/usr/bin/env python3
"""
Database Status Tool
Shows statistics and verification progress for the problems database.
"""

import sqlite3
import json
from datetime import datetime

DB_PATH = "problems.db"

def get_stats(conn):
    """Get comprehensive database statistics."""
    cursor = conn.cursor()
    
    # Problem statistics
    cursor.execute("SELECT COUNT(*) FROM problems")
    total_problems = cursor.fetchone()[0]
    
    cursor.execute("SELECT source, COUNT(*) FROM problems GROUP BY source ORDER BY source")
    problems_by_source = cursor.fetchall()
    
    # Response statistics
    cursor.execute("SELECT COUNT(*) FROM responses")
    total_responses = cursor.fetchone()[0]
    
    cursor.execute("SELECT verification_status, COUNT(*) FROM responses GROUP BY verification_status ORDER BY verification_status")
    responses_by_status = cursor.fetchall()
    
    cursor.execute("SELECT model, COUNT(*) FROM responses GROUP BY model ORDER BY COUNT(*) DESC")
    responses_by_model = cursor.fetchall()
    
    # Detailed error breakdown (top 5)
    cursor.execute("""
        SELECT verification_details, COUNT(*) as cnt 
        FROM responses 
        WHERE verification_status = 'error' 
        GROUP BY verification_details 
        ORDER BY cnt DESC 
        LIMIT 5
    """)
    top_errors = cursor.fetchall()
    
    # Pass rate statistics
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN verification_status = 'passed' THEN 1 END) as passed,
            COUNT(CASE WHEN verification_status = 'failed' THEN 1 END) as failed,
            COUNT(CASE WHEN verification_status IN ('passed', 'failed') THEN 1 END) as total_verified
        FROM responses
    """)
    pass_stats = cursor.fetchone()
    
    return {
        'total_problems': total_problems,
        'problems_by_source': problems_by_source,
        'total_responses': total_responses,
        'responses_by_status': responses_by_status,
        'responses_by_model': responses_by_model,
        'top_errors': top_errors,
        'pass_stats': pass_stats
    }

def format_number(num):
    """Format number with thousands separator."""
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
    
    # Top errors
    if stats['top_errors']:
        print(f"\n   ⚠️  Top Error Types:")
        for i, (error_json, count) in enumerate(stats['top_errors'], 1):
            try:
                error_data = json.loads(error_json)
                error_msg = error_data.get('error', 'Unknown')
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
        print(f"\n   🤖 By Model:")
        for model, count in stats['responses_by_model'][:5]:  # Top 5 models
            percentage = (count / stats['total_responses']) * 100
            model_name = model if model else "(unknown)"
            print(f"      {model_name:30s}: {format_number(count):>10s} ({percentage:>5.1f}%)")
    
    print("\n" + "=" * 80)

def main():
    try:
        conn = sqlite3.connect(DB_PATH)
        stats = get_stats(conn)
        print_status(stats)
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
