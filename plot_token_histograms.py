import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_PATH = "problems.db"
OUTPUT_FILE = "token_histograms.png"

def main():
    print("Loading data...")
    conn = sqlite3.connect(DB_PATH)
    
    query = """
        SELECT 
            r.model,
            p.difficulty,
            r.completion_tokens
        FROM responses r
        JOIN problems p ON r.problem_id = p.id
        WHERE r.completion_tokens IS NOT NULL
        AND r.verification_status = 'passed'
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} records.")
    
    # Get unique models and difficulties
    models = sorted(df['model'].unique())
    difficulties = sorted(df['difficulty'].unique())
    
    n_models = len(models)
    n_diffs = len(difficulties)
    
    print(f"Models: {models}")
    print(f"Difficulties: {difficulties}")
    
    # Create a grid of subplots
    # Rows = Models, Cols = Difficulties
    fig, axes = plt.subplots(n_models, n_diffs, figsize=(5 * n_diffs, 4 * n_models), sharex=True)
    
    # Handle single row/col case to ensure axes is 2D array
    if n_models == 1 and n_diffs == 1:
        axes = [[axes]]
    elif n_models == 1:
        axes = [axes]
    elif n_diffs == 1:
        axes = [[ax] for ax in axes]
        
    plt.suptitle("Average Completion Tokens Distribution by Model and Difficulty", fontsize=16)
    
    for i, model in enumerate(models):
        for j, diff in enumerate(difficulties):
            ax = axes[i][j]
            
            subset = df[(df['model'] == model) & (df['difficulty'] == diff)]
            data = subset['completion_tokens']
            
            if len(data) > 0:
                mean_val = data.mean()
                median_val = data.median()
                
                sns.histplot(data, ax=ax, kde=True, bins=30)
                ax.axvline(mean_val, color='r', linestyle='--', label=f'Mean: {mean_val:.1f}')
                ax.axvline(median_val, color='g', linestyle='-', label=f'Median: {median_val:.1f}')
                ax.legend()
            else:
                ax.text(0.5, 0.5, "No Data", ha='center', va='center')
            
            # Set titles and labels
            if i == 0:
                ax.set_title(f"Difficulty: {diff}", fontsize=12, fontweight='bold')
            if j == 0:
                # Shorten model name for display
                model_name = model if model else "Unknown"
                # if len(model_name) > 20:
                #     model_name = model_name[:17] + "..."
                ax.set_ylabel(f"{model_name}\nCount", fontsize=10)
            else:
                ax.set_ylabel("")
                
            if i == n_models - 1:
                ax.set_xlabel("Completion Tokens")
            
            ax.grid(True, alpha=0.3)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    print(f"Saving plot to {OUTPUT_FILE}...")
    plt.savefig(OUTPUT_FILE, dpi=300)
    print("Done.")

if __name__ == "__main__":
    main()
