import logging
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import select, and_, or_
from database import ReasoningDatabase

logger = logging.getLogger(__name__)

class ResponseUpdater:
    def __init__(self, db: ReasoningDatabase):
        self.db = db
        
    def process(self, 
                new_status: str,
                input_file: Optional[str] = None,
                after: Optional[datetime] = None,
                before: Optional[datetime] = None,
                difficulty: Optional[str] = None,
                current_status: Optional[str] = None,
                limit: Optional[int] = None,
                dryrun: bool = False):
        """
        Update verification status for responses matching the criteria.
        """
        
        target_ids = []
        
        # 1. Identify target IDs
        if input_file:
            logger.info(f"Reading target IDs from file: {input_file}")
            if not os.path.exists(input_file):
                logger.error(f"Input file not found: {input_file}")
                return
            try:
                with open(input_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        try:
                            data = json.loads(line)
                            if isinstance(data, dict) and 'id' in data:
                                target_ids.append(data['id'])
                            elif isinstance(data, str):
                                target_ids.append(data)
                        except:
                            target_ids.append(line)
            except Exception as e:
                logger.error(f"Error reading input file: {e}")
                return
            logger.info(f"Loaded {len(target_ids)} IDs from file.")
        
        # 2. Build Query
        # If we have file IDs, we filter by them. If not, we query strictly attributes.
        # We need to join with Problems if difficulty is specified.
        
        stmt = select(self.db.responses.c.id)
        
        join_problems = False
        if difficulty:
            join_problems = True
            
        if join_problems:
            stmt = stmt.select_from(
                self.db.responses.join(self.db.problems, self.db.responses.c.problem_id == self.db.problems.c.id)
            )
            
        conditions = []
        
        if target_ids:
            conditions.append(self.db.responses.c.id.in_(target_ids))
            
        if after:
            conditions.append(self.db.responses.c.timestamp >= after)
        
        if before:
            conditions.append(self.db.responses.c.timestamp <= before)
            
        if current_status:
            # allow comma separated
            statuses = [s.strip() for s in current_status.split(',')]
            status_conds = []
            for s in statuses:
                if s.lower() in ('none', 'null'):
                    status_conds.append(self.db.responses.c.verification_status.is_(None))
                else:
                    status_conds.append(self.db.responses.c.verification_status == s)
            if status_conds:
                conditions.append(or_(*status_conds))
                
        if difficulty:
            # allow comma separated
            diffs = [d.strip() for d in difficulty.split(',')]
            conditions.append(self.db.problems.c.difficulty.in_(diffs))

        if conditions:
            stmt = stmt.where(and_(*conditions))
            
        if limit and not input_file: # Only limit if not file-based (or apply limit to file content? assume query limit)
            stmt = stmt.limit(limit)

        # 3. Execute Selection
        logger.info("Querying candidates...")
        with self.db.engine.connect() as conn:
            candidates = [row[0] for row in conn.execute(stmt)]
            
        logger.info(f"Found {len(candidates)} matching responses match criteria.")
        
        if not candidates:
            return

        # 4. Perform Update
        logger.info(f"Updating {len(candidates)} responses to status='{new_status}' (dryrun={dryrun})")
        
        batch_size = 1000
        total_updated = 0
        
        for i in range(0, len(candidates), batch_size):
            batch_ids = candidates[i:i+batch_size]
            
            updates = []
            for rid in batch_ids:
                updates.append({
                    "id": rid,
                    "verification_status": new_status,
                    # Reset verified/verifier fields if needed? 
                    # For now just status.
                })
            
            if not dryrun:
                self.db.update_responses_batch(updates)
                
            total_updated += len(updates)
            
        logger.info(f"Done. Updated {total_updated} responses.")
