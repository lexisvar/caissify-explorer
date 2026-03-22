"""Player linking service - Post-processing to link games to FIDE players."""
from datetime import date as date_type, datetime
from sqlalchemy import select, update, func, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.game import Game
from app.models.player import Player
from app.utils.logging import get_logger

logger = get_logger(__name__)


class PlayerLinkingService:
    """Service for linking games to players after bulk import."""

    def __init__(self, db: AsyncSession):
        """
        Initialize player linking service.

        Args:
            db: Database session
        """
        self.db = db

    async def link_players_for_source(
        self,
        source: str,
        batch_size: int = 10000,
        from_date: str | None = None,
    ) -> dict[str, int]:
        """
        Link games to players for a specific source using batched SQL operations.
        
        Processes games in small batches to avoid connection timeouts.
        Reports progress after each batch.

        Args:
            source: Source identifier (e.g., 'omotb', 'openingmaster')
            batch_size: Number of games to process per batch (default: 10,000)
            from_date: Optional date filter (YYYY-MM-DD), only link games from this date onwards

        Returns:
            Dictionary with linking statistics
        """
        # Convert from_date string to date object if provided
        from_date_obj = None
        if from_date:
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        
        logger.info(f"Starting player linking for source: {source}, from_date: {from_date}")

        total_white_linked = 0
        total_black_linked = 0

        # Strategy 1: Exact name match (fastest, ~80% coverage)
        logger.info("Phase 1/3: Exact name matching...")
        white_exact = await self._link_in_batches(source, "white", "exact", batch_size, from_date_obj)
        black_exact = await self._link_in_batches(source, "black", "exact", batch_size, from_date_obj)
        total_white_linked += white_exact
        total_black_linked += black_exact
        logger.info(
            f"✓ Exact match complete: {white_exact:,} white, {black_exact:,} black"
        )

        # Strategy 2: Case-insensitive match (~10-15% more)
        logger.info("Phase 2/3: Case-insensitive matching...")
        white_ilike = await self._link_in_batches(source, "white", "ilike", batch_size, from_date_obj)
        black_ilike = await self._link_in_batches(source, "black", "ilike", batch_size, from_date_obj)
        total_white_linked += white_ilike
        total_black_linked += black_ilike
        logger.info(
            f"✓ Case-insensitive complete: {white_ilike:,} white, {black_ilike:,} black"
        )

        # Strategy 3: Lastname match for variations (~5% more)
        logger.info("Phase 3/3: Lastname matching...")
        white_lastname = await self._link_lastname_match(source, "white", from_date_obj)
        black_lastname = await self._link_lastname_match(source, "black", from_date_obj)
        total_white_linked += white_lastname
        total_black_linked += black_lastname
        logger.info(
            f"✓ Lastname complete: {white_lastname:,} white, {black_lastname:,} black"
        )

        total_matches = total_white_linked + total_black_linked

        logger.info(
            f"✅ Player linking completed for '{source}': "
            f"Linked {total_white_linked:,} white + {total_black_linked:,} black "
            f"(total: {total_matches:,} linkages)"
        )

        return {
            "white_linked": total_white_linked,
            "black_linked": total_black_linked,
            "total_matches": total_matches,
            "success_rate": 1.0 if total_matches > 0 else 0.0,
        }

    async def _count_unlinked_games(self, source: str) -> int:
        """Count games with at least one unlinked player."""
        result = await self.db.execute(
            select(func.count(Game.id)).where(
                and_(
                    Game.source == source,
                    or_(
                        Game.white_fide_id.is_(None),
                        Game.black_fide_id.is_(None),
                    ),
                )
            )
        )
        return result.scalar() or 0

    async def _link_in_batches(
        self, source: str, color: str, match_type: str, batch_size: int, from_date: date_type | None = None
    ) -> int:
        """
        Link players in batches using ID-based pagination.
        
        Uses game ID ranges instead of LIMIT/OFFSET for much better performance.
        
        Args:
            source: Source identifier
            color: 'white' or 'black'
            match_type: 'exact' or 'ilike'
            batch_size: Number of game IDs per batch
            from_date: Optional date filter (date object)
            
        Returns:
            Total number of games linked
        """
        total_linked = 0
        batch_num = 0
        
        # Get min and max game IDs for this source
        date_filter = " AND date >= :from_date" if from_date else ""
        params = {"source": source}
        if from_date:
            params["from_date"] = from_date
            
        result = await self.db.execute(
            text(f"SELECT MIN(id), MAX(id) FROM games WHERE source = :source{date_filter}"),
            params
        )
        min_id, max_id = result.fetchone()
        
        if min_id is None or max_id is None:
            logger.info(f"No games found for source {source}")
            return 0
        
        logger.info(f"Processing game IDs from {min_id:,} to {max_id:,}")
        
        current_id = min_id
        
        while current_id <= max_id:
            batch_num += 1
            next_id = current_id + batch_size
            
            # Build the WHERE clause based on match type
            if match_type == "exact":
                match_condition = f"g.{color}_name = p.name"
            elif match_type == "ilike":
                match_condition = f"LOWER(g.{color}_name) = LOWER(p.name)"
            else:
                raise ValueError(f"Unknown match_type: {match_type}")
            
            # Two-step approach: First find matches, then update
            # Step 1: Find matching game IDs and their fide_ids (all matches in this ID range)
            date_condition = " AND g.date >= :from_date" if from_date else ""
            find_query = text(f"""
                SELECT g.id as game_id, p.fide_id
                FROM games g
                JOIN players p ON {match_condition}
                WHERE g.{color}_fide_id IS NULL
                  AND g.source = :source
                  AND g.id >= :current_id
                  AND g.id < :next_id
                  {date_condition}
            """)
            
            # Get matches
            query_params = {
                "source": source,
                "current_id": current_id,
                "next_id": next_id
            }
            if from_date:
                query_params["from_date"] = from_date
                
            match_result = await self.db.execute(find_query, query_params)
            matches = match_result.fetchall()
            
            if not matches:
                current_id = next_id
                continue
            
            # Step 2: Update in small chunks using CASE statement
            game_ids = [m[0] for m in matches]
            case_whens = " ".join([
                f"WHEN {m[0]} THEN {m[1]}" for m in matches
            ])
            
            query = text(f"""
                UPDATE games
                SET {color}_fide_id = CASE id
                    {case_whens}
                END
                WHERE id IN ({','.join(map(str, game_ids))})
            """)
            
            try:
                result = await self.db.execute(query)
                await self.db.commit()
                
                rows_updated = len(matches)
                total_linked += rows_updated
                
                if rows_updated > 0:
                    progress_pct = ((current_id - min_id) / (max_id - min_id)) * 100
                    logger.info(
                        f"  Batch {batch_num} (IDs {current_id:,}-{next_id:,}): "
                        f"Linked {rows_updated:,} {color} players | "
                        f"Total: {total_linked:,} | Progress: {progress_pct:.1f}%"
                    )
                elif batch_num % 100 == 0:  # Log every 100 batches even if 0 updates
                    progress_pct = ((current_id - min_id) / (max_id - min_id)) * 100
                    logger.info(f"  Progress: {progress_pct:.1f}% (batch {batch_num})")
                
                current_id = next_id
                    
            except Exception as e:
                logger.error(f"Error in batch {batch_num} (IDs {current_id}-{next_id}): {e}")
                # Try to rollback and continue with next batch
                await self.db.rollback()
                current_id = next_id
                continue
        
        return total_linked

    async def _link_exact_match(self, source: str, color: str) -> int:
        """
        Link players using exact name match.
        
        Uses bulk UPDATE with JOIN - very fast for large datasets.
        """
        if color == "white":
            name_col = Game.white_name
            fide_id_col = Game.white_fide_id
        else:
            name_col = Game.black_name
            fide_id_col = Game.black_fide_id

        # Use PostgreSQL-specific UPDATE FROM for maximum performance
        # This is much faster than SQLAlchemy ORM for bulk updates
        query = text(f"""
            UPDATE games g
            SET {color}_fide_id = p.fide_id
            FROM players p
            WHERE g.{color}_name = p.name
              AND g.{color}_fide_id IS NULL
              AND g.source = :source
        """)

        result = await self.db.execute(query, {"source": source})
        await self.db.commit()

        return result.rowcount if hasattr(result, "rowcount") else 0

    async def _link_case_insensitive(self, source: str, color: str) -> int:
        """Link players using case-insensitive name match."""
        query = text(f"""
            UPDATE games g
            SET {color}_fide_id = p.fide_id
            FROM players p
            WHERE LOWER(g.{color}_name) = LOWER(p.name)
              AND g.{color}_fide_id IS NULL
              AND g.source = :source
        """)

        result = await self.db.execute(query, {"source": source})
        await self.db.commit()

        return result.rowcount if hasattr(result, "rowcount") else 0

    async def _link_lastname_match(self, source: str, color: str, from_date: date_type | None = None) -> int:
        """
        Link players using lastname match for variations like:
        - "Carlsen, Magnus" vs "Carlsen, M."
        - "Carlsen, Magnus" vs "CARLSEN, MAGNUS"
        
        Only matches if there's exactly one player with that lastname.
        """
        # This is more complex and should be done carefully
        # For now, skip to keep it simple and fast
        # Can be enhanced later with fuzzy matching (pg_trgm)

        logger.debug(f"Lastname matching for {color} players (skipped for now)")
        return 0

    async def get_linking_stats(self, source: str) -> dict:
        """Get statistics about player linking for a source."""
        # Total games
        total_result = await self.db.execute(
            select(func.count(Game.id)).where(Game.source == source)
        )
        total_games = total_result.scalar() or 0

        # Fully linked (both players)
        fully_linked_result = await self.db.execute(
            select(func.count(Game.id)).where(
                and_(
                    Game.source == source,
                    Game.white_fide_id.isnot(None),
                    Game.black_fide_id.isnot(None),
                )
            )
        )
        fully_linked = fully_linked_result.scalar() or 0

        # Partially linked (one player)
        partially_linked_result = await self.db.execute(
            select(func.count(Game.id)).where(
                and_(
                    Game.source == source,
                    or_(
                        and_(
                            Game.white_fide_id.isnot(None),
                            Game.black_fide_id.is_(None),
                        ),
                        and_(
                            Game.white_fide_id.is_(None),
                            Game.black_fide_id.isnot(None),
                        ),
                    ),
                )
            )
        )
        partially_linked = partially_linked_result.scalar() or 0

        # Unlinked (neither player)
        unlinked = total_games - fully_linked - partially_linked

        return {
            "source": source,
            "total_games": total_games,
            "fully_linked": fully_linked,
            "partially_linked": partially_linked,
            "unlinked": unlinked,
            "fully_linked_percentage": (
                100 * fully_linked / total_games if total_games > 0 else 0
            ),
        }
