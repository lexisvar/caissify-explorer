"""Lichess Broadcast Import Service - Download and import official broadcast games."""

from collections.abc import AsyncIterator
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path

import chess.pgn
import httpx
import zstandard as zstd

from app.services.pgn_import_service import PGNImportService
from app.utils.logging import get_logger

logger = get_logger(__name__)


class LichessBroadcastService:
    """Service for importing Lichess broadcast archives."""
    
    def __init__(self, db):
        """
        Initialize Lichess broadcast service.
        
        Args:
            db: Database session
        """
        self.db = db
        self.base_url = "https://database.lichess.org/broadcast"
        self.pgn_service = PGNImportService(db)
    
    def _get_archive_filename(self, year: int, month: int) -> str:
        """Get standard Lichess archive filename."""
        return f"lichess_db_broadcast_{year}-{month:02d}.pgn.zst"
    
    def _get_archive_url(self, year: int, month: int) -> str:
        """Get URL for Lichess broadcast archive."""
        filename = self._get_archive_filename(year, month)
        return f"{self.base_url}/{filename}"
    
    async def download_archive(
        self,
        year: int,
        month: int,
        data_dir: Path,
        progress_callback=None,
    ) -> Path:
        """
        Download Lichess broadcast archive if not already cached.
        
        Uses persistent volume to avoid re-downloading on worker restart.
        
        Args:
            year: Archive year
            month: Archive month (1-12)
            data_dir: Directory to store downloaded file
            progress_callback: Optional callback(downloaded_mb, total_mb, percent)
        
        Returns:
            Path to downloaded .pgn.zst file
        """
        filename = self._get_archive_filename(year, month)
        archive_path = data_dir / "lichess_broadcasts" / filename
        
        # Create directory if needed
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if already downloaded
        if archive_path.exists():
            file_size_mb = archive_path.stat().st_size / (1024 * 1024)
            logger.info(
                "Archive already downloaded, using cached file",
                archive=filename,
                size_mb=round(file_size_mb, 2),
                path=str(archive_path),
            )
            return archive_path

        # Download archive
        url = self._get_archive_url(year, month)
        logger.info(
            "Downloading Lichess broadcast archive",
            url=url,
            dest=str(archive_path),
        )
        
        try:
            async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    
                    total_size = int(response.headers.get("content-length", 0))
                    total_mb = total_size / (1024 * 1024)
                    
                    logger.info(
                        "Download started",
                        archive=filename,
                        size_mb=round(total_mb, 2),
                    )
                    
                    # Write to temp file first, then rename (atomic)
                    temp_path = archive_path.with_suffix('.tmp')
                    
                    with open(temp_path, "wb") as f:
                        downloaded = 0
                        last_update = 0
                        
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):  # 1MB chunks
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Update progress every 5MB
                            if downloaded - last_update >= (5 * 1024 * 1024):
                                downloaded_mb = downloaded / (1024 * 1024)
                                progress_pct = (downloaded / total_size * 100) if total_size else 0
                                
                                if progress_callback:
                                    progress_callback(downloaded_mb, total_mb, progress_pct)
                                
                                logger.info(
                                    "Download progress",
                                    archive=filename,
                                    downloaded_mb=round(downloaded_mb, 2),
                                    total_mb=round(total_mb, 2),
                                    progress_pct=round(progress_pct, 1),
                                )
                                last_update = downloaded

                    # Rename temp file to final name (atomic operation)
                    temp_path.rename(archive_path)

                    logger.info(
                        "Download completed",
                        archive=filename,
                        size_mb=round(downloaded / (1024 * 1024), 2),
                    )
                    
                    return archive_path
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.error(
                    "Archive not found (may not exist yet)",
                    archive=filename,
                    url=url,
                )
                raise ValueError(f"Archive {filename} not found on Lichess") from None
            raise

        except Exception as e:
            # Clean up partial download
            if temp_path.exists():
                temp_path.unlink()
            logger.error("Download failed", archive=filename, error=str(e))
            raise
    
    async def parse_archive_games(
        self,
        archive_path: Path,
    ) -> AsyncIterator[chess.pgn.Game]:
        """
        Stream parse games from compressed Lichess archive.
        
        Decompresses .zst file on-the-fly without creating temporary files.
        
        Args:
            archive_path: Path to .pgn.zst archive file
        
        Yields:
            chess.pgn.Game objects
        """
        logger.info("Parsing archive", path=str(archive_path))

        try:
            # Decompress and parse in streaming fashion
            with open(archive_path, "rb") as compressed_file:
                dctx = zstd.ZstdDecompressor()

                # Stream decompress
                with dctx.stream_reader(compressed_file) as reader:
                    # Wrap in text mode for chess.pgn
                    text_stream = TextIOWrapper(reader, encoding='utf-8')

                    # Parse PGN games
                    game_count = 0
                    while True:
                        game = chess.pgn.read_game(text_stream)
                        if game is None:
                            break

                        game_count += 1
                        if game_count % 1000 == 0:
                            logger.debug(f"Parsed {game_count} games from archive")

                        yield game

            logger.info("Archive parsing complete", games_parsed=game_count)

        except Exception as e:
            logger.error("Failed to parse archive", path=str(archive_path), error=str(e))
            raise
    
    async def import_broadcast_archive(
        self,
        year: int,
        month: int,
        data_dir: Path = Path("/app/data"),
        batch_size: int = 1000,
        progress_callback=None,
    ) -> dict:
        """
        Download and import Lichess broadcast archive.
        
        This method:
        1. Downloads archive to persistent volume (skips if already exists)
        2. Decompresses and parses PGN games
        3. Extracts FIDE IDs from headers when available
        4. Imports games with automatic deduplication
        5. Links players by FIDE ID or name matching
        
        Args:
            year: Archive year
            month: Archive month (1-12)
            data_dir: Persistent storage directory (Railway volume)
            batch_size: Games per database batch
            progress_callback: Optional progress update callback
        
        Returns:
            Import statistics dict
        """
        source = f"lichess_broadcast_{year}_{month:02d}"

        logger.info(
            "Starting Lichess broadcast import",
            year=year,
            month=month,
            source=source,
        )
        
        # Step 1: Download archive (uses cache if available)
        archive_path = await self.download_archive(
            year=year,
            month=month,
            data_dir=data_dir,
            progress_callback=progress_callback,
        )
        
        # Step 2: Convert games to dict format with FIDE IDs
        stats = {
            "total": 0,
            "inserted": 0,
            "skipped": 0,
            "errors": 0,
            "with_fide_ids": 0,
        }
        
        batch = []
        sequence_number = 0
        
        async for game in self.parse_archive_games(archive_path):
            sequence_number += 1
            stats["total"] += 1
            
            try:
                # Extract game data
                headers = game.headers
                
                # Extract FIDE IDs from headers (Lichess provides these!)
                white_fide_id = headers.get("WhiteFideId")
                black_fide_id = headers.get("BlackFideId")
                
                if white_fide_id or black_fide_id:
                    stats["with_fide_ids"] += 1
                
                # Parse FIDE IDs (they're strings in PGN)
                white_fide_id = int(white_fide_id) if white_fide_id and white_fide_id.isdigit() else None
                black_fide_id = int(black_fide_id) if black_fide_id and black_fide_id.isdigit() else None
                
                # Get date
                date_str = headers.get("UTCDate", headers.get("Date", "????.??.??"))
                game_date = self.pgn_service._parse_pgn_date(date_str)
                
                # Skip games without date
                if not game_date:
                    stats["errors"] += 1
                    continue
                
                # Extract moves
                full_pgn = str(game)
                
                # Generate unique game ID from GameURL or broadcast info
                game_url = headers.get("GameURL", "")
                if game_url:
                    # Use the unique game URL as source_game_id
                    source_game_id = game_url
                else:
                    # Fallback: use broadcast info + board number
                    broadcast_name = headers.get("BroadcastName", "unknown")
                    round_name = headers.get("Round", "?")
                    board = headers.get("Board", "?")
                    source_game_id = f"{broadcast_name}_{round_name}_{board}"
                
                # Build game data
                game_data = {
                    "source": source,
                    "source_game_id": source_game_id,
                    "game_sequence_number": sequence_number,
                    "white_fide_id": white_fide_id,
                    "black_fide_id": black_fide_id,
                    "white_name": headers.get("White", "Unknown"),
                    "black_name": headers.get("Black", "Unknown"),
                    "white_rating": int(headers.get("WhiteElo", 0)) or None,
                    "black_rating": int(headers.get("BlackElo", 0)) or None,
                    "event": headers.get("Event") or headers.get("BroadcastName", "Unknown"),
                    "site": headers.get("Site", "Lichess"),
                    "date": game_date,
                    "round": headers.get("Round", "?"),
                    "result": headers.get("Result", "*"),
                    "eco": headers.get("ECO"),
                    "time_control": headers.get("TimeControl"),
                    "pgn": full_pgn,
                    "source_url": headers.get("BroadcastURL"),
                    "has_parsing_errors": False,
                    "parsing_error_message": None,
                    "last_valid_move_number": None,
                    "created_at": datetime.utcnow(),
                }
                
                batch.append(game_data)
                
                # Insert batch
                if len(batch) >= batch_size:
                    inserted, skipped = await self.pgn_service._insert_batch(batch, stats["total"])
                    stats["inserted"] += inserted
                    stats["skipped"] += skipped
                    
                    if progress_callback:
                        progress_callback(stats)

                    batch = []

            except Exception as e:
                logger.warning(f"Failed to parse game {sequence_number}: {e}")
                stats["errors"] += 1

        # Insert final batch
        if batch:
            inserted, skipped = await self.pgn_service._insert_batch(batch, stats["total"])
            stats["inserted"] += inserted
            stats["skipped"] += skipped

        logger.info(
            "Lichess broadcast import complete",
            year=year,
            month=month,
            total=stats["total"],
            inserted=stats["inserted"],
            skipped=stats["skipped"],
            with_fide_ids=stats["with_fide_ids"],
            errors=stats["errors"],
        )
        
        return stats
