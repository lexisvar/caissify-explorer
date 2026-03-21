use serde_json::{Value, json};

/// Returns the full OpenAPI 3.1.0 specification as a JSON value.
pub fn spec() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Caissify Opening Explorer",
            "description": "High-performance chess opening explorer for OTB (Caissify/OMOTB), Lichess, and Masters games.\n\n## Authentication\nAll endpoints are public. Admin endpoints (`/import/*`, `/compact`, `/monitor`) should be protected by your reverse proxy in production.\n\n## Position input\nAll explorer endpoints accept a position via:\n- `fen` — FEN string of the starting position (defaults to the standard start)\n- `play` — comma-separated UCI moves to apply from the FEN (e.g. `e2e4,e7e5`)\n- `variant` — game variant (default `chess`)",
            "version": "1.0.0",
            "contact": {
                "name": "Caissify",
                "url": "https://caissify.com"
            }
        },
        "servers": [
            { "url": "/", "description": "This server" }
        ],
        "tags": [
            { "name": "Explorer",   "description": "Query opening statistics and games" },
            { "name": "Games",      "description": "Browse and retrieve individual games" },
            { "name": "FIDE",       "description": "FIDE player profiles and rating history" },
            { "name": "PGN",        "description": "Fetch full PGN of individual games" },
            { "name": "Import",     "description": "Import games into the database" },
            { "name": "Admin",      "description": "Monitoring and maintenance" }
        ],
        "paths": {

            // ── /caissify ─────────────────────────────────────────────────
            "/caissify": {
                "get": {
                    "tags": ["Explorer"],
                    "summary": "Query Caissify / OMOTB opening database",
                    "description": "Returns opening statistics from the Caissify custom OTB game database (e.g. OMOTB).\nAccepts all games regardless of rating. Response format is identical to `/masters`.",
                    "operationId": "getCaissify",
                    "parameters": position_params().iter().chain(year_range_params().iter()).chain(explorer_limit_params().iter()).cloned().collect::<Vec<_>>(),
                    "responses": {
                        "200": explorer_response_200("Caissify opening statistics"),
                        "400": bad_request_response()
                    }
                }
            },

            "/caissify/pgn/{id}": {
                "get": {
                    "tags": ["PGN"],
                    "summary": "Fetch PGN for a Caissify game",
                    "operationId": "getCaissifyPgn",
                    "parameters": [game_id_param()],
                    "responses": {
                        "200": pgn_200_response(),
                        "404": not_found_response()
                    }
                }
            },

            // ── /caissify/games ───────────────────────────────────────────
            "/caissify/games": {
                "get": {
                    "tags": ["Games"],
                    "summary": "List Caissify games (paginated)",
                    "description": "Returns a cursor-paginated list of Caissify games sorted by year. Use `next_page_token` from the response to fetch the next page.",
                    "operationId": "listCaissifyGames",
                    "parameters": [
                        param("fen",         false, "string",  Some("FEN of the position to filter by. When provided, returns games passing through that position (up to 15 top games from the opening index) instead of the full date-sorted list."), None),
                        param("play",        false, "string",  Some("Comma-separated UCI moves to play from the FEN before filtering"), None),
                        param("variant",     false, "string",  Some("Variant (default: chess)"), Some(json!("chess"))),
                        param("limit",       false, "integer", Some("Results per page (default 50, max 200)"), Some(json!(50))),
                        param("since",       false, "integer", Some("Earliest year to include (inclusive)"), Some(json!(2020))),
                        param("until",       false, "integer", Some("Latest year to include (inclusive)"), Some(json!(2026))),
                        param("page_token",  false, "string",  Some("Opaque cursor from a previous response (ignored when fen is set)"), None),
                        param("reverse",     false, "boolean", Some("Return newest games first (default true)"), Some(json!(true))),
                        param("result",      false, "string",  Some("Filter by game result: white, draw, or black"), Some(json!("white"))),
                        param("min_rating",  false, "integer", Some("Minimum max(white_rating, black_rating)"), Some(json!(2700))),
                        param("max_rating",  false, "integer", Some("Maximum max(white_rating, black_rating)"), Some(json!(3000))),
                        param("fide_id",     false, "integer", Some("Filter by FIDE player ID. When set, games are returned from the caissify_game_by_fide index instead of the date index. Cannot be combined with fen/play."), None),
                        param("color",       false, "string",  Some("Filter by colour the FIDE player played (white or black). Only used when fide_id is set."), None),
                    ],
                    "responses": {
                        "200": {
                            "description": "Paginated game list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CaissifyGameListResponse" }
                                }
                            }
                        }
                    }
                }
            },

            "/caissify/games/{id}": {
                "get": {
                    "tags": ["Games"],
                    "summary": "Get compact metadata for a Caissify game",
                    "description": "Returns compact metadata (year, ratings, result, FIDE IDs) for a single Caissify game. Fast point-get on the `caissify_game_meta` column family.",
                    "operationId": "getCaissifyGameMeta",
                    "parameters": [game_id_param()],
                    "responses": {
                        "200": {
                            "description": "Game metadata",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CaissifyGameMeta" }
                                }
                            }
                        },
                        "404": not_found_response()
                    }
                }
            },

            // ── /masters ──────────────────────────────────────────────────
            "/masters": {
                "get": {
                    "tags": ["Explorer"],
                    "summary": "Query Masters opening database",
                    "description": "Returns opening statistics from top-level OTB games (average rating ≥ 2200).",
                    "operationId": "getMasters",
                    "parameters": position_params().iter().chain(year_range_params().iter()).chain(explorer_limit_params().iter()).cloned().collect::<Vec<_>>(),
                    "responses": {
                        "200": explorer_response_200("Masters opening statistics"),
                        "400": bad_request_response()
                    }
                }
            },

            "/masters/pgn/{id}": {
                "get": {
                    "tags": ["PGN"],
                    "summary": "Fetch PGN for a Masters game",
                    "operationId": "getMastersPgn",
                    "parameters": [game_id_param()],
                    "responses": {
                        "200": pgn_200_response(),
                        "404": not_found_response()
                    }
                }
            },

            // ── /lichess ──────────────────────────────────────────────────
            "/lichess": {
                "get": {
                    "tags": ["Explorer"],
                    "summary": "Query Lichess opening database",
                    "description": "Returns opening statistics from the full Lichess.org rated game database, broken down by speed and rating group.",
                    "operationId": "getLichess",
                    "parameters": lichess_params(),
                    "responses": {
                        "200": explorer_response_200("Lichess opening statistics"),
                        "400": bad_request_response()
                    }
                }
            },

            "/lichess/history": {
                "get": {
                    "tags": ["Explorer"],
                    "summary": "Query Lichess per-month history",
                    "description": "Returns per-month game counts for a position (Lichess games only). Same parameters as `/lichess` minus `moves`, `topGames`, and `recentGames`.",
                    "operationId": "getLichessHistory",
                    "parameters": lichess_params(),
                    "responses": {
                        "200": {
                            "description": "Per-month game history",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/HistoryResponse" }
                                }
                            }
                        },
                        "400": bad_request_response()
                    }
                }
            },

            // ── /player ───────────────────────────────────────────────────
            "/player": {
                "get": {
                    "tags": ["Explorer"],
                    "summary": "Query per-player opening statistics",
                    "description": "Returns opening statistics for a specific Lichess player. Streams NDJSON — keepalive newlines are sent while indexing is in progress, then the result object is sent.\n\nIndexing is triggered automatically if the player hasn't been indexed recently.",
                    "operationId": "getPlayer",
                    "parameters": (player_params()),
                    "responses": {
                        "200": {
                            "description": "Player opening statistics (NDJSON stream)",
                            "content": {
                                "application/x-ndjson": {
                                    "schema": { "$ref": "#/components/schemas/ExplorerResponse" }
                                }
                            }
                        },
                        "400": bad_request_response(),
                        "503": { "description": "Player indexer queue is full" }
                    }
                }
            },

            // ── /import/caissify/pgn-url ───────────────────────────────────
            "/import/caissify/pgn-url": {
                "post": {
                    "tags": ["Import"],
                    "summary": "Trigger bulk PGN import from URL",
                    "description": "Instructs the server to download a PGN file from the given URL and import all games into the Caissify database in the background.\n\nReturns immediately with `202 Accepted`. Poll `/import/caissify/pgn-url/status` to track progress.\n\nOnly one import job can run at a time.",
                    "operationId": "importCaissifyPgnUrl",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/PgnUrlImportRequest" },
                                "example": {
                                    "url": "https://m224.sync.com/u/OMOTB202602PGN.pgn?...",
                                    "cookie": "sync_auth=...; signature=..."
                                }
                            }
                        }
                    },
                    "responses": {
                        "202": { "description": "Import job started in the background" },
                        "409": { "description": "An import is already running" }
                    }
                }
            },

            "/import/caissify/pgn-url/status": {
                "get": {
                    "tags": ["Import"],
                    "summary": "Check bulk import status",
                    "description": "Returns the current status of the background PGN import job.",
                    "operationId": "getCaissifyImportStatus",
                    "responses": {
                        "200": {
                            "description": "Import status",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ImportStatus" }
                                }
                            }
                        }
                    }
                }
            },

            "/import/caissify": {
                "put": {
                    "tags": ["Import"],
                    "summary": "Import a single Caissify game",
                    "description": "Import a single game into the Caissify database. Accepts any game regardless of rating.",
                    "operationId": "importCaissifySingle",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "multipart/form-data": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "pgn": { "type": "string", "description": "PGN content of the game" }
                                    },
                                    "required": ["pgn"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Game imported successfully" },
                        "400": bad_request_response()
                    }
                }
            },

            "/import/masters": {
                "put": {
                    "tags": ["Import"],
                    "summary": "Import a single Masters game",
                    "description": "Import a single game into the Masters database. Requires average rating ≥ 2200.",
                    "operationId": "importMastersSingle",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "multipart/form-data": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "pgn": { "type": "string", "description": "PGN content of the game" }
                                    },
                                    "required": ["pgn"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Game imported successfully" },
                        "400": bad_request_response()
                    }
                }
            },

            // ── /fide ──────────────────────────────────────────────────────
            "/fide/player/{fide_id}": {
                "get": {
                    "tags": ["FIDE"],
                    "summary": "Get FIDE player profile",
                    "description": "Returns profile information for a FIDE player: name, country, title, sex, birth year, and activity flag.",
                    "operationId": "getFidePlayer",
                    "parameters": [fide_id_param()],
                    "responses": {
                        "200": {
                            "description": "FIDE player profile",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/FidePlayer" }
                                }
                            }
                        },
                        "404": not_found_response()
                    }
                }
            },

            "/fide/player/{fide_id}/ratings": {
                "get": {
                    "tags": ["FIDE"],
                    "summary": "Get FIDE rating history",
                    "description": "Returns monthly standard/rapid/blitz rating snapshots for a FIDE player.",
                    "operationId": "getFidePlayerRatings",
                    "parameters": (
                        vec![fide_id_param()]
                            .into_iter()
                            .chain(vec![
                                param("since", false, "string", Some("Start month inclusive (YYYY-MM)"), Some(json!("2020-01"))),
                                param("until", false, "string", Some("End month inclusive (YYYY-MM)"),   Some(json!("2026-03"))),
                            ])
                            .collect::<Vec<_>>()
                    ),
                    "responses": {
                        "200": {
                            "description": "Rating history array",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/FideRatingSnapshot" }
                                    }
                                }
                            }
                        },
                        "404": not_found_response()
                    }
                }
            },

            "/fide/search": {
                "get": {
                    "tags": ["FIDE"],
                    "summary": "Search FIDE players by name",
                    "description": "Prefix-match search over the in-memory FIDE name index. Returns up to `limit` matching player profiles. Normalised tokens are used so \"Carlsen\" matches \"Carlsen, Magnus\" and vice-versa.",
                    "operationId": "searchFidePlayers",
                    "parameters": [
                        param("name",  true,  "string",  Some("Name prefix to search for (case-insensitive)"), None),
                        param("limit", false, "integer", Some("Maximum results to return (default 10, max 50)"), Some(json!(10)))
                    ],
                    "responses": {
                        "200": {
                            "description": "Matching FIDE players",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/FideSearchEntry" }
                                    }
                                }
                            }
                        },
                        "400": bad_request_response()
                    }
                }
            },

            // ── Import ────────────────────────────────────────────────────
            "/import/fide": {
                "put": {
                    "tags": ["Import"],
                    "summary": "Import FIDE player batch",
                    "description": "Import a batch of FIDE player profiles and rating snapshots for a given month. Called by the `import-fide` binary after parsing the FIDE standard rating list XML.",
                    "operationId": "importFide",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/FideImportBatch" },
                                "example": {
                                    "month": "2026-03",
                                    "players": [{ "fide_id": 1503014, "name": "Carlsen, Magnus", "country": "NOR", "standard": 2833 }]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Number of FIDE records imported" },
                        "400": bad_request_response()
                    }
                }
            },

            "/import/fide/refresh": {
                "post": {
                    "tags": ["Import"],
                    "summary": "Trigger FIDE rating list refresh",
                    "description": "Downloads the current FIDE standard rating list immediately (same as the automatic ~32-day background task). Useful after initial deployment or to force an early update. Response is the number of players imported.",
                    "operationId": "refreshFide",
                    "responses": {
                        "200": { "description": "Number of FIDE players imported" },
                        "500": { "description": "Download or parse failed — check server logs" }
                    }
                }
            },

            "/import/caissify/reindex": {
                "post": {
                    "tags": ["Import"],
                    "summary": "Rebuild game metadata index",
                    "description": "Backfills the `caissify_game_meta` and `caissify_game_by_date` column families from the existing `caissify_game` data. Safe to run multiple times. Progress is logged server-side.",
                    "operationId": "caissifyReindex",
                    "responses": {
                        "200": { "description": "Number of games reindexed" },
                        "500": { "description": "Reindex failed — check server logs" }
                    }
                }
            },

            "/import/caissify/fide-link": {
                "post": {
                    "tags": ["Import"],
                    "summary": "Batch-link games to FIDE player IDs",
                    "description": "Scans existing Caissify games in order and writes entries into the `caissify_game_by_fide` index for any game whose players match a known FIDE profile.\n\nThe scan is cursor-resumable: pass the `next_cursor` from the previous response to continue where you left off after a restart or partial run.\n\nSafe to call multiple times; already-linked entries are overwritten with the same value.",
                    "operationId": "caissifyFideLink",
                    "parameters": [
                        param("batch",  false, "integer", Some("Number of games to process per call (default 5000, max 100000)"), Some(json!(5000))),
                        param("cursor", false, "string",  Some("Opaque hex cursor from a previous response to resume from a specific position"), None)
                    ],
                    "responses": {
                        "200": {
                            "description": "Link pass result",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/FideLinkResponse" }
                                }
                            }
                        },
                        "500": { "description": "Link pass failed — check server logs" }
                    }
                }
            },

            // ── Admin ─────────────────────────────────────────────────────
            "/monitor": {
                "get": {
                    "tags": ["Admin"],
                    "summary": "Get metrics (InfluxDB line protocol)",
                    "description": "Returns server and database metrics in InfluxDB line protocol format. Key fields include `caissify_game`, `masters`, `lichess`, cache hit ratios, and RocksDB block cache stats.",
                    "operationId": "getMonitor",
                    "responses": {
                        "200": {
                            "description": "InfluxDB line protocol metrics",
                            "content": { "text/plain": { "schema": { "type": "string" } } }
                        }
                    }
                }
            },

            "/import/openings": {
                "post": {
                    "tags": ["Admin"],
                    "summary": "Refresh opening names",
                    "description": "Re-downloads and refreshes opening names from the chess-openings dataset. Also runs automatically every ~167 minutes.",
                    "operationId": "importOpenings",
                    "responses": {
                        "200": { "description": "Opening names refreshed" }
                    }
                }
            },

            "/compact": {
                "post": {
                    "tags": ["Admin"],
                    "summary": "Trigger RocksDB compaction",
                    "description": "Triggers a full manual RocksDB compaction. This is slow and should only be run during maintenance windows.",
                    "operationId": "compact",
                    "responses": {
                        "200": { "description": "Compaction triggered" }
                    }
                }
            }
        },

        "components": {
            "schemas": {

                "ExplorerResponse": {
                    "type": "object",
                    "description": "Opening statistics for a position",
                    "properties": {
                        "white":    { "type": "integer", "description": "Number of games won by White" },
                        "draws":    { "type": "integer", "description": "Number of drawn games" },
                        "black":    { "type": "integer", "description": "Number of games won by Black" },
                        "moves":    { "type": "array", "items": { "$ref": "#/components/schemas/ExplorerMove" }, "description": "Legal moves from this position with their statistics" },
                        "topGames": { "type": "array", "items": { "$ref": "#/components/schemas/ExplorerGameWithMove" }, "description": "Highest-rated games reaching this position" },
                        "recentGames": { "type": "array", "items": { "$ref": "#/components/schemas/ExplorerGameWithMove" }, "description": "Most recent games reaching this position" },
                        "opening":  { "$ref": "#/components/schemas/Opening" },
                        "queuePosition": { "type": "integer", "description": "Player indexer queue position (player endpoint only)" },
                        "history":  { "$ref": "#/components/schemas/HistoryResponse" }
                    },
                    "required": ["white", "draws", "black", "moves"]
                },

                "ExplorerMove": {
                    "type": "object",
                    "description": "A legal move from the current position with opening statistics",
                    "properties": {
                        "uci":     { "type": "string", "description": "Move in UCI format", "example": "e2e4" },
                        "san":     { "type": "string", "description": "Move in SAN format", "example": "e4" },
                        "white":   { "type": "integer" },
                        "draws":   { "type": "integer" },
                        "black":   { "type": "integer" },
                        "averageRating":         { "type": "integer", "description": "Average rating of the player making this move" },
                        "averageOpponentRating": { "type": "integer", "description": "Average rating of the opponent" },
                        "performance":           { "type": "integer", "description": "Performance rating for this move" },
                        "game":    { "$ref": "#/components/schemas/ExplorerGame" },
                        "opening": { "$ref": "#/components/schemas/Opening" }
                    },
                    "required": ["uci", "san", "white", "draws", "black"]
                },

                "ExplorerGame": {
                    "type": "object",
                    "description": "Metadata for a single game",
                    "properties": {
                        "id":     { "type": "string", "description": "8-character base-62 game ID", "example": "AbCd1234" },
                        "winner": { "type": "string", "nullable": true, "enum": ["white", "black"], "description": "Winner, or null for draw" },
                        "speed":  { "type": "string", "enum": ["ultraBullet","bullet","blitz","rapid","classical","correspondence"], "description": "Game speed (Lichess only)" },
                        "mode":   { "type": "string", "enum": ["rated","casual"], "description": "Game mode (Lichess only)" },
                        "white":  { "$ref": "#/components/schemas/GamePlayer" },
                        "black":  { "$ref": "#/components/schemas/GamePlayer" },
                        "year":   { "type": "integer", "example": 2024 },
                        "month":  { "type": "string", "description": "YYYY-MM (Lichess only)", "example": "2024-05" }
                    },
                    "required": ["id", "winner", "white", "black", "year"]
                },

                "ExplorerGameWithMove": {
                    "type": "object",
                    "allOf": [{ "$ref": "#/components/schemas/ExplorerGame" }],
                    "properties": {
                        "uci": { "type": "string", "description": "The move played in this position (UCI)", "example": "e2e4" }
                    },
                    "required": ["uci"]
                },

                "GamePlayer": {
                    "type": "object",
                    "properties": {
                        "name":   { "type": "string", "example": "Kasparov, Garry" },
                        "rating": { "type": "integer", "example": 2800 }
                    },
                    "required": ["name", "rating"]
                },

                "Opening": {
                    "type": "object",
                    "description": "ECO opening classification",
                    "properties": {
                        "eco":  { "type": "string", "example": "B20" },
                        "name": { "type": "string", "example": "Sicilian Defense" }
                    },
                    "required": ["eco", "name"]
                },

                "HistoryResponse": {
                    "type": "array",
                    "description": "Per-month game counts",
                    "items": {
                        "type": "object",
                        "properties": {
                            "month": { "type": "string", "example": "2024-01" },
                            "black": { "type": "integer" },
                            "draws": { "type": "integer" },
                            "white": { "type": "integer" }
                        },
                        "required": ["month", "black", "draws", "white"]
                    }
                },

                "ImportStatus": {
                    "oneOf": [
                        {
                            "type": "object",
                            "title": "Idle",
                            "properties": { "status": { "type": "string", "enum": ["idle"] } },
                            "required": ["status"]
                        },
                        {
                            "type": "object",
                            "title": "Running",
                            "properties": {
                                "status":           { "type": "string", "enum": ["running"] },
                                "games_imported":   { "type": "integer", "example": 4548806 },
                                "games_skipped":    { "type": "integer", "example": 61194 },
                                "bytes_downloaded": { "type": "integer", "example": 9643684724_u64 }
                            },
                            "required": ["status", "games_imported", "games_skipped", "bytes_downloaded"]
                        },
                        {
                            "type": "object",
                            "title": "Done",
                            "properties": {
                                "status":          { "type": "string", "enum": ["done"] },
                                "games_imported":  { "type": "integer" },
                                "games_skipped":   { "type": "integer" },
                                "elapsed_secs":    { "type": "number" }
                            },
                            "required": ["status", "games_imported", "games_skipped", "elapsed_secs"]
                        },
                        {
                            "type": "object",
                            "title": "Failed",
                            "properties": {
                                "status": { "type": "string", "enum": ["failed"] },
                                "error":  { "type": "string" }
                            },
                            "required": ["status", "error"]
                        }
                    ]
                },

                "PgnUrlImportRequest": {
                    "type": "object",
                    "description": "Parameters for a bulk PGN import from a remote URL",
                    "properties": {
                        "url":    { "type": "string", "description": "Direct download URL for the PGN file" },
                        "cookie": { "type": "string", "description": "Cookie header value to send with the download request (e.g. for Sync.com authenticated links)" }
                    },
                    "required": ["url", "cookie"]
                },

                "CaissifyGameMeta": {
                    "type": "object",
                    "description": "Compact per-game metadata stored in caissify_game_meta CF",
                    "properties": {
                        "year":           { "type": "integer", "example": 2024 },
                        "white_rating":   { "type": "integer", "example": 2800 },
                        "black_rating":   { "type": "integer", "example": 2750 },
                        "result":         { "type": "string", "enum": ["white", "draw", "black"] },
                        "white_fide_id":  { "type": "integer", "nullable": true, "description": "FIDE ID of the White player, omitted when not yet linked" },
                        "black_fide_id":  { "type": "integer", "nullable": true, "description": "FIDE ID of the Black player, omitted when not yet linked" }
                    },
                    "required": ["year", "white_rating", "black_rating", "result"]
                },

                "CaissifyGameListEntry": {
                    "type": "object",
                    "description": "One entry in a paginated game list, containing full PGN header fields",
                    "properties": {
                        "id":            { "type": "string",  "description": "8-character base-62 game ID", "example": "AbCd1234" },
                        "white":         { "type": "string",  "description": "White player name", "example": "Carlsen, Magnus" },
                        "white_rating":  { "type": "integer", "description": "White player Elo rating (0 = unknown)", "example": 2800 },
                        "black":         { "type": "string",  "description": "Black player name", "example": "Nepomniachtchi, Ian" },
                        "black_rating":  { "type": "integer", "description": "Black player Elo rating (0 = unknown)", "example": 2750 },
                        "event":         { "type": "string",  "description": "Event name", "example": "Candidates 2024" },
                        "site":          { "type": "string",  "description": "Site or location", "example": "Toronto CAN" },
                        "date":          { "type": "string",  "description": "Full PGN date (YYYY.MM.DD or partial)", "example": "2024.04.04" },
                        "round":         { "type": "string",  "description": "Round number or label", "example": "1" },
                        "result":        { "type": "string",  "enum": ["white", "draw", "black"], "description": "Game result from White's perspective" },
                        "white_fide_id": { "type": "integer", "nullable": true, "description": "White player FIDE ID (absent if unlinked)" },
                        "black_fide_id": { "type": "integer", "nullable": true, "description": "Black player FIDE ID (absent if unlinked)" }
                    },
                    "required": ["id", "white", "white_rating", "black", "black_rating", "event", "site", "date", "round", "result"]
                },

                "CaissifyGameListResponse": {
                    "type": "object",
                    "description": "Cursor-paginated list of Caissify games",
                    "properties": {
                        "games":           { "type": "array", "items": { "$ref": "#/components/schemas/CaissifyGameListEntry" } },
                        "next_page_token": { "type": "string", "nullable": true, "description": "Opaque hex cursor; absent on the last page", "example": "0007d00000001234" }
                    },
                    "required": ["games"]
                },

                "FidePlayer": {
                    "type": "object",
                    "description": "FIDE player profile. The rating/games/k fields are the most recent known snapshot, injected at query time (absent when no snapshot has been imported yet).",
                    "properties": {
                        "fide_id":         { "type": "integer", "example": 1503014 },
                        "name":            { "type": "string",  "example": "Carlsen, Magnus" },
                        "country":         { "type": "string",  "example": "NOR" },
                        "sex":             { "type": "string",  "example": "M" },
                        "title":           { "type": "string",  "example": "GM",  "description": "Open title (GM, IM, FM, CM, NM)" },
                        "w_title":         { "type": "string",  "example": "WGM", "description": "Women's title; empty string when not held" },
                        "o_title":         { "type": "string",  "example": "",   "description": "FIDE Online Arena title; empty string when not held" },
                        "foa_title":       { "type": "string",  "example": "",   "description": "FOA title; empty string when not held" },
                        "birth_year":      { "type": "integer", "example": 1990 },
                        "flag":            { "type": "string",  "enum": ["Active", "Inactive", "Unknown"] },
                        "rating_standard": { "type": "integer", "nullable": true, "example": 2833 },
                        "games_standard":  { "type": "integer", "example": 0 },
                        "k_standard":      { "type": "integer", "example": 10 },
                        "rating_rapid":    { "type": "integer", "nullable": true, "example": 2850 },
                        "games_rapid":     { "type": "integer", "example": 0 },
                        "k_rapid":         { "type": "integer", "example": 20 },
                        "rating_blitz":    { "type": "integer", "nullable": true, "example": 2886 },
                        "games_blitz":     { "type": "integer", "example": 0 },
                        "k_blitz":         { "type": "integer", "example": 20 }
                    },
                    "required": ["fide_id", "name", "country", "flag"]
                },

                "FideRatingSnapshot": {
                    "type": "object",
                    "description": "Monthly FIDE rating snapshot",
                    "properties": {
                        "month":     { "type": "string",  "example": "2026-03" },
                        "standard":  { "type": "integer", "nullable": true, "example": 2833 },
                        "rapid":     { "type": "integer", "nullable": true, "example": 2850 },
                        "blitz":     { "type": "integer", "nullable": true, "example": 2886 }
                    },
                    "required": ["month"]
                },

                "FideSearchEntry": {
                    "type": "object",
                    "description": "A single FIDE player match returned by GET /fide/search",
                    "properties": {
                        "fide_id":  { "type": "integer", "example": 1503014 },
                        "name":     { "type": "string",  "example": "Carlsen, Magnus" },
                        "country": { "type": "string",  "example": "NOR" },
                        "title":   { "type": "string",  "example": "GM", "description": "Highest open title; empty string when none" },
                        "rating":  { "type": "integer", "nullable": true, "example": 2833, "description": "Most recent standard rating; absent when unrated" }
                    },
                    "required": ["fide_id", "name", "country"]
                },

                "FideLinkResponse": {
                    "type": "object",
                    "description": "Result of a POST /import/caissify/fide-link batch run",
                    "properties": {
                        "linked":      { "type": "integer", "description": "Number of player-game links written in this batch", "example": 8742 },
                        "skipped":     { "type": "integer", "description": "Number of games whose players could not be resolved to a FIDE ID", "example": 312 },
                        "next_cursor": { "type": "string",  "nullable": true, "description": "Hex cursor to resume from on the next call; absent when the scan has reached the end of the database", "example": "0000deadbeef" }
                    },
                    "required": ["linked", "skipped"]
                },

                "FideImportBatch": {
                    "type": "object",
                    "description": "Batch of FIDE players and rating snapshots for a given month",
                    "properties": {
                        "month":   { "type": "string", "description": "YYYY-MM of this rating list", "example": "2026-03" },
                        "players": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "fide_id":       { "type": "integer" },
                                    "name":          { "type": "string" },
                                    "country":       { "type": "string" },
                                    "sex":           { "type": "string" },
                                    "title":         { "type": "string", "description": "Open title (GM, IM, FM, CM, NM)" },
                                    "w_title":       { "type": "string", "description": "Women's title (WGM, WIM, WFM, WCM)" },
                                    "o_title":       { "type": "string", "description": "FIDE Online Arena title" },
                                    "foa_title":     { "type": "string", "description": "FOA title" },
                                    "birth_year":    { "type": "integer" },
                                    "flag":          { "type": "string" },
                                    "standard":      { "type": "integer" },
                                    "rapid":         { "type": "integer" },
                                    "blitz":         { "type": "integer" },
                                    "games_standard":{ "type": "integer" },
                                    "games_rapid":   { "type": "integer" },
                                    "games_blitz":   { "type": "integer" },
                                    "k_standard":    { "type": "integer" },
                                    "k_rapid":       { "type": "integer" },
                                    "k_blitz":       { "type": "integer" }
                                },
                                "required": ["fide_id", "name", "country"]
                            }
                        }
                    },
                    "required": ["month", "players"]
                }
            }
        }
    })
}

// ── Parameter helpers ─────────────────────────────────────────────────────────

fn param(
    name: &str,
    required: bool,
    typ: &str,
    description: Option<&str>,
    example: Option<Value>,
) -> Value {
    let mut p = json!({ "name": name, "in": "query", "required": required, "schema": { "type": typ } });
    if let Some(d) = description {
        p["description"] = json!(d);
    }
    if let Some(e) = example {
        p["example"] = e;
    }
    p
}

fn position_params() -> Vec<Value> {
    vec![
        param("fen",     false, "string", Some("FEN of the starting position (defaults to the standard starting position)"), Some(json!("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1"))),
        param("play",    false, "string", Some("Comma-separated UCI moves to play from the FEN"), Some(json!("e2e4,e7e5"))),
        param("variant", false, "string", Some("Game variant (chess, chess960, crazyhouse, antichess, atomic, horde, kingOfTheHill, racingKings, threeCheck)"), Some(json!("chess"))),
    ]
}

fn year_range_params() -> Vec<Value> {
    vec![
        param("since", false, "integer", Some("Only include games from this year onwards"), Some(json!(1990))),
        param("until", false, "integer", Some("Only include games up to this year"), Some(json!(2025))),
    ]
}

fn explorer_limit_params() -> Vec<Value> {
    vec![
        param("moves",    false, "integer", Some("Max number of moves to return (default 12)"), Some(json!(12))),
        param("topGames", false, "integer", Some("Max number of top games to return (default 15)"), Some(json!(15))),
    ]
}

fn player_params() -> Vec<Value> {
    vec![
        json!({ "name": "player", "in": "query", "required": true,  "schema": { "type": "string" }, "description": "Lichess username", "example": "DrNykterstein" }),
        json!({ "name": "color",  "in": "query", "required": true,  "schema": { "type": "string", "enum": ["white", "black"] }, "description": "Player's color" }),
    ]
    .into_iter()
    .chain(position_params())
    .chain(vec![
        param("speeds",      false, "string",  Some("Comma-separated speed filter: ultraBullet,bullet,blitz,rapid,classical,correspondence"), Some(json!("blitz,rapid"))),
        param("modes",       false, "string",  Some("Comma-separated mode filter: rated,casual"), Some(json!("rated"))),
        param("since",       false, "string",  Some("Only include games since this month (YYYY-MM)"), Some(json!("2020-01"))),
        param("until",       false, "string",  Some("Only include games until this month (YYYY-MM)"), Some(json!("2025-12"))),
        param("moves",       false, "integer", Some("Max number of moves to return (default unlimited)"), Some(json!(12))),
        param("recentGames", false, "integer", Some("Max number of recent games to return"), Some(json!(4))),
    ])
    .collect()
}

fn lichess_params() -> Vec<Value> {
    position_params().into_iter()
        .chain(vec![
            param("speeds",      false, "string",  Some("Comma-separated speed filter: ultraBullet,bullet,blitz,rapid,classical,correspondence"), Some(json!("blitz,rapid"))),
            param("ratings",     false, "string",  Some("Comma-separated rating group filter: 1000,1200,1400,1600,1800,2000,2200,2500"), Some(json!("2000,2200,2500"))),
            param("since",       false, "string",  Some("Only include games since this month (YYYY-MM)"), Some(json!("2020-01"))),
            param("until",       false, "string",  Some("Only include games until this month (YYYY-MM)"), Some(json!("2025-12"))),
            param("moves",       false, "integer", Some("Max number of moves to return (default 12)"), Some(json!(12))),
            param("topGames",    false, "integer", Some("Max number of top games (default 4)"), Some(json!(4))),
            param("recentGames", false, "integer", Some("Max number of recent games (default 4)"), Some(json!(4))),
            param("history",     false, "boolean", Some("Include per-month game history in the response"), Some(json!(false))),
        ])
        .collect()
}

fn game_id_param() -> Value {
    json!({
        "name": "id",
        "in": "path",
        "required": true,
        "schema": { "type": "string", "minLength": 8, "maxLength": 8 },
        "description": "8-character base-62 game ID",
        "example": "AbCd1234"
    })
}

fn fide_id_param() -> Value {
    json!({
        "name": "fide_id",
        "in": "path",
        "required": true,
        "schema": { "type": "integer" },
        "description": "FIDE ID",
        "example": 1503014
    })
}

fn explorer_response_200(description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": { "$ref": "#/components/schemas/ExplorerResponse" },
                "example": {
                    "white": 14820,
                    "draws": 8930,
                    "black": 6250,
                    "moves": [
                        {
                            "uci": "e7e5", "san": "e5",
                            "white": 7100, "draws": 4200, "black": 3000,
                            "averageRating": 2650
                        }
                    ],
                    "topGames": [],
                    "opening": { "eco": "C20", "name": "King's Pawn Game" }
                }
            }
        }
    })
}

fn pgn_200_response() -> Value {
    json!({
        "description": "PGN text of the game",
        "content": { "application/x-chess-pgn": { "schema": { "type": "string" } } }
    })
}

fn bad_request_response() -> Value {
    json!({ "description": "Invalid FEN, illegal move sequence, or unknown variant" })
}

fn not_found_response() -> Value {
    json!({ "description": "Game not found" })
}
