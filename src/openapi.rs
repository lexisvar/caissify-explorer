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
