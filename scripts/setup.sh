#!/usr/bin/env bash
# ============================================================
# Analytics MCP Server — one-command setup
# Usage: bash scripts/setup.sh
# ============================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_NAME="analytics_mcp"
VENV_DIR="$PROJECT_DIR/.venv"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   Analytics MCP Server — Setup           ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── 1. Python virtual environment ─────────────────────────────
echo "▶ Creating Python virtual environment…"
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$PROJECT_DIR/requirements.txt"
echo "  ✓ Python env ready"

# ── 2. .env file ──────────────────────────────────────────────
if [ ! -f "$PROJECT_DIR/.env" ]; then
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
    echo "  ✓ .env created from .env.example"
else
    echo "  ✓ .env already exists"
fi

# ── 3. PostgreSQL database ────────────────────────────────────
echo "▶ Setting up PostgreSQL database '$DB_NAME'…"

# Check postgres is running (Postgres.app on macOS)
if ! pg_isready -q 2>/dev/null; then
    echo "  ⚠ PostgreSQL not running. Please start Postgres.app and re-run."
    exit 1
fi

# Create DB if it doesn't exist
if psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "  ✓ Database '$DB_NAME' already exists"
else
    createdb "$DB_NAME"
    echo "  ✓ Database '$DB_NAME' created"
fi

# ── 4. Schema + seed data ─────────────────────────────────────
echo "▶ Applying schema…"
psql "$DB_NAME" -f "$PROJECT_DIR/sql/01_schema.sql" -q
echo "  ✓ Schema applied"

echo "▶ Seeding data (90 days of analytics)…"
psql "$DB_NAME" -f "$PROJECT_DIR/sql/02_seed.sql" -q
echo "  ✓ Seed data inserted"

# ── 5. Register MCP server with Claude Code ───────────────────
echo "▶ Registering MCP server with Claude Code…"
CLAUDE_CONFIG="$HOME/.claude.json"
SERVER_PATH="$VENV_DIR/bin/python"
SCRIPT_PATH="$PROJECT_DIR/server.py"

# Add to claude.json if it exists (non-destructive)
if [ -f "$CLAUDE_CONFIG" ]; then
    python3 - <<PYEOF
import json, sys
with open("$CLAUDE_CONFIG") as f:
    config = json.load(f)
mcp = config.setdefault("mcpServers", {})
mcp["analytics-mcp"] = {
    "command": "$SERVER_PATH",
    "args": ["$SCRIPT_PATH"],
    "env": {"DATABASE_URL": "postgresql://localhost:5432/$DB_NAME"}
}
with open("$CLAUDE_CONFIG", "w") as f:
    json.dump(config, f, indent=2)
print("  ✓ MCP server registered in ~/.claude.json")
PYEOF
else
    echo "  ℹ Claude Code config not found — add manually (see README)"
fi

# ── Done ──────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   ✓ Setup complete!                      ║"
echo "╚══════════════════════════════════════════╝"
echo ""
echo "  Start the server:  source .venv/bin/activate && python server.py"
echo "  Test it:           python tests/test_server.py"
echo ""
