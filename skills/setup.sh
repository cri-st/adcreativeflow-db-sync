#!/usr/bin/env bash
#
# Setup script for AI coding assistant integrations
# Creates symlinks for skills directory across multiple AI tools
#

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Help message
show_help() {
    echo "Usage: ./skills/setup.sh [--help]"
    echo ""
    echo "Creates symlinks for AI coding assistant integrations:"
    echo "  - Claude Code / Claude Desktop (.claude/skills)"
    echo "  - OpenCode (.claude/skills - uses same path as Claude)"
    echo "  - Gemini CLI (.gemini/skills)"
    echo "  - Codex (.codex/skills)"
    echo "  - Cursor IDE (.cursor/skills)"
    echo "  - GitHub Copilot (.github/copilot-instructions.md)"
    echo ""
    echo "Options:"
    echo "  --help    Show this help message"
}

# Create symlink if it doesn't exist
create_symlink() {
    local target_dir="$1"
    local link_name="$2"
    local full_path="$PROJECT_ROOT/$target_dir/$link_name"
    local target="../skills"

    # Create parent directory if needed
    mkdir -p "$PROJECT_ROOT/$target_dir"

    if [ -L "$full_path" ]; then
        echo -e "${YELLOW}[SKIP]${NC} $target_dir/$link_name already exists"
    elif [ -e "$full_path" ]; then
        echo -e "${YELLOW}[SKIP]${NC} $target_dir/$link_name exists (not a symlink)"
    else
        ln -s "$target" "$full_path"
        echo -e "${GREEN}[OK]${NC} Created $target_dir/$link_name -> skills"
    fi
}

# Copy AGENTS.md for Copilot
setup_copilot() {
    local target="$PROJECT_ROOT/.github/copilot-instructions.md"
    local source="$PROJECT_ROOT/AGENTS.md"

    mkdir -p "$PROJECT_ROOT/.github"

    if [ -f "$target" ]; then
        echo -e "${YELLOW}[SKIP]${NC} .github/copilot-instructions.md already exists"
    else
        cp "$source" "$target"
        echo -e "${GREEN}[OK]${NC} Copied AGENTS.md -> .github/copilot-instructions.md"
    fi
}

# Main
main() {
    # Check for help flag
    if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
        show_help
        exit 0
    fi

    echo "Setting up AI coding assistant integrations..."
    echo ""

    # Create symlinks for each tool
    create_symlink ".claude" "skills"    # Claude Code / Claude Desktop / OpenCode
    create_symlink ".gemini" "skills"    # Gemini CLI
    create_symlink ".codex" "skills"     # Codex
    create_symlink ".cursor" "skills"    # Cursor IDE

    # Setup GitHub Copilot (file copy, not symlink)
    setup_copilot

    echo ""
    echo "Setup complete!"
}

main "$@"
