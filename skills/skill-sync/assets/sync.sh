#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SKILLS_DIR="$PROJECT_ROOT/skills"

get_target_file() {
    case "$1" in
        root) echo "$PROJECT_ROOT/AGENTS.md" ;;
        src) echo "$PROJECT_ROOT/src/AGENTS.md" ;;
        ui) echo "$PROJECT_ROOT/ui/AGENTS.md" ;;
        *) echo "" ;;
    esac
}

extract_yaml_field() {
    awk -v field="$2" '
        /^---$/ { if (in_yaml) exit; in_yaml=1; next }
        in_yaml && $1 == field":" { gsub(/^[^:]+:[[:space:]]*/, ""); print; exit }
    ' "$1"
}

extract_yaml_list() {
    awk -v field="$2" '
        /^---$/ { if (in_yaml) exit; in_yaml=1; next }
        in_yaml && $1 == field":" { capturing=1; next }
        in_yaml && capturing && /^[[:space:]]*-[[:space:]]/ {
            gsub(/^[[:space:]]*-[[:space:]]*/, "")
            gsub(/^["'"'"'"]|["'"'"'"]$/, "")
            print
        }
        in_yaml && capturing && /^[[:space:]]*[a-z_]+:/ { exit }
    ' "$1"
}

update_section() {
    local target_file="$1"
    local marker_start="$2"
    local marker_end="$3"
    local content_file="$4"

    [ ! -f "$target_file" ] && return 1
    grep -q "$marker_start" "$target_file" || return 1

    local tmp_file=$(mktemp)
    local in_section=0
    
    while IFS= read -r line; do
        if [[ "$line" == *"$marker_start"* ]]; then
            echo "$line" >> "$tmp_file"
            cat "$content_file" >> "$tmp_file"
            in_section=1
        elif [[ "$line" == *"$marker_end"* ]]; then
            echo "$line" >> "$tmp_file"
            in_section=0
        elif [ $in_section -eq 0 ]; then
            echo "$line" >> "$tmp_file"
        fi
    done < "$target_file"
    
    mv "$tmp_file" "$target_file"
    echo "  [OK] $target_file"
}

echo "Syncing skill metadata to AGENTS.md files..."
echo ""

skills_table_file=$(mktemp)
echo "| Skill | Description | Link |" > "$skills_table_file"
echo "|-------|-------------|------|" >> "$skills_table_file"

for skill_dir in "$SKILLS_DIR"/*/; do
    skill_file="$skill_dir/SKILL.md"
    [ -f "$skill_file" ] || continue
    
    skill_name=$(extract_yaml_field "$skill_file" "name")
    skill_desc=$(extract_yaml_field "$skill_file" "description")
    
    [ -z "$skill_name" ] && continue
    
    echo "Processing: $skill_name"
    
    echo "| $skill_name | $skill_desc | [$skill_name](skills/$skill_name/SKILL.md) |" >> "$skills_table_file"
    
    while IFS= read -r scope; do
        [ -z "$scope" ] && continue
        target_file=$(get_target_file "$scope")
        [ -z "$target_file" ] && continue
        
        auto_invoke_file=$(mktemp)
        echo "| Action | Skill |" > "$auto_invoke_file"
        echo "|--------|-------|" >> "$auto_invoke_file"
        
        while IFS= read -r action; do
            [ -z "$action" ] && continue
            echo "| $action | $skill_name |" >> "$auto_invoke_file"
        done < <(extract_yaml_list "$skill_file" "auto_invoke")
        
        update_section "$target_file" "<!-- AUTO_INVOKE_START -->" "<!-- AUTO_INVOKE_END -->" "$auto_invoke_file"
        rm -f "$auto_invoke_file"
    done < <(extract_yaml_list "$skill_file" "scope")
done

echo ""
echo "Updating Skills Reference..."
update_section "$PROJECT_ROOT/AGENTS.md" "<!-- SKILLS_TABLE_START -->" "<!-- SKILLS_TABLE_END -->" "$skills_table_file"
rm -f "$skills_table_file"

echo ""
echo "Sync complete!"
