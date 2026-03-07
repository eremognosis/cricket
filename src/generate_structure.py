# =========================================================
# FILE DESCRIPTION : `generate_structure.py`
#
# Generates a condensed project directory structure showing file organization
# without overwhelming output. Groups similar files and shows summaries.
#
# WRITTEN BY : RAJ
# Last UPDATED : 2026-03-07
# =========================================================


# ===== IMPORTS =====
import os
from datetime import datetime
from collections import defaultdict
# ===================


# ===== CONFIG =====
ROOT_DIR = "./"
OUTPUT_FILE = "STRUCTURE.md"
MAX_SHOW_PER_TYPE = 2  # Show this many files of same type before summarizing
MAX_SHOW_SUBDIRS = 10  # Only summarize if more than this many subdirs (for data-heavy folders)
SHOW_ALL_FILES_IN = {'src', 'logs', 'pyscripts', 'dbtcrick'}  # Always show all files in these directories
IGNORE_DIRS = {'.git', '__pycache__', '.idea','.venv','pyscripts' ,'venv', 'node_modules', '.dbt', 'target'}
IGNORE_FILES = {'.DS_Store', 'Thumbs.db', '.gitkeep'}
# ===================


def get_file_extension(filename):
    """Extract file extension including the dot, or return the filename if no extension."""
    if '.' in filename and not filename.startswith('.'):
        return os.path.splitext(filename)[1]
    return None


def group_files_by_extension(files):
    """Group files by extension and return a dict."""
    grouped = defaultdict(list)
    for f in files:
        ext = get_file_extension(f)
        if ext:
            grouped[ext].append(f)
        else:
            grouped['_no_ext'].append(f)
    return grouped


def format_directory_tree(root_path, prefix="", is_last=True, parent_name=""):
    """
    Recursively build a tree structure string with file summarization.
    
    Args:
        root_path: Current directory path
        prefix: Prefix for tree drawing characters
        is_last: Whether this is the last item in parent directory
        parent_name: Name to display for this directory
    """
    lines = []
    
    # Skip ignored directories
    if os.path.basename(root_path) in IGNORE_DIRS:
        return lines
    
    # Add current directory name
    if parent_name:
        connector = "└── " if is_last else "├── "
        lines.append(f"{prefix}{connector}{parent_name}/")
        extension = "    " if is_last else "│   "
        prefix = prefix + extension
    
    try:
        items = os.listdir(root_path)
    except PermissionError:
        return lines
    
    # Filter out ignored items
    items = [i for i in items if i not in IGNORE_FILES and i not in IGNORE_DIRS]
    
    # Separate directories and files
    dirs = sorted([i for i in items if os.path.isdir(os.path.join(root_path, i))])
    files = sorted([i for i in items if os.path.isfile(os.path.join(root_path, i))])
    
    # Group files by extension
    grouped_files = group_files_by_extension(files)
    
    # Check if this directory should show all files
    current_dir_name = os.path.basename(root_path)
    show_all_files = current_dir_name in SHOW_ALL_FILES_IN
    
    # Determine what to show
    display_items = []
    
    # Add files (with summarization, unless in special dir)
    for ext, file_list in sorted(grouped_files.items()):
        if not show_all_files and len(file_list) > MAX_SHOW_PER_TYPE:
            # Show first MAX_SHOW_PER_TYPE files, then summarize
            for f in file_list[:MAX_SHOW_PER_TYPE]:
                display_items.append(('file', f))
            remaining = len(file_list) - MAX_SHOW_PER_TYPE
            ext_display = ext if ext != '_no_ext' else 'files'
            summary = f"({remaining} more {ext_display} files)" if remaining > 1 else f"(1 more {ext_display} file)"
            display_items.append(('summary', summary))
        else:
            # Show all files
            for f in file_list:
                display_items.append(('file', f))
    
    # Add subdirectories (with summarization)
    if len(dirs) > MAX_SHOW_SUBDIRS:
        # Show first MAX_SHOW_SUBDIRS directories, then summarize
        for d in dirs[:MAX_SHOW_SUBDIRS]:
            display_items.append(('dir', d))
        remaining = len(dirs) - MAX_SHOW_SUBDIRS
        summary = f"({remaining} more subdirectories)" if remaining > 1 else "(1 more subdirectory)"
        display_items.append(('summary', summary))
    else:
        # Show all subdirectories
        for d in dirs:
            display_items.append(('dir', d))
    
    # Render items
    total_items = len(display_items)
    for idx, (item_type, item_name) in enumerate(display_items):
        is_last_item = (idx == total_items - 1)
        
        if item_type == 'dir':
            # Recursively process subdirectory
            subdir_lines = format_directory_tree(
                os.path.join(root_path, item_name),
                prefix=prefix,
                is_last=is_last_item,
                parent_name=item_name
            )
            lines.extend(subdir_lines)
        elif item_type == 'file':
            connector = "└── " if is_last_item else "├── "
            lines.append(f"{prefix}{connector}{item_name}")
        elif item_type == 'summary':
            connector = "└── " if is_last_item else "├── "
            lines.append(f"{prefix}{connector}{item_name}")
    
    return lines


def generate_structure_file():
    """Generate the STRUCTURE.md file with current directory tree."""
    
    print(f"Generating structure for: {ROOT_DIR}")
    
    # Generate tree
    tree_lines = [f"{os.path.basename(ROOT_DIR)}/"]
    tree_lines.extend(format_directory_tree(ROOT_DIR, prefix=""))
    
    # Create markdown content
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = f"""# Project Directory Structure

Generated on: {timestamp}

```
{chr(10).join(tree_lines)}
```
"""
    
    # Write to file
    output_path = os.path.join(ROOT_DIR, OUTPUT_FILE)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✓ Structure written to {OUTPUT_FILE}")
    print(f"  Total lines: {len(tree_lines)}")


def main():
    generate_structure_file()


if __name__ == '__main__':
    main()
