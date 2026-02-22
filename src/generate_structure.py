"""
Generate and save the directory structure of the project.
"""
import os
from pathlib import Path
from datetime import datetime


def generate_tree(directory, prefix="", ignore_dirs=None, ignore_files=None, collapse_extensions=None, max_show=2):
    """
    Generate a tree structure of the directory.
    
    Args:
        directory: Path to the directory
        prefix: Prefix for the current line
        ignore_dirs: Set of directory names to ignore
        ignore_files: Set of file patterns to ignore
        collapse_extensions: Set of file extensions to collapse (e.g., {'.json', '.csv'})
        max_show: Maximum number of files to show for collapsed extensions
    
    Returns:
        List of strings representing the tree structure
    """
    if ignore_dirs is None:
        ignore_dirs = {'.git', '__pycache__', '.venv', 'venv', 'node_modules', 
                       '.dbt', 'target', 'dbt_packages', '.pytest_cache', 'rawdata'}
    
    if ignore_files is None:
        ignore_files = {'.pyc', '.pyo', '.pyd', '.so', '.dll', '.dylib'}
    
    if collapse_extensions is None:
        collapse_extensions = {'.json'}
    
    tree_lines = []
    directory = Path(directory)
    
    try:
        items = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
    except PermissionError:
        return tree_lines
    
    # Filter out ignored items and separate files from directories
    dirs = []
    files = []
    
    for item in items:
        if item.is_dir() and item.name in ignore_dirs:
            continue
        if any(item.name.endswith(ext) for ext in ignore_files):
            continue
        
        if item.is_dir():
            dirs.append(item)
        else:
            files.append(item)
    
    # Group files by extension for collapsing
    from collections import defaultdict
    files_by_ext = defaultdict(list)
    regular_files = []
    
    for file in files:
        ext = file.suffix.lower()
        if ext in collapse_extensions:
            files_by_ext[ext].append(file)
        else:
            regular_files.append(file)
    
    # Combine items: directories first, then regular files, then collapsed files
    all_items = []
    all_items.extend(dirs)
    all_items.extend(regular_files)
    
    # Add collapsed file groups
    for ext, ext_files in sorted(files_by_ext.items()):
        if len(ext_files) <= max_show:
            all_items.extend(ext_files)
        else:
            # Show first max_show files
            all_items.extend(ext_files[:max_show])
            # Add a placeholder for remaining files
            remaining = len(ext_files) - max_show
            all_items.append((ext, remaining))  # Tuple to mark as placeholder
    
    # Generate tree lines
    for i, item in enumerate(all_items):
        is_last = i == len(all_items) - 1
        current_prefix = "└── " if is_last else "├── "
        
        # Check if this is a placeholder tuple
        if isinstance(item, tuple):
            ext, count = item
            tree_lines.append(f"{prefix}{current_prefix}({count} more {ext} files)")
        elif item.is_dir():
            tree_lines.append(f"{prefix}{current_prefix}{item.name}/")
            extension_prefix = "    " if is_last else "│   "
            tree_lines.extend(
                generate_tree(item, prefix + extension_prefix, ignore_dirs, ignore_files, collapse_extensions, max_show)
            )
        else:
            tree_lines.append(f"{prefix}{current_prefix}{item.name}")
    
    return tree_lines


def save_structure(root_dir, output_file="STRUCTURE.md"):
    """
    Generate and save the directory structure to a markdown file.
    
    Args:
        root_dir: Root directory to analyze
        output_file: Output file name
    """
    root_dir = Path(root_dir)
    tree_lines = [root_dir.name + "/"]
    tree_lines.extend(generate_tree(root_dir))
    
    # Create markdown content
    content = "# Project Directory Structure\n\n"
    content += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    content += "```\n"
    content += "\n".join(tree_lines)
    content += "\n```\n"
    
    # Save to file
    output_path = root_dir / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Directory structure saved to: {output_path}")
    print("\nPreview:")
    print("\n".join(tree_lines))


if __name__ == "__main__":
    # Get the project root directory (parent of src)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    print(f"Analyzing directory: {project_root}")
    save_structure(project_root)
