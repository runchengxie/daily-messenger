import argparse
import json
import logging
import os
from pathlib import Path
from typing import List, Optional, Set

# --- CONFIGURATION ---
try:
    # Assumes the script is in a 'tools' folder inside the project root
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # Fallback for interactive environments where __file__ is not defined
    PROJECT_ROOT = Path.cwd()

OUTPUT_FILENAME = "full_project_source.txt"

# --- EXCLUSION LISTS ---

# Directories to exclude if they appear ANYWHERE in the project structure.
EXCLUDE_DIRS_ANYWHERE: Set[str] = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    "cache",
    "outputs",
    ".vscode",
    ".idea",
    "venv",
    ".venv",
    "env",
    "build",
    "dist",
    "renv",
    "node_modules",
}

# Directories to exclude ONLY if they are in the project root directory.
# This allows keeping nested directories with the same name (e.g., 'src/app/data').
EXCLUDE_DIRS_ROOT_ONLY: Set[str] = {
    "data",  # User-specific data, not source code
    "project_tools",
    "tests",
    ".ruff_cache",    
}

# Directory name patterns to exclude (e.g., any directory ending with .egg-info).
EXCLUDE_DIR_PATTERNS: tuple[str, ...] = (".egg-info",)

# File extensions to exclude, typically for binary or non-source files.
EXCLUDE_EXTENSIONS: Set[str] = {
    ".pyc",
    ".pyo",
    ".so",
    ".dll",
    ".exe",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".svg",
    ".parquet",
    ".arrow",
    ".feather",
    ".csv",
    ".zip",
    ".gz",
    ".tar",
    ".rar",
    ".7z",
    ".db",
    ".sqlite3",
    ".pdf",
    ".docx",
    ".xlsx",
    ".swp",
    ".swo",
}

# Specific filenames to exclude. The chosen output file will be added at runtime
# to ensure it is not reprocessed on subsequent runs.
EXCLUDE_FILES: Set[str] = {
    OUTPUT_FILENAME,
    ".DS_Store",
    "Thumbs.db",
    "celerybeat-schedule",
    ".env",
    "uv.lock",
}


def process_notebook(filepath: Path) -> Optional[str]:
    """
    Parses a Jupyter Notebook (.ipynb) file, extracting only the code and
    markdown content while ignoring all cell outputs.
    """
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            notebook = json.load(f)

        content_parts: List[str] = []
        for i, cell in enumerate(notebook.get("cells", [])):
            cell_type = cell.get("cell_type")
            source_list = cell.get("source", [])

            # Ensure 'source' is a single string
            source = (
                "".join(source_list)
                if isinstance(source_list, list)
                else str(source_list)
            )

            if not source.strip():
                continue

            if cell_type == "code":
                content_parts.append(f"# --- Code Cell {i+1} ---\n{source}\n")
            elif cell_type == "markdown":
                content_parts.append(f"# --- Markdown Cell {i+1} ---\n{source}\n")

        return "\n".join(content_parts)
    except Exception as e:
        logging.warning("Could not parse notebook %s: %s", filepath.name, e)
        return None


def is_likely_text_file(filepath: Path) -> bool:
    """
    Checks if a file is likely to be a text file by checking its extension
    and sniffing the first 1024 bytes for null characters.
    """
    if filepath.suffix.lower() in EXCLUDE_EXTENSIONS:
        return False
    try:
        with open(filepath, "rb") as f:
            # If the first 1KB contains a null byte, it's likely a binary file.
            return b"\0" not in f.read(1024)
    except (IOError, PermissionError):
        return False


def combine_project_files(  # noqa: C901 - high complexity due to multiple nested checks
    project_root: Path = PROJECT_ROOT,
    output_filename: str = OUTPUT_FILENAME,
) -> None:
    """Scans the project directory, filters out unwanted files/directories,
    and combines all relevant source code into a single text file."""

    output_filepath = project_root / output_filename
    logging.info("Project root identified as: %s", project_root)
    logging.info("Output will be saved to: %s\n", output_filepath)

    files_processed_count = 0
    files_skipped_count = 0

    exclude_files = set(EXCLUDE_FILES)
    exclude_files.add(output_filename)

    try:
        with open(output_filepath, "w", encoding="utf-8", errors="replace") as outfile:
            outfile.write("--- Project Source Code Archive ---\n\n")
            outfile.write(
                "This file contains the concatenated source code of the project, "
                "with each file wrapped in tags indicating its relative path.\n\n"
            )

            for dirpath, dirnames, filenames in os.walk(project_root, topdown=True):
                current_path = Path(dirpath)

                # We filter 'dirnames' in-place
                # to prevent os.walk from recursing into them.
                original_dirs = list(dirnames)  # Make a copy to iterate over
                dirnames.clear()  # Clear the original list to rebuild it

                for d in original_dirs:
                    # Rule 1: Exclude if the directory name should be excluded anywhere.
                    if d in EXCLUDE_DIRS_ANYWHERE:
                        continue
                    # Rule 2: Exclude if it's a root-only-exclusion
                    # and we are at the root.
                    if d in EXCLUDE_DIRS_ROOT_ONLY and current_path == project_root:
                        continue
                    # Rule 3: Exclude if the directory name matches a pattern.
                    if any(d.endswith(p) for p in EXCLUDE_DIR_PATTERNS):
                        continue
                    # If all checks pass, add the directory back to be traversed.
                    dirnames.append(d)

                dirnames.sort()

                # --- FILE PROCESSING LOGIC ---
                for filename in sorted(filenames):
                    if filename in exclude_files:
                        continue

                    filepath = current_path / filename
                    relative_path_str = filepath.relative_to(project_root).as_posix()
                    content: Optional[str] = None

                    try:
                        # Step 1: Specifically handle Jupyter Notebooks.
                        if filepath.suffix.lower() == ".ipynb":
                            logging.info(
                                "  + Processing Notebook: %s", relative_path_str
                            )
                            content = process_notebook(filepath)
                        # Step 2: Handle general text files.
                        elif is_likely_text_file(filepath):
                            logging.info(
                                "  + Processing Text File: %s", relative_path_str
                            )
                            with open(
                                filepath, "r", encoding="utf-8", errors="replace"
                            ) as infile:
                                content = infile.read()
                        # Step 3: If neither, skip the file.
                        else:
                            logging.info(
                                "  - Skipping binary/excluded file: %s",
                                relative_path_str,
                            )
                            files_skipped_count += 1
                            continue

                        # Write content to the output file if it's not empty.
                        if content and content.strip():
                            outfile.write(f"<{relative_path_str}>\n")
                            outfile.write(content.strip())
                            outfile.write(f"\n</{relative_path_str}>\n\n")
                            files_processed_count += 1
                        else:
                            files_skipped_count += 1
                            logging.info(
                                "    No content extracted from %s", relative_path_str
                            )

                    except Exception as e:
                        files_skipped_count += 1
                        logging.error(
                            "Could not read file %s: %s", relative_path_str, e
                        )

        logging.info("\n--- Summary ---")
        logging.info("Successfully processed %d files.", files_processed_count)
        logging.info(
            "Skipped %d binary, excluded, or unreadable files.", files_skipped_count
        )
        logging.info("Combined output saved to: %s", output_filepath)

    except IOError as e:
        logging.error("Could not write to output file %s: %s", output_filepath, e)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Combine project source files")
    parser.add_argument(
        "--root",
        type=Path,
        default=PROJECT_ROOT,
        help="Project root directory",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILENAME,
        help="Name of the output file",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging level",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s: %(message)s",
    )

    combine_project_files(args.root.resolve(), args.output)


if __name__ == "__main__":
    main()
