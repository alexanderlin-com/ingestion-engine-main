# utils/parsing.py
import os
import pdfplumber

def parse_pdf(file_path: str):
    """Extract text from a PDF, page by page, with metadata."""
    results = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            results.append({
                "text": text.strip(),
                "page_start": i,
                "page_end": i,
                "section_path": [],
                "parser": "pdfplumber",
                "ocr": False,
            })
    return results

def parse_md(file_path: str):
    """Extract text + headings from a markdown file."""
    results = []
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    current_section = []
    buffer = []
    for line in lines:
        if line.startswith("#"):  # heading
            if buffer:
                results.append({
                    "text": "".join(buffer).strip(),
                    "page_start": 1,
                    "page_end": 1,
                    "section_path": current_section.copy(),
                    "parser": "markdown",
                    "ocr": False,
                })
                buffer = []
            # update section path
            level = line.count("#")
            heading = line.strip("#").strip()
            current_section = current_section[:level-1] + [heading]
        else:
            buffer.append(line)
    # flush last buffer
    if buffer:
        results.append({
            "text": "".join(buffer).strip(),
            "page_start": 1,
            "page_end": 1,
            "section_path": current_section.copy(),
            "parser": "markdown",
            "ocr": False,
        })
    return results

def parse_file(file_path: str):
    """Router that calls the correct parser based on extension."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        return parse_pdf(file_path)
    elif ext == ".md":
        return parse_md(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")