#!/usr/bin/env python3
"""Render a local Markdown report to a standalone UTF-8 HTML file.

The converter intentionally uses only the Python standard library so it can run
on the Windows development machines used by this repository without requiring
Pandoc or Python markdown packages.
"""

from __future__ import annotations

import argparse
import html
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Heading:
    level: int
    text: str
    anchor: str


def render_inline(text: str) -> str:
    escaped = html.escape(text, quote=False)

    escaped = re.sub(
        r"`([^`]+)`",
        lambda m: f"<code>{html.escape(m.group(1), quote=False)}</code>",
        escaped,
    )
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        lambda m: (
            f'<a href="{html.escape(m.group(2), quote=True)}">'
            f"{m.group(1)}</a>"
        ),
        escaped,
    )
    return escaped


def slugify(text: str, used: set[str]) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    slug = re.sub(r"[^\w\u4e00-\u9fff]+", "-", normalized, flags=re.UNICODE)
    slug = slug.strip("-").lower() or "section"

    candidate = slug
    index = 2
    while candidate in used:
        candidate = f"{slug}-{index}"
        index += 1
    used.add(candidate)
    return candidate


def is_table_separator(line: str) -> bool:
    return bool(
        re.match(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$", line)
    )


def split_table_row(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", stripped)]


def render_table(lines: list[str]) -> str:
    rows = [split_table_row(line) for line in lines]
    header = rows[0]
    body = rows[2:]

    html_lines = ["<div class=\"table-wrap\"><table>", "<thead><tr>"]
    for cell in header:
        html_lines.append(f"<th>{render_inline(cell)}</th>")
    html_lines.append("</tr></thead>")

    html_lines.append("<tbody>")
    for row in body:
        html_lines.append("<tr>")
        for cell in row:
            html_lines.append(f"<td>{render_inline(cell)}</td>")
        html_lines.append("</tr>")
    html_lines.append("</tbody></table></div>")
    return "\n".join(html_lines)


def is_unordered_list(line: str) -> bool:
    return bool(re.match(r"^\s*[-*]\s+", line))


def is_ordered_list(line: str) -> bool:
    return bool(re.match(r"^\s*\d+\.\s+", line))


def render_list(lines: list[str], ordered: bool) -> str:
    tag = "ol" if ordered else "ul"
    item_pattern = r"^\s*\d+\.\s+" if ordered else r"^\s*[-*]\s+"
    rendered = [f"<{tag}>"]
    for line in lines:
        item = re.sub(item_pattern, "", line, count=1)
        rendered.append(f"<li>{render_inline(item)}</li>")
    rendered.append(f"</{tag}>")
    return "\n".join(rendered)


def looks_special(line: str, next_line: str | None) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if stripped.startswith("```"):
        return True
    if re.match(r"^#{1,6}\s+", stripped):
        return True
    if stripped in {"---", "***", "___"}:
        return True
    if next_line is not None and "|" in line and is_table_separator(next_line):
        return True
    if is_unordered_list(line) or is_ordered_list(line):
        return True
    if stripped.startswith(">"):
        return True
    return False


def render_markdown(markdown_text: str) -> tuple[str, list[Heading]]:
    lines = markdown_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    output: list[str] = []
    headings: list[Heading] = []
    used_anchors: set[str] = set()
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        if stripped.startswith("```"):
            language = stripped[3:].strip()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            if i < len(lines):
                i += 1
            lang_class = (
                f" class=\"language-{html.escape(language, quote=True)}\""
                if language
                else ""
            )
            output.append(
                "<pre><code"
                + lang_class
                + ">"
                + html.escape("\n".join(code_lines), quote=False)
                + "</code></pre>"
            )
            continue

        heading = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if heading:
            level = len(heading.group(1))
            text = heading.group(2).strip()
            anchor = slugify(text, used_anchors)
            headings.append(Heading(level=level, text=text, anchor=anchor))
            output.append(
                f'<h{level} id="{anchor}">{render_inline(text)}</h{level}>'
            )
            i += 1
            continue

        if stripped in {"---", "***", "___"}:
            output.append("<hr>")
            i += 1
            continue

        if i + 1 < len(lines) and "|" in line and is_table_separator(lines[i + 1]):
            table_lines = [line, lines[i + 1]]
            i += 2
            while i < len(lines) and "|" in lines[i] and lines[i].strip():
                table_lines.append(lines[i])
                i += 1
            output.append(render_table(table_lines))
            continue

        if is_unordered_list(line) or is_ordered_list(line):
            ordered = is_ordered_list(line)
            list_lines: list[str] = []
            while i < len(lines):
                current = lines[i]
                if ordered and not is_ordered_list(current):
                    break
                if not ordered and not is_unordered_list(current):
                    break
                list_lines.append(current)
                i += 1
            output.append(render_list(list_lines, ordered=ordered))
            continue

        if stripped.startswith(">"):
            quote_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_lines.append(re.sub(r"^\s*>\s?", "", lines[i]))
                i += 1
            output.append(
                "<blockquote>"
                + "<br>".join(render_inline(item) for item in quote_lines)
                + "</blockquote>"
            )
            continue

        paragraph: list[str] = [line.strip()]
        i += 1
        while i < len(lines):
            next_line = lines[i]
            after_next = lines[i + 1] if i + 1 < len(lines) else None
            if looks_special(next_line, after_next):
                break
            paragraph.append(next_line.strip())
            i += 1
        output.append("<p>" + "<br>".join(render_inline(item) for item in paragraph) + "</p>")

    return "\n".join(output), headings


def build_toc(headings: list[Heading]) -> str:
    toc_items = [
        heading for heading in headings if 1 <= heading.level <= 3
    ]
    if not toc_items:
        return ""

    lines = [
        "<nav class=\"toc\" aria-label=\"Table of contents\">",
        "<strong>\u76ee\u5f55</strong>",
        "<ol>",
    ]
    for heading in toc_items:
        indent_class = f"toc-level-{heading.level}"
        lines.append(
            f'<li class="{indent_class}"><a href="#{heading.anchor}">'
            f"{html.escape(heading.text, quote=False)}</a></li>"
        )
    lines.extend(["</ol>", "</nav>"])
    return "\n".join(lines)


def build_html(title: str, body: str, headings: list[Heading]) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f8fafc;
      --surface: #ffffff;
      --text: #111827;
      --muted: #4b5563;
      --border: #d0d7de;
      --accent: #0969da;
      --code-bg: #f6f8fa;
      --code-text: #24292f;
      --table-head: #eef2f7;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif;
      font-size: 16px;
      line-height: 1.7;
    }}

    .layout {{
      display: grid;
      grid-template-columns: minmax(220px, 300px) minmax(0, 1fr);
      gap: 24px;
      width: min(1480px, 100%);
      margin: 0 auto;
      padding: 24px;
    }}

    .toc {{
      position: sticky;
      top: 16px;
      align-self: start;
      max-height: calc(100vh - 32px);
      overflow: auto;
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--surface);
    }}

    .toc strong {{
      display: block;
      margin-bottom: 10px;
      font-size: 14px;
    }}

    .toc ol {{
      list-style: none;
      padding: 0;
      margin: 0;
    }}

    .toc li {{
      margin: 3px 0;
    }}

    .toc-level-2 {{
      padding-left: 12px;
    }}

    .toc-level-3 {{
      padding-left: 24px;
    }}

    .toc a {{
      color: var(--muted);
      text-decoration: none;
      font-size: 13px;
      line-height: 1.4;
    }}

    .toc a:hover {{
      color: var(--accent);
      text-decoration: underline;
    }}

    main {{
      min-width: 0;
      padding: 32px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: 0 1px 2px rgba(31, 35, 40, 0.06);
    }}

    h1, h2, h3, h4, h5, h6 {{
      line-height: 1.3;
      margin: 1.5em 0 0.65em;
      letter-spacing: 0;
    }}

    h1 {{
      margin-top: 0;
      padding-bottom: 0.35em;
      border-bottom: 1px solid var(--border);
      font-size: 2rem;
    }}

    h2 {{
      padding-bottom: 0.25em;
      border-bottom: 1px solid var(--border);
      font-size: 1.55rem;
    }}

    h3 {{
      font-size: 1.25rem;
    }}

    p {{
      margin: 0.85em 0;
    }}

    a {{
      color: var(--accent);
    }}

    code {{
      border-radius: 4px;
      background: var(--code-bg);
      color: var(--code-text);
      font-family: "Cascadia Mono", Consolas, "Liberation Mono", monospace;
      font-size: 0.92em;
      padding: 0.12em 0.32em;
    }}

    pre {{
      max-width: 100%;
      overflow-x: auto;
      margin: 1em 0;
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--code-bg);
      color: var(--code-text);
      line-height: 1.55;
      tab-size: 4;
      -webkit-overflow-scrolling: touch;
    }}

    pre code {{
      display: block;
      min-width: 0;
      padding: 0;
      border-radius: 0;
      background: transparent;
      color: inherit;
      white-space: pre-wrap;
      word-break: break-word;
      overflow-wrap: anywhere;
    }}

    .table-wrap {{
      max-width: 100%;
      overflow-x: auto;
      margin: 1em 0;
      border: 1px solid var(--border);
      border-radius: 8px;
    }}

    table {{
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      font-size: 0.94em;
    }}

    th, td {{
      padding: 8px 10px;
      border-right: 1px solid var(--border);
      border-bottom: 1px solid var(--border);
      vertical-align: top;
    }}

    th:last-child, td:last-child {{
      border-right: 0;
    }}

    tr:last-child td {{
      border-bottom: 0;
    }}

    th {{
      background: var(--table-head);
      font-weight: 600;
      text-align: left;
      white-space: nowrap;
    }}

    blockquote {{
      margin: 1em 0;
      padding: 0.2em 1em;
      border-left: 4px solid var(--border);
      color: var(--muted);
    }}

    :target {{
      scroll-margin-top: 16px;
    }}

    @media (max-width: 900px) {{
      .layout {{
        display: block;
        padding: 12px;
      }}

      .toc {{
        position: static;
        max-height: 280px;
        margin-bottom: 12px;
      }}

      main {{
        padding: 18px;
      }}

      h1 {{
        font-size: 1.55rem;
      }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    {build_toc(headings)}
    <main>
      {body}
    </main>
  </div>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--title", default=None)
    args = parser.parse_args()

    markdown_text = args.input.read_text(encoding="utf-8")
    body, headings = render_markdown(markdown_text)
    title = args.title or (headings[0].text if headings else args.input.stem)
    document = build_html(title, body, headings)
    args.output.write_text(document, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
