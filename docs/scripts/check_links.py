"""Check internal links and assets in the built documentation."""

import argparse
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit


class Page(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.ids = set()
        self.references = []
        self.canonical = None
        self.feed(text)

    def handle_starttag(self, tag, attributes):
        attributes = dict(attributes)
        if attributes.get("id"):
            self.ids.add(attributes["id"])
        if tag == "a" and attributes.get("name"):
            self.ids.add(attributes["name"])
        if tag == "link" and attributes.get("rel") == "canonical":
            self.canonical = attributes.get("href")
        for attribute in ("href", "src"):
            if attributes.get(attribute):
                self.references.append(attributes[attribute])
        srcset = attributes.get("srcset", "")
        if srcset and not srcset.startswith("data:"):
            self.references.extend(
                candidate.split()[0] for candidate in srcset.split(",") if candidate.strip()
            )


def check_links(directory):
    root = directory.resolve()
    pages = {path.resolve(): Page(path.read_text(encoding="utf-8"))
             for path in root.rglob("*.html")}
    home = pages.get(root / "index.html")
    if home is None or not home.canonical:
        raise ValueError("built documentation requires a homepage and canonical URL")
    base = urlsplit(home.canonical)
    if base.scheme != "https" or not base.netloc or not base.path.endswith("/"):
        raise ValueError("homepage canonical URL must be an HTTPS directory URL")
    failures = []
    checked = 0
    for path, page in pages.items():
        relative = path.relative_to(root).as_posix()
        source = urljoin(home.canonical, relative.removesuffix("index.html"))
        for reference in page.references:
            url = urlsplit(urljoin(source, reference))
            if url.scheme not in ("http", "https") or url.netloc != base.netloc:
                continue
            checked += 1
            if not url.path.startswith(base.path):
                failures.append(f"{relative}: outside documentation base: {reference}")
                continue
            target = (root / unquote(url.path[len(base.path):])).resolve()
            if not target.is_relative_to(root):
                failures.append(f"{relative}: path escapes documentation: {reference}")
                continue
            if target.is_dir():
                target /= "index.html"
            if not target.is_file():
                failures.append(f"{relative}: missing target: {reference}")
            elif target in pages and url.fragment and unquote(url.fragment) not in pages[target].ids:
                failures.append(f"{relative}: missing anchor: {reference}")
    if failures:
        raise ValueError("\n".join(failures))
    if checked == 0:
        raise ValueError("built documentation contains no internal references")
    return len(pages), checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        pages, references = check_links(args.directory)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Documentation link check failed:\n{error}\n")
    print(f"Documentation links passed: {pages} pages, {references} internal references.")


if __name__ == "__main__":
    main()
