import tempfile
import unittest
from pathlib import Path

from docs.scripts.check_links import check_links


class DocumentationLinksTests(unittest.TestCase):
    def test_rendered_routes_assets_and_anchors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "index.html").write_text(
                '<link rel="canonical" href="https://example.test/docs/">'
                '<a href="guide/#details">Guide</a>'
                '<a href="https://external.test/not-checked">External</a>'
                '<img src="logo.svg">',
                encoding="utf-8",
            )
            (root / "logo.svg").write_text("<svg/>", encoding="utf-8")
            (root / "guide").mkdir()
            page = root / "guide/index.html"
            valid = '<h1 id="details">Guide</h1><a href="../">Home</a>'
            page.write_text(valid, encoding="utf-8")
            self.assertEqual(check_links(root), (2, 4))
            for invalid in (
                '<a href="../missing/">Missing route</a>',
                '<a href="#absent">Missing anchor</a>',
                '<img src="../absent.svg">',
                '<a href="/wrong-base/">Wrong base</a>',
                '<a href="../%2e%2e/outside">Escaped path</a>',
            ):
                with self.subTest(invalid=invalid):
                    page.write_text(valid + invalid, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        check_links(root)

    def test_missing_build_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                check_links(Path(directory))
