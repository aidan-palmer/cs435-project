from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
import unittest


class DashboardHTMLTest(unittest.TestCase):
    def test_element_ids_are_unique(self):
        class IDs(HTMLParser):
            def __init__(self):
                super().__init__()
                self.ids = []

            def handle_starttag(self, tag, attrs):
                self.ids.extend(value for name, value in attrs if name == 'id')

        for name in ('grid_dashboard.html', 'allocation_dashboard.html', 'results_dashboard.html'):
            with self.subTest(page=name):
                parser = IDs()
                page = Path(__file__).resolve().parents[1] / 'scripts' / name
                parser.feed(page.read_text())
                duplicates = [key for key, count in Counter(parser.ids).items() if count > 1]
                self.assertEqual(duplicates, [], 'Duplicate IDs can send updates to the wrong component')


if __name__ == '__main__':
    unittest.main()
