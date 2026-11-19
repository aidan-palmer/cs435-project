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

        parser = IDs()
        page = Path(__file__).resolve().parents[1] / 'scripts' / 'grid_dashboard.html'
        parser.feed(page.read_text())
        duplicates = [name for name, count in Counter(parser.ids).items() if count > 1]
        self.assertEqual(duplicates, [], 'Duplicate IDs can cause summary updates to destroy a map')


if __name__ == '__main__':
    unittest.main()
