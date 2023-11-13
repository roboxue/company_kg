import unittest
from main import my_fun

class TestMain(unittest.TestCase):
    def test_my_fun(self):
        self.assertEqual(my_fun(), 'Hello world')

if __name__ == '__main__':
    unittest.main()