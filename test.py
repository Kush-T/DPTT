import unittest

# Import all test modules
from dptt.tests import test_parser
from dptt.tests import test_cleaner
from dptt.tests import test_transforms

# Initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# Add tests to the suite
suite.addTest(loader.loadTestsFromModule(test_parser))
suite.addTest(loader.loadTestsFromModule(test_cleaner))
suite.addTest(loader.loadTestsFromModule(test_transforms))
  
# Run the test suite
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite) 