import unittest

# Import all test modules
from dptt.tests import test_table
from dptt.tests import test_parser
from dptt.tests import test_cleaner
from dptt.tests import test_transforms
from dptt.tests import test_export


# Initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# Add tests to the suite
suite.addTest(loader.loadTestsFromModule(test_table))
suite.addTest(loader.loadTestsFromModule(test_parser))
suite.addTest(loader.loadTestsFromModule(test_cleaner))
suite.addTest(loader.loadTestsFromModule(test_transforms))
suite.addTest(loader.loadTestsFromModule(test_export))
  
# Run the test suite
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite) 