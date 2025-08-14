"""Test modules for OLIDS testing framework."""

from .data_quality import AllNullColumnsTest, EmptyTablesTest, ColumnCompletenessTest
from .person_patterns import PersonPatternTest
from .concept_mapping import ConceptMappingTest
from .referential_integrity import ReferentialIntegrityTest
from olids_testing.core.global_validator import create_global_validator_from_legacy_test

# Main test registry - all tests now use consistent SQL output format
# Using global validator for complex tests, keeping individual classes for simple ones
TEST_REGISTRY = {
    'null_columns': AllNullColumnsTest,
    'empty_tables': EmptyTablesTest,
    'column_completeness': ColumnCompletenessTest,
    'referential_integrity': lambda: create_global_validator_from_legacy_test('referential_integrity'),
    'person_patterns': lambda: create_global_validator_from_legacy_test('person_patterns'),
    'concept_mapping': lambda: create_global_validator_from_legacy_test('concept_mapping'),
}

# All tests registry (same as main registry)
ALL_TESTS_REGISTRY = TEST_REGISTRY

# Category mappings
TEST_CATEGORIES = {
    'data_quality': [
        'null_columns',
        'empty_tables',
        'column_completeness',
    ],
    'referential_integrity': [
        'referential_integrity',
    ],
    'person_validation': [
        'person_patterns',
    ],
    'concept_mapping': [
        'concept_mapping',
    ],
    'business_rules': [],
}

def get_test_class(test_name: str):
    """Get test class by name.
    
    Args:
        test_name: Name of the test
        
    Returns:
        Test class or test instance
        
    Raises:
        KeyError: If test name not found
    """
    if test_name not in ALL_TESTS_REGISTRY:
        raise KeyError(f"Test '{test_name}' not found. Available tests: {list(ALL_TESTS_REGISTRY.keys())}")
    
    test_class_or_factory = ALL_TESTS_REGISTRY[test_name]
    
    # If it's a lambda (factory function), call it to get the instance
    if callable(test_class_or_factory) and hasattr(test_class_or_factory, '__name__') and test_class_or_factory.__name__ == '<lambda>':
        return test_class_or_factory()
    
    # Otherwise, return the class
    return test_class_or_factory

def list_tests():
    """Get list of main test names (for 'run all' command)."""
    return list(TEST_REGISTRY.keys())

def list_all_tests():
    """Get list of all available test names (including individual tests)."""
    return list(ALL_TESTS_REGISTRY.keys())

def list_tests_by_category(category: str):
    """Get list of test names for a specific category.
    
    Args:
        category: Category name
        
    Returns:
        List of test names in the category
    """
    return TEST_CATEGORIES.get(category, [])