"""Test modules for OLIDS testing framework."""

from .data_quality import AllNullColumnsTest, EmptyTablesTest, ColumnCompletenessTest
from .person_patterns import PersonPatternTest
from .concept_mapping import ConceptMappingTest
from .referential_integrity import (
    AllReferentialIntegrityTest,
    AllergyRelationshipTest,
    AppointmentRelationshipTest,
    DiagnosticRelationshipTest,
    EncounterRelationshipTest,
    EpisodeOfCareRelationshipTest,
    FlagRelationshipTest,
    LocationRelationshipTest,
    MedicationRelationshipTest,
    ObservationRelationshipTest,
    OrganisationRelationshipTest,
    PatientRelationshipTest,
    PersonRelationshipTest,
    PractitionerRelationshipTest,
    ProcedureRelationshipTest,
    ReferralRelationshipTest,
    ScheduleRelationshipTest,
)

# Main test registry - these are included in "run all" 
TEST_REGISTRY = {
    'all_null_columns': AllNullColumnsTest,
    'empty_tables': EmptyTablesTest,
    'column_completeness': ColumnCompletenessTest,
    'all_referential_integrity': AllReferentialIntegrityTest,
    'person_patterns': PersonPatternTest,
    'concept_mapping': ConceptMappingTest,
}

# Individual test registry - available for specific execution but not in "run all"
INDIVIDUAL_TEST_REGISTRY = {
    'allergy_relationships': AllergyRelationshipTest,
    'appointment_relationships': AppointmentRelationshipTest,
    'diagnostic_relationships': DiagnosticRelationshipTest,
    'encounter_relationships': EncounterRelationshipTest,
    'episode_of_care_relationships': EpisodeOfCareRelationshipTest,
    'flag_relationships': FlagRelationshipTest,
    'location_relationships': LocationRelationshipTest,
    'medication_relationships': MedicationRelationshipTest,
    'observation_relationships': ObservationRelationshipTest,
    'organisation_relationships': OrganisationRelationshipTest,
    'patient_relationships': PatientRelationshipTest,
    'person_relationships': PersonRelationshipTest,
    'practitioner_relationships': PractitionerRelationshipTest,
    'procedure_relationships': ProcedureRelationshipTest,
    'referral_relationships': ReferralRelationshipTest,
    'schedule_relationships': ScheduleRelationshipTest,
}

# Combined registry for lookup by name
ALL_TESTS_REGISTRY = {**TEST_REGISTRY, **INDIVIDUAL_TEST_REGISTRY}

# Category mappings
TEST_CATEGORIES = {
    'data_quality': [
        'all_null_columns',
        'empty_tables',
        'column_completeness',
    ],
    'referential_integrity': [
        'all_referential_integrity',
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
        Test class
        
    Raises:
        KeyError: If test name not found
    """
    if test_name not in ALL_TESTS_REGISTRY:
        raise KeyError(f"Test '{test_name}' not found. Available tests: {list(ALL_TESTS_REGISTRY.keys())}")
    
    return ALL_TESTS_REGISTRY[test_name]

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