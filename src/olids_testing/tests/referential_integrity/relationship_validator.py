"""Referential integrity validation tests for OLIDS testing framework."""

import yaml
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from snowflake.snowpark import Session

from olids_testing.core.test_base import BaseTest, TestResult, TestStatus, TestContext
from olids_testing.core.sql_logger import log_sql_query

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn


class ReferentialIntegrityTest(BaseTest):
    """Test to validate foreign key relationships using configuration mapping."""
    
    def __init__(self, relationship_groups: Optional[List[str]] = None, mapping_file: Optional[Path] = None):
        """Initialize the test.
        
        Args:
            relationship_groups: List of relationship group names to test (e.g., ['patient_relationships'])
            mapping_file: Path to YAML file containing relationship mappings
        """
        super().__init__(
            name="referential_integrity",
            description="Validates foreign key relationships using configuration mapping",
            category="referential_integrity"
        )
        
        self.relationship_groups = relationship_groups or ["patient_relationships"]
        
        # Default mapping file location
        if mapping_file is None:
            mapping_file = Path(__file__).parent.parent.parent.parent.parent / "config" / "referential_mappings.yml"
        
        self.mapping_file = mapping_file
        self.relationships = self._load_relationships()
    
    def _load_relationships(self) -> List[Dict]:
        """Load relationship mappings from YAML configuration.
        
        Returns:
            List of relationship dictionaries
        """
        try:
            with open(self.mapping_file, 'r') as f:
                mapping_data = yaml.safe_load(f)
            
            relationships = []
            for group_name in self.relationship_groups:
                if group_name in mapping_data:
                    group = mapping_data[group_name]
                    for rel in group.get('relationships', []):
                        rel['group'] = group_name
                        rel['group_description'] = group.get('description', '')
                        relationships.append(rel)
            
            return relationships
            
        except Exception as e:
            raise ValueError(f"Failed to load relationship mappings from {self.mapping_file}: {e}")
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute the referential integrity validation.
        
        Args:
            context: Test execution context
            
        Returns:
            TestResult with details about referential integrity violations
        """
        session = context.session
        source_db = context.databases["source"]
        schema = context.schemas["masked"]
        
        try:
            # First, get available columns to validate relationships exist
            available_columns = self._get_available_columns(session, source_db, schema)
            
            validation_results = []
            total_relationships = len(self.relationships)
            total_violations = 0
            skipped_relationships = 0
            
            # Simple progress reporting with x/total status  
            import sys
            
            for i, relationship in enumerate(self.relationships):
                # Show simple progress status with proper overwrite
                sys.stdout.write(f"\r  Validating referential integrity relationships [{i+1}/{total_relationships}]")
                sys.stdout.flush()
                
                result = self._validate_relationship(
                    session, source_db, schema, relationship, available_columns
                )
                validation_results.append(result)
                
                if result['status'] == 'SKIPPED':
                    skipped_relationships += 1
                elif result['status'] == 'VIOLATED':
                    total_violations += result['violation_count']
            
            # Clear the progress line and show completion
            completion_msg = f"Completed {total_relationships} relationship validations"
            spaces = " " * max(0, 80 - len(completion_msg))
            sys.stdout.write(f"\r  {completion_msg}{spaces}")
            sys.stdout.flush()
            
            # Build failure details - ensure no duplicates
            failure_details = []
            if total_violations > 0:
                failure_details.append(f"Found {total_violations:,} referential integrity violations across {total_relationships} relationships:")
                
                # Sort results for consistent output and avoid duplicates
                violated_results = [r for r in validation_results if r['status'] == 'VIOLATED']
                
                # Sort by source table and foreign key for consistent output
                violated_results.sort(key=lambda x: (x['source_table'], x['foreign_key']))
                
                for result in violated_results:
                    percentage = result.get('violation_percentage', 0.0)
                    total_with_fk = result.get('total_with_fk', 0)
                    
                    # Truncate table names if they're too long, but keep the relationship structure
                    source_table = result['source_table']
                    reference_table = result['reference_table']
                    
                    # Truncate table names after 25 characters to be more readable
                    if len(source_table) > 25:
                        source_table = source_table[:22] + "..."
                    if len(reference_table) > 25:
                        reference_table = reference_table[:22] + "..."
                    
                    # Create the relationship description with truncated table names
                    relationship_desc = f"{source_table}.{result['foreign_key']} -> {reference_table}.{result['reference_key']}"
                    
                    if total_with_fk > 0:
                        # Keep the line under 120 characters total
                        line = f"  • {relationship_desc}: {result['violation_count']:,} invalid ({percentage:.1f}% of {total_with_fk:,})"
                    else:
                        line = f"  • {relationship_desc}: {result['violation_count']:,} invalid references"
                    
                    failure_details.append(line)
            
            if skipped_relationships > 0:
                failure_details.append(f"\nSkipped {skipped_relationships} relationships due to missing columns:")
                for result in validation_results:
                    if result['status'] == 'SKIPPED':
                        failure_details.append(f"  • {result['source_table']}.{result['foreign_key']}: {result['reason']}")
            
            # Determine test status
            if total_violations > 0:
                status = TestStatus.FAILED
            elif skipped_relationships == total_relationships:
                status = TestStatus.ERROR  # All relationships were skipped
            else:
                status = TestStatus.PASSED
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=status,
                total_tested=total_relationships,
                failed_records=len([r for r in validation_results if r['status'] == 'VIOLATED']),
                failure_rate=(len([r for r in validation_results if r['status'] == 'VIOLATED']) / total_relationships * 100) if total_relationships > 0 else 0.0,
                failure_details="\n".join(failure_details) if failure_details else None,
                metadata={
                    'relationship_groups': self.relationship_groups,
                    'total_relationships': total_relationships,
                    'total_violations': total_violations,
                    'skipped_relationships': skipped_relationships,
                    'validation_results': validation_results
                }
            )
            
        except Exception as e:
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=f"Failed to execute referential integrity test: {str(e)}",
                metadata={'relationship_groups': self.relationship_groups}
            )
    
    def _get_available_columns(self, session: Session, database: str, schema: str) -> set:
        """Get set of available (table, column) pairs.
        
        Args:
            session: Snowflake session
            database: Database name
            schema: Schema name
            
        Returns:
            Set of (table_name, column_name) tuples
        """
        columns_query = f"""
        SELECT table_name, column_name
        FROM "{database}".INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = '{schema}'
        """
        
        # Log the query
        log_sql_query(
            columns_query,
            self.name,
            "get_available_columns",
            {"database": database, "schema": schema}
        )
        
        columns = session.sql(columns_query).collect()
        return {(row['TABLE_NAME'], row['COLUMN_NAME']) for row in columns}
    
    def _validate_relationship(self, session: Session, database: str, schema: str, 
                             relationship: Dict, available_columns: set) -> Dict:
        """Validate a single foreign key relationship.
        
        Args:
            session: Snowflake session
            database: Database name 
            schema: Schema name
            relationship: Relationship configuration dictionary
            available_columns: Set of available (table, column) pairs
            
        Returns:
            Dictionary with validation result
        """
        source_table = relationship['source_table']
        foreign_key = relationship['foreign_key']
        reference_table = relationship['reference_table']
        reference_key = relationship['reference_key']
        description = relationship.get('description', '')
        
        # Check if both columns exist
        source_exists = (source_table, foreign_key) in available_columns
        ref_exists = (reference_table, reference_key) in available_columns
        
        if not source_exists or not ref_exists:
            missing = []
            if not source_exists:
                missing.append(f"{source_table}.{foreign_key}")
            if not ref_exists:
                missing.append(f"{reference_table}.{reference_key}")
            
            return {
                'source_table': source_table,
                'foreign_key': foreign_key,
                'reference_table': reference_table,
                'reference_key': reference_key,
                'description': description,
                'status': 'SKIPPED',
                'reason': f"Missing columns: {', '.join(missing)}",
                'violation_count': 0
            }
        
        try:
            # Find records with foreign keys that don't exist in the referenced table
            # Using LEFT JOIN (same as legacy script) with query optimization
            # Also get total row count for percentage calculation
            validation_query = f"""
            SELECT 
                COUNT(*) as violation_count,
                (SELECT COUNT(*) FROM "{database}"."{schema}"."{source_table}" WHERE "{foreign_key}" IS NOT NULL) as total_with_fk
            FROM "{database}"."{schema}"."{source_table}" src
            LEFT JOIN "{database}"."{schema}"."{reference_table}" ref 
                ON src."{foreign_key}" = ref."{reference_key}"
            WHERE src."{foreign_key}" IS NOT NULL 
                AND ref."{reference_key}" IS NULL
            """
            
            # Log the validation query
            log_sql_query(
                validation_query,
                "referential_integrity",
                f"{source_table}_{foreign_key}_to_{reference_table}_{reference_key}",
                {
                    "source_table": source_table,
                    "foreign_key": foreign_key,
                    "reference_table": reference_table,
                    "reference_key": reference_key,
                    "description": description
                }
            )
            
            result = session.sql(validation_query).collect()[0]
            violation_count = result['VIOLATION_COUNT']
            total_with_fk = result['TOTAL_WITH_FK']
            
            # Calculate violation percentage
            violation_percentage = (violation_count / total_with_fk * 100) if total_with_fk > 0 else 0.0
            
            return {
                'source_table': source_table,
                'foreign_key': foreign_key,
                'reference_table': reference_table,
                'reference_key': reference_key,
                'description': description,
                'status': 'VIOLATED' if violation_count > 0 else 'VALID',
                'violation_count': violation_count,
                'total_with_fk': total_with_fk,
                'violation_percentage': violation_percentage,
                'query_executed': validation_query.replace('\n', ' ').strip()
            }
            
        except Exception as e:
            return {
                'source_table': source_table,
                'foreign_key': foreign_key,
                'reference_table': reference_table,
                'reference_key': reference_key,
                'description': description,
                'status': 'ERROR',
                'reason': f"Query execution failed: {str(e)}",
                'violation_count': 0,
                'total_with_fk': 0,
                'violation_percentage': 0.0
            }


# Convenience classes for specific relationship groups
class AllReferentialIntegrityTest(ReferentialIntegrityTest):
    """Test all 127 referential integrity relationships from legacy system."""
    
    def __init__(self):
        super().__init__(
            relationship_groups=[
                "allergy_relationships",
                "appointment_relationships", 
                "diagnostic_relationships",
                "encounter_relationships",
                "episode_of_care_relationships",
                "flag_relationships",
                "location_relationships",
                "medication_relationships",
                "observation_relationships",
                "organisation_relationships",
                "patient_relationships",
                "person_relationships",
                "practitioner_relationships",
                "procedure_relationships",
                "referral_relationships",
                "schedule_relationships"
            ]
        )
        self.name = "all_referential_integrity"
        self.description = "Validates all 85 foreign key relationships from legacy referential_integrity_checker.py"


class PatientRelationshipTest(ReferentialIntegrityTest):
    """Test patient-specific foreign key relationships."""
    
    def __init__(self):
        super().__init__(
            relationship_groups=["patient_relationships"],
        )
        self.name = "patient_relationships"
        self.description = "Validates foreign key relationships for patient entities"


class PersonRelationshipTest(ReferentialIntegrityTest):
    """Test person-specific foreign key relationships."""
    
    def __init__(self):
        super().__init__(
            relationship_groups=["person_relationships"],
        )
        self.name = "person_relationships"  
        self.description = "Validates foreign key relationships for person entities"


class AllergyRelationshipTest(ReferentialIntegrityTest):
    """Test allergy and intolerance foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["allergy_relationships"])
        self.name = "allergy_relationships"
        self.description = "Validates foreign key relationships for allergy and intolerance entities"

class AppointmentRelationshipTest(ReferentialIntegrityTest):
    """Test appointment-specific foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["appointment_relationships"])
        self.name = "appointment_relationships"
        self.description = "Validates foreign key relationships for appointment entities"

class DiagnosticRelationshipTest(ReferentialIntegrityTest):
    """Test diagnostic order foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["diagnostic_relationships"])
        self.name = "diagnostic_relationships" 
        self.description = "Validates foreign key relationships for diagnostic order entities"

class EncounterRelationshipTest(ReferentialIntegrityTest):
    """Test encounter foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["encounter_relationships"])
        self.name = "encounter_relationships"
        self.description = "Validates foreign key relationships for encounter entities"

class EpisodeOfCareRelationshipTest(ReferentialIntegrityTest):
    """Test episode of care foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["episode_of_care_relationships"])
        self.name = "episode_of_care_relationships"
        self.description = "Validates foreign key relationships for episode of care entities"

class FlagRelationshipTest(ReferentialIntegrityTest):
    """Test flag foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["flag_relationships"])
        self.name = "flag_relationships"
        self.description = "Validates foreign key relationships for flag entities"

class LocationRelationshipTest(ReferentialIntegrityTest):
    """Test location foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["location_relationships"])
        self.name = "location_relationships"
        self.description = "Validates foreign key relationships for location entities"

class MedicationRelationshipTest(ReferentialIntegrityTest):
    """Test medication foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["medication_relationships"])
        self.name = "medication_relationships"
        self.description = "Validates foreign key relationships for medication entities"

class ObservationRelationshipTest(ReferentialIntegrityTest):
    """Test observation foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["observation_relationships"])
        self.name = "observation_relationships"
        self.description = "Validates foreign key relationships for observation entities"

class OrganisationRelationshipTest(ReferentialIntegrityTest):
    """Test organisation foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["organisation_relationships"])
        self.name = "organisation_relationships"
        self.description = "Validates foreign key relationships for organisation entities"

class PractitionerRelationshipTest(ReferentialIntegrityTest):
    """Test practitioner foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["practitioner_relationships"])
        self.name = "practitioner_relationships"
        self.description = "Validates foreign key relationships for practitioner entities"

class ProcedureRelationshipTest(ReferentialIntegrityTest):
    """Test procedure foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["procedure_relationships"])
        self.name = "procedure_relationships"
        self.description = "Validates foreign key relationships for procedure entities"

class ReferralRelationshipTest(ReferentialIntegrityTest):
    """Test referral foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["referral_relationships"])
        self.name = "referral_relationships"
        self.description = "Validates foreign key relationships for referral entities"

class ScheduleRelationshipTest(ReferentialIntegrityTest):
    """Test schedule foreign key relationships."""
    
    def __init__(self):
        super().__init__(relationship_groups=["schedule_relationships"])
        self.name = "schedule_relationships"
        self.description = "Validates foreign key relationships for schedule entities"