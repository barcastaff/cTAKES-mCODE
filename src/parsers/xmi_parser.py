"""
XMI Parser for cTAKES Output

Extracts entities and relations from cTAKES XMI files using dkpro-cassis:
- Entities: Diseases, Medications, Procedures, Anatomical Sites
- Relations: LOCATION_OF, DEGREE_OF
- CUIs: UMLS Concept Unique Identifiers with preferred text
- Polarity: Negation status for assertions
"""

from cassis import load_cas_from_xmi, load_typesystem
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import warnings


class XMIParser:
    """Parse cTAKES XMI output files using dkpro-cassis."""
    
    # cTAKES type system class names
    ENTITY_TYPES = {
        'diseases': 'org.apache.ctakes.typesystem.type.textsem.DiseaseDisorderMention',
        'medications': 'org.apache.ctakes.typesystem.type.textsem.MedicationMention',
        'procedures': 'org.apache.ctakes.typesystem.type.textsem.ProcedureMention',
        'anatomical_sites': 'org.apache.ctakes.typesystem.type.textsem.AnatomicalSiteMention',
        'signs_symptoms': 'org.apache.ctakes.typesystem.type.textsem.SignSymptomMention',
    }
    
    RELATION_TYPES = {
        'location_of': 'org.apache.ctakes.typesystem.type.relation.LocationOfTextRelation',
        'degree_of': 'org.apache.ctakes.typesystem.type.relation.DegreeOfTextRelation',
    }
    
    TEMPORAL_TYPES = {
        'time_mentions': 'org.apache.ctakes.typesystem.type.textsem.TimeMention',
        'events': 'org.apache.ctakes.typesystem.type.textsem.EventMention',
        'temporal_relations': 'org.apache.ctakes.typesystem.type.relation.TemporalTextRelation',
    }
    
    SENTENCE_TYPE = 'org.apache.ctakes.typesystem.type.textspan.Sentence'
    UMLS_CONCEPT_TYPE = 'org.apache.ctakes.typesystem.type.refsem.UmlsConcept'
    
    def __init__(self, xmi_path: str, typesystem_path: Optional[str] = None):
        """
        Initialize parser with XMI file path.
        
        Args:
            xmi_path: Path to the XMI file
            typesystem_path: Path to TypeSystem.xml (auto-detected if None)
        """
        self.xmi_path = Path(xmi_path)
        self.typesystem_path = self._find_typesystem(typesystem_path)
        
        # Load typesystem and CAS
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            with open(self.typesystem_path, 'rb') as ts_file:
                self.typesystem = load_typesystem(ts_file)
            with open(self.xmi_path, 'rb') as xmi_file:
                self.cas = load_cas_from_xmi(xmi_file, typesystem=self.typesystem)
        
        self.view = self.cas.get_view('_InitialView')
        self.text = self.view.sofa_string or ''
        
        # Build entity index for relation resolution
        self._entity_index = self._build_entity_index()
    
    def _find_typesystem(self, typesystem_path: Optional[str]) -> Path:
        """Find TypeSystem.xml in cTAKES installation."""
        if typesystem_path:
            return Path(typesystem_path)
        
        # Try common locations
        candidates = [
            self.xmi_path.parent / 'TypeSystem.xml',
            Path('apache-ctakes-6.0.0/resources/org/apache/ctakes/typesystem/types/TypeSystem.xml'),
            Path.home() / 'apache-ctakes-6.0.0/resources/org/apache/ctakes/typesystem/types/TypeSystem.xml',
        ]
        
        for candidate in candidates:
            if candidate.exists():
                return candidate
        
        raise FileNotFoundError(
            "TypeSystem.xml not found. Please provide typesystem_path or place it in the XMI directory."
        )
    
    def _build_entity_index(self) -> Dict[int, Dict]:
        """Build index of all entities by their xmi:id for relation resolution."""
        index = {}
        for entity_type, type_name in self.ENTITY_TYPES.items():
            try:
                for entity in self.view.select(type_name):
                    index[entity.xmiID] = {
                        'id': str(entity.xmiID),
                        'type': entity_type,
                        'begin': entity.begin,
                        'end': entity.end,
                        'text': entity.get_covered_text(),
                        'polarity': getattr(entity, 'polarity', 0),
                    }
            except Exception:
                pass  # Type may not exist in this XMI
        return index
    
    def _extract_cuis(self, entity) -> List[Dict]:
        """
        Extract CUIs and preferred text from an entity's ontologyConceptArr.
        
        Returns:
            List of dicts with 'cui', 'preferred_text', 'coding_scheme'
        """
        cuis = []
        try:
            ontology_arr = entity.ontologyConceptArr
            if ontology_arr:
                for concept in ontology_arr.elements:
                    if hasattr(concept, 'cui') and concept.cui:
                        cuis.append({
                            'cui': concept.cui,
                            'preferred_text': getattr(concept, 'preferredText', '') or '',
                            'coding_scheme': getattr(concept, 'codingScheme', '').lower() or '',
                        })
        except (AttributeError, TypeError):
            pass
        return cuis
    
    def extract_entities(self) -> Dict[str, List[Dict]]:
        """
        Extract all clinical entities from XMI.
        
        Returns:
            Dictionary with entity types as keys and lists of entities as values.
            Each entity contains: id, begin, end, text, polarity, negated, cuis
        """
        entities = {etype: [] for etype in self.ENTITY_TYPES.keys()}
        
        for entity_type, type_name in self.ENTITY_TYPES.items():
            try:
                for entity in self.view.select(type_name):
                    polarity = getattr(entity, 'polarity', 0)
                    cuis = self._extract_cuis(entity)
                    
                    entities[entity_type].append({
                        'id': str(entity.xmiID),
                        'begin': entity.begin,
                        'end': entity.end,
                        'text': entity.get_covered_text(),
                        'polarity': polarity,
                        'negated': polarity < 0,
                        'subject': getattr(entity, 'subject', 'patient'),  # patient vs family_member
                        'historyOf': getattr(entity, 'historyOf', 0),  # 0=current, 1=historical
                        'conditional': getattr(entity, 'conditional', False),  # hypothetical
                        'cuis': cuis,
                        'primary_cui': cuis[0]['cui'] if cuis else None,
                        'preferred_text': cuis[0]['preferred_text'] if cuis else None,
                    })
            except Exception:
                pass  # Type may not exist in this XMI
        
        return entities
    
    def extract_relations(self) -> Dict[str, List[Dict]]:
        """
        Extract relations between entities.
        
        Returns:
            Dictionary with relation types as keys and lists of relations as values.
            Each relation contains: source_id, target_id, source_text, target_text
        """
        relations = {rtype: [] for rtype in self.RELATION_TYPES.keys()}
        
        for relation_type, type_name in self.RELATION_TYPES.items():
            try:
                for relation in self.view.select(type_name):
                    parsed = self._parse_relation(relation)
                    if parsed:
                        relations[relation_type].append(parsed)
            except Exception:
                pass  # Type may not exist in this XMI
        
        return relations
    
    def _parse_relation(self, relation) -> Optional[Dict]:
        """Parse a single relation, resolving arguments to entities."""
        try:
            # Get the RelationArgument objects
            arg1 = relation.arg1
            arg2 = relation.arg2
            
            if not arg1 or not arg2:
                return None
            
            # Get the actual entity referenced by each argument
            entity1 = arg1.argument if hasattr(arg1, 'argument') else None
            entity2 = arg2.argument if hasattr(arg2, 'argument') else None
            
            if not entity1 or not entity2:
                return None
            
            return {
                'source_id': str(entity1.xmiID),
                'target_id': str(entity2.xmiID),
                'source_text': entity1.get_covered_text(),
                'target_text': entity2.get_covered_text(),
            }
        except (AttributeError, TypeError):
            return None
    
    def extract_temporal_data(self) -> Dict[str, List[Dict]]:
        """
        Extract temporal information (dates, events, temporal relations).
        
        Returns:
            Dictionary with temporal data types as keys and lists of items as values.
            - time_mentions: Dates and times with normalized values
            - events: Clinical events (procedures, diagnoses, treatments)
            - temporal_relations: Links between dates and events (CONTAINS, BEFORE, AFTER, etc.)
        """
        temporal_data = {ttype: [] for ttype in self.TEMPORAL_TYPES.keys()}
        
        # Extract time mentions (dates, times)
        try:
            for time_mention in self.view.select(self.TEMPORAL_TYPES['time_mentions']):
                temporal_data['time_mentions'].append({
                    'id': str(time_mention.xmiID),
                    'begin': time_mention.begin,
                    'end': time_mention.end,
                    'text': time_mention.get_covered_text(),
                    'time_class': getattr(time_mention, 'timeClass', ''),  # DATE, TIME, DURATION, etc.
                })
        except Exception:
            pass
        
        # Extract events (clinical events like procedures, diagnoses)
        try:
            for event in self.view.select(self.TEMPORAL_TYPES['events']):
                temporal_data['events'].append({
                    'id': str(event.xmiID),
                    'begin': event.begin,
                    'end': event.end,
                    'text': event.get_covered_text(),
                    'event_type': getattr(event, 'eventType', ''),  # ASPECTUAL, EVIDENTIAL, etc.
                    'polarity': getattr(event, 'polarity', 0),
                })
        except Exception:
            pass
        
        # Extract temporal relations
        try:
            for relation in self.view.select(self.TEMPORAL_TYPES['temporal_relations']):
                parsed = self._parse_temporal_relation(relation)
                if parsed:
                    temporal_data['temporal_relations'].append(parsed)
        except Exception:
            pass
        
        return temporal_data
    
    def _parse_temporal_relation(self, relation) -> Optional[Dict]:
        """Parse a temporal relation between dates and events."""
        try:
            # Get the RelationArgument objects
            arg1 = relation.arg1
            arg2 = relation.arg2
            
            if not arg1 or not arg2:
                return None
            
            # Get the actual entity referenced by each argument
            entity1 = arg1.argument if hasattr(arg1, 'argument') else None
            entity2 = arg2.argument if hasattr(arg2, 'argument') else None
            
            if not entity1 or not entity2:
                return None
            
            return {
                'source_id': str(entity1.xmiID),
                'target_id': str(entity2.xmiID),
                'source_text': entity1.get_covered_text(),
                'target_text': entity2.get_covered_text(),
                'relation_type': getattr(relation, 'category', ''),  # CONTAINS, BEFORE, AFTER, OVERLAP
            }
        except (AttributeError, TypeError):
            return None
    
    def extract_sentences(self) -> List[Dict]:
        """
        Extract sentence boundaries from cTAKES annotations.
        
        Returns:
            List of sentence dictionaries with begin/end positions and text.
        """
        sentences = []
        try:
            for sentence in self.view.select(self.SENTENCE_TYPE):
                sentences.append({
                    'begin': sentence.begin,
                    'end': sentence.end,
                    'text': sentence.get_covered_text()
                })
        except Exception:
            pass  # Sentence type may not exist in this XMI
        
        return sentences


def parse_xmi_file(xmi_path: str, typesystem_path: Optional[str] = None) -> Tuple[Dict[str, List[Dict]], Dict[str, List[Dict]], Dict[str, List[Dict]], List[Dict], str]:
    """
    Convenience function to parse an XMI file.
    
    Args:
        xmi_path: Path to the XMI file
        typesystem_path: Path to TypeSystem.xml (auto-detected if None)
    
    Returns:
        Tuple of (entities, relations, temporal_data, sentences, original_text)
    """
    parser = XMIParser(xmi_path, typesystem_path)
    entities = parser.extract_entities()
    relations = parser.extract_relations()
    temporal_data = parser.extract_temporal_data()
    sentences = parser.extract_sentences()
    
    return entities, relations, temporal_data, sentences, parser.text
