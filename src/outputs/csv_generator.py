"""
mCODE CSV Generator

Generates mCODE-compliant CSV output from extracted cTAKES entities and relations.
MVP version: Only populates fields that can be directly extracted from cTAKES.
"""

import csv
import re
from pathlib import Path
from typing import Dict, List, Optional


class MCODECSVGenerator:
    """Generate mCODE CSV from cTAKES entities and relations."""
    
    # mCODE fields that can be directly populated from cTAKES (MVP scope)
    DIRECT_CTAKES_FIELDS = [
        # Tumor/Cancer fields
        'tumor_body_location',
        'primary_cancer_body_site',
        'primary_cancer_histology_morphology',
        'primary_cancer_cui',  # UMLS CUI for primary cancer
        
        # Medications
        'medication_request_medication',
        'medication_administration_medication',
        'medication_cuis',  # UMLS CUIs for medications
        
        # Procedures
        'cancer_related_procedure_code',
        'cancer_related_procedure_body_site',
        'procedure_cuis',  # UMLS CUIs for procedures
        
        # Tumor markers
        'tumor_marker_test_type',
        
        # Specimen
        'specimen_collection_site',
        'specimen_type',
        
        # Assertion context
        'negated_findings',  # Entities that were negated
    ]
    
    def __init__(self, config: Dict = None):
        """Initialize CSV generator."""
        self.config = config or {}
        self.mcode_data = {}
        self.document_text = ""  # Store document text for context-based filtering
        self.temporal_data = {}  # Store temporal data (dates, events, relations)
        self.sentences = []  # Store sentence boundaries from cTAKES
    
    def populate_from_ctakes(self, entities: Dict[str, List[Dict]], relations: Dict[str, List[Dict]], temporal_data: Dict[str, List[Dict]] = None, sentences: List[Dict] = None, document_text: str = ""):
        """
        Populate mCODE fields from cTAKES entities and relations.
        
        Args:
            entities: Dictionary of extracted entities by type
            relations: Dictionary of extracted relations by type
            temporal_data: Dictionary of temporal data (time_mentions, events, temporal_relations)
            sentences: List of sentence boundary dictionaries
            document_text: Full document text for context-based filtering
        """
        self.document_text = document_text
        self.temporal_data = temporal_data or {}
        self.sentences = sentences or []
        self._extract_tumor_info(entities, relations)
        self._extract_medications(entities)
        self._extract_procedures(entities, relations)
        self._extract_specimen_info(entities, relations)
        self._extract_tumor_markers(entities)
        self._extract_tnm_staging(document_text)
        self._extract_tumor_dimensions(document_text)
        self._extract_radiotherapy_info(document_text)
        self._extract_negated_findings(entities)
        self._extract_primary_cancer_date(entities)  # Add date extraction
    
    def _extract_tumor_info(self, entities: Dict, relations: Dict):
        """Extract tumor and cancer condition information."""
        # Filter diseases: exclude negated, family history (subject != patient), and historical
        all_diseases = entities.get('diseases', [])
        
        diseases = [
            e for e in all_diseases 
            if not e.get('negated', False)
            and e.get('subject', 'patient') == 'patient'  # Exclude family_member
            and e.get('historyOf', 0) == 0  # Exclude historical conditions
            # Additional heuristic: check surrounding text for family markers
            and not self._is_family_history_mention(e)
        ]
        
        anatomical_sites = entities.get('anatomical_sites', [])
        
        # Find primary cancer (look for cancer-related terms)
        cancer_terms = ['cancer', 'carcinoma', 'adenocarcinoma', 'tumor', 'neoplasm', 'malignant']
        primary_cancers = [
            d for d in diseases 
            if any(term in d['text'].lower() for term in cancer_terms)
        ]
        
        if primary_cancers:
            # Prioritize by morphology specificity, not chronological order
            # More specific diagnoses (e.g., "invasive ductal adenocarcinoma") 
            # should be selected over generic terms (e.g., "breast cancer")
            def score_morphology(cancer):
                text_lower = cancer['text'].lower()
                score = 0
                
                # High priority: specific morphology terms
                if 'invasive' in text_lower: score += 3
                if 'ductal' in text_lower or 'lobular' in text_lower: score += 2
                if 'squamous' in text_lower or 'medullary' in text_lower: score += 2
                if 'mucinous' in text_lower or 'tubular' in text_lower: score += 2
                if 'papillary' in text_lower or 'serous' in text_lower: score += 2
                
                # Medium priority: differentiation/grading terms
                if 'grade' in text_lower or 'differentiated' in text_lower: score += 1.5
                if 'metastatic' in text_lower: score += 1.5
                
                # Low priority: specific cancer subtypes
                if 'adenocarcinoma' in text_lower: score += 1
                if 'carcinoma' in text_lower and 'adenocarcinoma' not in text_lower: score += 0.5
                
                # Penalize generic/uncertain terms
                if 'suspected' in text_lower or 'possible' in text_lower: score -= 2
                
                return score
            
            # Sort by: morphology score (desc), text length (desc), position (asc)
            primary_cancers_sorted = sorted(
                primary_cancers,
                key=lambda c: (score_morphology(c), len(c['text']), -c.get('begin', 0)),
                reverse=True
            )
            
            primary = primary_cancers_sorted[0]
            
            # Prefer preferred_text from CUI over raw text
            preferred = primary.get('preferred_text')
            self.mcode_data['primary_cancer_histology_morphology'] = preferred or primary['text']
            
            # Store CUI if available
            if primary.get('primary_cui'):
                self.mcode_data['primary_cancer_cui'] = primary['primary_cui']
            
            # Blacklist: histology terms that should NOT be used as body sites
            ANATOMY_BLACKLIST = {
                'squamous', 'squamous cell', 'cell', 'cells',
                'adenocarcinoma', 'carcinoma', 
                'sarcoma', 'melanoma', 'lymphoma', 'leukemia', 'neoplasm',
                'tumor', 'tumour', 'malignant', 'benign', 'invasive',
                'metastatic', 'primary', 'secondary', 'grade', 'stage',
                'keratinizing', 'non-keratinizing', 'differentiated',
                'ductal', 'lobular', 'papillary', 'mucinous', 'serous',
                'disease', 'lesion', 'mass', 'nodule', 'cancer', 'oral'
            }
            
            def is_valid_body_site(site_text):
                """Check if site text is a valid anatomical location, not histology."""
                return site_text.lower() not in ANATOMY_BLACKLIST
            
            # Find body site from LOCATION_OF relations
            # cTAKES LOCATION_OF: Source=Disease, Target=AnatomicalSite
            # Strategy: Try each cancer mention in score order, use first valid relation
            # Skip "metastatic" entities - they point to secondary sites, not primary
            body_site_found = False
            for cancer in primary_cancers_sorted:
                # Skip metastatic entities - they describe WHERE cancer spread TO
                if 'metastatic' in cancer['text'].lower():
                    continue
                for rel in relations.get('location_of', []):
                    if rel['source_id'] == cancer['id']:
                        site_text = rel['target_text']
                        if is_valid_body_site(site_text):
                            self.mcode_data['primary_cancer_body_site'] = site_text
                            self.mcode_data['tumor_body_location'] = site_text
                            body_site_found = True
                            break
                if body_site_found:
                    break
            
            # Only fall back to document order if NO cancer mention has a valid relation
            # WARNING: This is less reliable - takes first in document order
            if not body_site_found and anatomical_sites:
                for anat in anatomical_sites:
                    if is_valid_body_site(anat['text']):
                        self.mcode_data['primary_cancer_body_site'] = anat['text']
                        self.mcode_data['tumor_body_location'] = anat['text']
                        break
    
    def _is_family_history_mention(self, entity: Dict) -> bool:
        """
        Check if entity appears in family history context.
        Heuristic: look for family relation words within 50 chars before mention.
        Reduced from 100 to avoid false positives when patient's condition 
        appears shortly after family history section.
        """
        if not self.document_text:
            return False
        
        begin = entity.get('begin', 0)
        
        # Check text before the entity mention (up to 50 chars)
        # Typical pattern: "maternal aunt with ovarian cancer" = ~15-20 chars
        context_start = max(0, begin - 50)
        context_text = self.document_text[context_start:begin].lower()
        
        # Family relation markers
        family_markers = [
            'maternal', 'paternal', 'mother', 'father', 'sister', 'brother',
            'aunt', 'uncle', 'grandmother', 'grandfather', 'cousin',
            'family history', 'family member', 'relative', 'daughter', 'son'
        ]
        
        # Check if any family marker appears in context
        return any(marker in context_text for marker in family_markers)
        
        if primary_cancers:
            # Use the first detected cancer
            primary = primary_cancers[0]
            
            # Prefer preferred_text from CUI over raw text
            preferred = primary.get('preferred_text')
            self.mcode_data['primary_cancer_histology_morphology'] = preferred or primary['text']
            
            # Store CUI if available
            if primary.get('primary_cui'):
                self.mcode_data['primary_cancer_cui'] = primary['primary_cui']
            
            # Find body site from LOCATION_OF relations
            for rel in relations.get('location_of', []):
                if rel['target_id'] == primary['id']:
                    self.mcode_data['primary_cancer_body_site'] = rel['source_text']
                    self.mcode_data['tumor_body_location'] = rel['source_text']
                    break
            
            # If no relation found, use first anatomical site mention
            if 'primary_cancer_body_site' not in self.mcode_data and anatomical_sites:
                self.mcode_data['primary_cancer_body_site'] = anatomical_sites[0]['text']
                self.mcode_data['tumor_body_location'] = anatomical_sites[0]['text']
    
    def _extract_medications(self, entities: Dict):
        """Extract medication information."""
        medications = [e for e in entities.get('medications', []) if not e.get('negated', False)]
        
        # Filter out non-medication terms
        MEDICATION_BLACKLIST = {
            # Packaging/dosage forms
            'pack', 'tablet', 'capsule', 'dose', 'iv', 'pill', 'unit', 'vial',
            'oral dosage form', 'dosage form', 'dental dosage form',
            
            # Social history substances
            'ethanol', 'alcohol', 'tobacco', 'cigarette', 'smoking',
            
            # Generic/abstract medication terms
            'pharmaceutical preparations', 'drug', 'medication',
            'alkalies', 'proteins', 'fluorides',
            
            # Temporal markers incorrectly tagged as medications
            'today', 'yesterday', 'week', 'month',
            
            # Imaging/diagnostic agents
            'contrast media', 'fluorodeoxyglucose f18', 'fdg',
            
            # Vaccines (not cancer treatment medications)
            'human papilloma virus vaccine', 'hpv vaccine', 'vaccine',
            
            # Lab tests incorrectly tagged as medications
            'antibodies, antinuclear', 'ana',
            
            # Other non-medication substances
            'cytidine monophosphate', 'cmp',
        }
        TUMOR_MARKER_CUIS = {
            'C0069515',  # HER2 receptor (erbB-2)
        }
        
        filtered_meds = []
        for med in medications:
            text = med.get('text', '').lower()
            preferred = med.get('preferred_text', '').lower()
            cui = med.get('primary_cui', '')
            
            # Skip blacklisted terms (check both raw text and preferred text)
            if text in MEDICATION_BLACKLIST or preferred in MEDICATION_BLACKLIST:
                continue
            
            # Skip tumor markers
            if cui in TUMOR_MARKER_CUIS:
                continue
            
            filtered_meds.append(med)
        
        if filtered_meds:
            # Prefer preferred_text from CUI over raw text
            med_names = [m.get('preferred_text') or m['text'] for m in filtered_meds]
            med_cuis = [m['primary_cui'] for m in filtered_meds if m.get('primary_cui')]
            
            # Deduplicate while preserving order
            seen = set()
            unique_med_names = []
            for name in med_names:
                if name.lower() not in seen:
                    seen.add(name.lower())
                    unique_med_names.append(name)
            
            # Populate both request and administration (we can't distinguish from entities alone)
            self.mcode_data['medication_request_medication'] = '; '.join(unique_med_names)
            self.mcode_data['medication_administration_medication'] = '; '.join(unique_med_names)
            
            # Store CUIs
            if med_cuis:
                self.mcode_data['medication_cuis'] = '; '.join(med_cuis)
    
    def _extract_procedures(self, entities: Dict, relations: Dict):
        """Extract procedure information."""
        procedures = [e for e in entities.get('procedures', []) if not e.get('negated', False)]
        
        if procedures:
            # Prefer preferred_text from CUI over raw text
            proc_names = [p.get('preferred_text') or p['text'] for p in procedures]
            proc_cuis = [p['primary_cui'] for p in procedures if p.get('primary_cui')]
            
            self.mcode_data['cancer_related_procedure_code'] = '; '.join(proc_names)
            
            # Store CUIs
            if proc_cuis:
                self.mcode_data['procedure_cuis'] = '; '.join(proc_cuis)
            
            # Find body site from LOCATION_OF relations
            body_sites = []
            for proc in procedures:
                for rel in relations.get('location_of', []):
                    if rel['target_id'] == proc['id']:
                        body_sites.append(rel['source_text'])
            
            if body_sites:
                self.mcode_data['cancer_related_procedure_body_site'] = '; '.join(set(body_sites))
    
    def _extract_specimen_info(self, entities: Dict, relations: Dict):
        """Extract specimen collection site and type from procedures."""
        procedures = [e for e in entities.get('procedures', []) if not e.get('negated', False)]
        anatomical_sites = entities.get('anatomical_sites', [])
        
        # Map procedure keywords to specimen types
        # Only procedures that actually collect specimens
        SPECIMEN_TYPE_MAP = {
            'tissue': ['biopsy', 'mastectomy', 'excision', 'resection', 'lumpectomy', 'surgical removal', 'surgery'],
            'blood': ['blood draw', 'serum', 'plasma', 'venipuncture'],
            'bone marrow': ['bone marrow', 'marrow aspiration', 'marrow biopsy'],
            'fluid': ['fluid aspiration', 'aspirate', 'pleural tap', 'ascites', 'paracentesis'],
        }
        
        # Extract specimen type from procedures
        specimen_type = None
        specimen_procedures = []  # Track which procedures collected specimens
        
        for proc in procedures:
            proc_text = proc['text'].lower()
            for spec_type, keywords in SPECIMEN_TYPE_MAP.items():
                if any(kw in proc_text for kw in keywords):
                    specimen_type = spec_type
                    specimen_procedures.append(proc)
                    break
            if specimen_type:
                break
        
        # Only populate specimen fields if we have evidence of specimen collection
        if specimen_type and specimen_procedures:
            self.mcode_data['specimen_type'] = specimen_type
            
            # Extract collection site - try multiple approaches
            collection_site = None
            
            # Approach 1: Look for anatomical terms in specimen collection procedure text
            for proc in specimen_procedures:
                proc_text = proc['text'].lower()
                for anat in anatomical_sites:
                    anat_text = anat['text'].lower()
                    if anat_text in proc_text:
                        collection_site = anat['text']
                        break
                if collection_site:
                    break
            
            # Approach 2: Use primary cancer body site as fallback (only if specimen was collected)
            if not collection_site and 'primary_cancer_body_site' in self.mcode_data:
                collection_site = self.mcode_data['primary_cancer_body_site']
            
            if collection_site:
                self.mcode_data['specimen_collection_site'] = collection_site
    
    def _extract_tumor_markers(self, entities: Dict):
        """Extract tumor marker information from signs/symptoms."""
        # Tumor markers often appear as SignSymptomMention
        # Common markers: ER, PR, HER2, PSA, CA-125, CEA
        marker_keywords = ['er', 'pr', 'her2', 'psa', 'ca-125', 'ca 125', 'cea']
        
        signs_symptoms = entities.get('signs_symptoms', [])
        markers = []
        
        for ss in signs_symptoms:
            if any(keyword in ss['text'].lower() for keyword in marker_keywords):
                # Prefer preferred_text from CUI
                markers.append(ss.get('preferred_text') or ss['text'])
        
        if markers:
            self.mcode_data['tumor_marker_test_type'] = '; '.join(markers)
    
    def _extract_tnm_staging(self, document_text: str):
        """
        Extract TNM staging from document text using regex.
        Handles both concatenated (cT3N1M0) and space-separated (cT3 cN1 cM0) formats.
        Prioritizes pathological (p) over clinical (c) staging.
        
        TNM format: [prefix]T[0-4][a-d]?[is]? [prefix]N[0-3][a-c]? [prefix]M[0-1][a-c]?
        Prefixes: c (clinical), p (pathological), y (post-therapy), r (recurrence), m (multiple)
        """
        import re
        
        if not document_text:
            return
        
        # Priority order for prefixes: p > y > c > r > m > no prefix
        prefix_priority = {'p': 5, 'y': 4, 'c': 3, 'r': 2, 'm': 1, '': 0}
        
        # Strategy 1: Try to match concatenated TNM (e.g., "cT3N1M0")
        # Pattern: optional prefix + T/N/M components together
        concat_pattern = r'([cpyrm]?)T([0-4Xx])([a-d])?([is])?N([0-3Xx])([a-c])?M([0-1Xx])([a-c])?'
        concat_matches = re.finditer(concat_pattern, document_text, re.IGNORECASE)
        
        best_t, best_n, best_m = None, None, None
        best_priority = -1
        
        for match in concat_matches:
            groups = match.groups()
            prefix = groups[0].lower()
            priority = prefix_priority.get(prefix, 0)
            
            # Only use this match if it's higher priority than what we have
            if priority > best_priority:
                # Extract T, N, M from groups
                # Keep prefix lowercase, uppercase the rest
                t_parts = [prefix, 'T'] + [g.upper() for g in groups[1:4] if g]
                n_parts = [prefix, 'N'] + [g.upper() for g in groups[4:6] if g]
                m_parts = [prefix, 'M'] + [g.upper() for g in groups[6:8] if g]
                
                best_t = ''.join(t_parts)
                best_n = ''.join(n_parts)
                best_m = ''.join(m_parts)
                best_priority = priority
        
        # Strategy 2: If no concatenated match, try individual components (space-separated)
        if best_t is None:
            t_pattern = r'(?<![a-zA-Z])([cpyrm]?)T([0-4Xx])([a-d])?([is])?(?=\s|$|[,;.])'
            n_pattern = r'(?<![a-zA-Z])([cpyrm]?)N([0-3Xx])([a-c])?(?=\s|$|[,;.])'
            m_pattern = r'(?<![a-zA-Z])([cpyrm]?)M([0-1Xx])([a-c])?(?=\s|$|[,;.])'
            
            t_matches = re.findall(t_pattern, document_text, re.IGNORECASE)
            n_matches = re.findall(n_pattern, document_text, re.IGNORECASE)
            m_matches = re.findall(m_pattern, document_text, re.IGNORECASE)
            
            def select_best(matches, category_letter):
                """Select staging with highest priority prefix."""
                if not matches:
                    return None
                staged = []
                for match in matches:
                    prefix = match[0].lower()
                    # Keep prefix lowercase, uppercase category and suffixes
                    code = prefix + category_letter + ''.join([m.upper() for m in match[1:] if m])
                    priority = prefix_priority.get(prefix, 0)
                    staged.append((priority, code))
                staged.sort(reverse=True, key=lambda x: x[0])
                return staged[0][1]
            
            best_t = select_best(t_matches, 'T')
            best_n = select_best(n_matches, 'N')
            best_m = select_best(m_matches, 'M')
        
        # Store results
        if best_t:
            self.mcode_data['staging_t_category'] = best_t
        if best_n:
            self.mcode_data['staging_n_category'] = best_n
        if best_m:
            self.mcode_data['staging_m_category'] = best_m
    
    def _extract_tumor_dimensions(self, document_text: str):
        """
        Extract tumor dimensions from document text using regex.
        Extracts the longest dimension in cm.
        
        Common formats:
        - "2.5 cm tumor"
        - "2.5cm tumor"
        - "2.5 x 3.0 x 4.0 cm tumor" (extracts 4.0 cm as longest)
        """
        import re
        
        if not document_text:
            return
        
        # Pattern for dimension: number (optional decimal) + optional space + unit
        # Units: cm, mm, centimeter(s), millimeter(s)
        dimension_pattern = r'(\d+\.?\d*)\s*(cm|mm|centimeters?|millimeters?)\b'
        
        # Find all dimension mentions
        matches = re.finditer(dimension_pattern, document_text, re.IGNORECASE)
        
        # Keywords for context filtering
        tumor_keywords = ['tumor', 'tumour', 'mass', 'lesion', 'neoplasm', 'carcinoma', 'cancer']
        # Negative keywords - dimensions near these are likely NOT primary tumor
        node_keywords = ['node', 'nodal', 'lymph', 'lymphadenopathy', 'adenopathy']
        
        primary_dimensions = []  # Dimensions near tumor keywords, NOT near node keywords
        node_dimensions = []     # Dimensions near node keywords (fallback only)
        
        for match in matches:
            start_pos = match.start()
            end_pos = match.end()
            
            # Check context (50 chars before and after)
            context_start = max(0, start_pos - 50)
            context_end = min(len(document_text), end_pos + 50)
            context = document_text[context_start:context_end].lower()
            
            # Check if dimension is near tumor keywords
            has_tumor_context = any(keyword in context for keyword in tumor_keywords)
            has_node_context = any(keyword in context for keyword in node_keywords)
            
            if has_tumor_context:
                value = float(match.group(1))
                unit = match.group(2).lower()
                
                # Convert to cm
                if 'mm' in unit or 'millimeter' in unit:
                    value = value / 10.0  # Convert mm to cm
                
                if has_node_context:
                    # Near node keywords - lower priority
                    node_dimensions.append((value, match.group(0)))
                else:
                    # Near tumor keywords, NOT near node - high priority
                    primary_dimensions.append((value, match.group(0)))
        
        # Prefer primary tumor dimensions, fall back to node dimensions
        if primary_dimensions:
            longest = max(primary_dimensions, key=lambda x: x[0])
            self.mcode_data['tumor_longest_dimension'] = longest[1]
        elif node_dimensions:
            # Only use node dimensions if no primary tumor dimensions found
            longest = max(node_dimensions, key=lambda x: x[0])
            self.mcode_data['tumor_longest_dimension'] = longest[1]
    
    def _extract_radiotherapy_info(self, document_text: str):
        """
        Extract radiotherapy course summary information from document text.
        
        Extracts:
        - Total dose (Gy)
        - Number of fractions
        - Modality (IMRT, VMAT, 3D-CRT, etc.)
        - Technique
        - Body site
        """
        import re
        
        if not document_text:
            return
        
        # 1. Extract Total Dose (Gy)
        # Patterns: "70 Gy", "70Gy", "70 Gray"
        dose_pattern = r'(\d+\.?\d*)\s*(Gy|Gray)\b'
        dose_matches = re.findall(dose_pattern, document_text, re.IGNORECASE)
        if dose_matches:
            # Take the highest dose (usually the primary prescription dose)
            doses = [(float(m[0]), f"{m[0]} {m[1]}") for m in dose_matches]
            highest_dose = max(doses, key=lambda x: x[0])
            self.mcode_data['radiotherapy_total_dose'] = highest_dose[1]
        
        # 2. Extract Number of Fractions
        # Patterns: "35 fractions", "in 35 fractions", "35 fx"
        fraction_pattern = r'(\d+)\s*(?:fractions?|fx)\b'
        fraction_matches = re.findall(fraction_pattern, document_text, re.IGNORECASE)
        if fraction_matches:
            # Take the first (usually the primary fractionation scheme)
            self.mcode_data['radiotherapy_number_of_fractions'] = fraction_matches[0]
        
        # 3. Extract Modality
        # Common modalities: IMRT, VMAT, 3D-CRT, SBRT, SRS, Proton, Brachytherapy
        modality_keywords = [
            'IMRT', 'VMAT', '3D-CRT', '3DCRT', 'SBRT', 'SRS', 'IGRT',
            'proton', 'brachytherapy', 'electron', 'photon',
            'intensity.modulated', 'volumetric.modulated', 'stereotactic'
        ]
        modality_pattern = r'\b(' + '|'.join(modality_keywords) + r')\b'
        modality_matches = re.findall(modality_pattern, document_text, re.IGNORECASE)
        if modality_matches:
            # Deduplicate and join unique modalities
            unique_modalities = []
            seen = set()
            for m in modality_matches:
                m_upper = m.upper()
                if m_upper not in seen:
                    seen.add(m_upper)
                    unique_modalities.append(m)
            self.mcode_data['radiotherapy_modality'] = '; '.join(unique_modalities)
        
        # 4. Extract Radiotherapy Body Site
        # Look for patterns like "IMRT to the oropharynx" or "whole breast radiation"
        rt_body_sites = []
        
        # Pattern 1: "[modality] to the [body site]"
        to_pattern = r'(?:IMRT|VMAT|radiation|radiotherapy|irradiation)\s+(?:\([^)]+\)\s+)?to\s+(?:the\s+)?([a-zA-Z]+(?:\s+(?:and\s+)?[a-zA-Z]+)*?)(?:\s+with|\s*,|\s*\.|\s+for)'
        to_matches = re.findall(to_pattern, document_text, re.IGNORECASE)
        rt_body_sites.extend(to_matches)
        
        # Pattern 2: "whole [body site] radiation/irradiation"
        whole_pattern = r'(whole\s+[a-zA-Z]+)\s+(?:radiation|radiotherapy|irradiation)'
        whole_matches = re.findall(whole_pattern, document_text, re.IGNORECASE)
        rt_body_sites.extend(whole_matches)
        
        # Pattern 3: "[body site] radiation/irradiation" (e.g., "breast radiation")
        site_first_pattern = r'([a-zA-Z]+\s+(?:nodal\s+)?(?:region(?:s)?)?)\s+(?:radiation|irradiation)'
        site_first_matches = re.findall(site_first_pattern, document_text, re.IGNORECASE)
        rt_body_sites.extend(site_first_matches)
        
        if rt_body_sites:
            # Clean and deduplicate
            cleaned_sites = []
            seen = set()
            skip_terms = ['therapy', 'treatment', 'course', 'plan', 'technique', 'daily', 
                         'definitive', 'oncology', 'consultation', 'recommended', 'prior',
                         'no', 'thermoplastic', 'head and neck']
            for site in rt_body_sites:
                site_clean = site.strip().lower()
                if site_clean not in seen and len(site_clean) > 2:
                    if not any(term in site_clean for term in skip_terms):
                        seen.add(site_clean)
                        cleaned_sites.append(site.strip())
            if cleaned_sites:
                self.mcode_data['radiotherapy_body_site'] = '; '.join(cleaned_sites)
    
    def _extract_negated_findings(self, entities: Dict):
        """Extract negated findings for assertion context."""
        negated = []
        
        for entity_type, entity_list in entities.items():
            for e in entity_list:
                if e.get('negated', False):
                    negated.append(f"{e['text']} (negated)")
        
        if negated:
            self.mcode_data['negated_findings'] = '; '.join(negated)
    
    def _extract_primary_cancer_date(self, entities: Dict):
        """
        Extract primary cancer diagnosis date using 2-tier approach:
        1. Temporal relations: Find dates linked to diagnostic procedures via cTAKES temporal relations
        2. LLM disambiguation: (Future) Use small LLM with multi-sentence context for cases where Tier 1 fails
        
        Args:
            entities: Dictionary of extracted entities (diseases)
        """
        # Keywords that indicate diagnostic procedures or confirmation
        DIAGNOSTIC_KEYWORDS = [
            'confirmed', 'diagnosed', 'diagnosis', 'biopsy', 'pathology', 
            'detected', 'identified', 'laryngoscopy', 'endoscopy', 
            'imaging', 'scan', 'mammography', 'colonoscopy'
        ]
        
        time_mentions = self.temporal_data.get('time_mentions', [])
        events = self.temporal_data.get('events', [])
        temporal_relations = self.temporal_data.get('temporal_relations', [])
        diseases = entities.get('diseases', [])
        
        # Filter diseases: exclude negated, family history, and historical
        active_diseases = [
            d for d in diseases 
            if not d.get('negated', False)
            and d.get('subject', 'patient') == 'patient'
            and d.get('historyOf', 0) == 0
        ]
        
        if not time_mentions or not active_diseases:
            return
        
        # Tier 1: Use cTAKES temporal relations
        # Find dates linked to diagnostic procedures via temporal relations
        diagnostic_date = self._find_date_via_temporal_relations(
            time_mentions, events, temporal_relations, DIAGNOSTIC_KEYWORDS
        )
        
        if diagnostic_date:
            self.mcode_data['primary_cancer_asserted_date'] = diagnostic_date['text']
            return
        
        # Tier 2: LLM disambiguation
        # If Tier 1 fails and LLM is enabled, use multi-sentence context to disambiguate
        if self.config.get('llm', {}).get('enable_disambiguation', False):
            try:
                from src.outputs.llm_disambiguator import DateDisambiguator
                disambiguator = DateDisambiguator(self.config)
                llm_date = disambiguator.disambiguate_date(
                    self.document_text,
                    time_mentions,
                    self.sentences,
                    active_diseases,
                    events
                )
                if llm_date:
                    self.mcode_data['primary_cancer_asserted_date'] = llm_date
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"LLM disambiguation failed: {e}")
    
    def _is_absolute_date(self, date_text: str) -> bool:
        """
        Check if a date string represents an absolute date (not relative).
        
        Absolute dates include:
        - Month Day, Year (e.g., "January 9, 2026", "Feb 15, 2025")
        - Day/Month/Year (e.g., "09/01/2026", "15-02-2025")
        - Year-Month-Day (e.g., "2026-01-09")
        
        Relative dates (rejected):
        - "today", "yesterday", "tomorrow"
        - "several months", "x weeks ago"
        - "last week", "next month"
        
        Args:
            date_text: The date string to validate
        
        Returns:
            True if absolute date, False if relative
        """
        date_lower = date_text.lower().strip()
        
        # Reject relative date keywords
        relative_keywords = [
            'today', 'yesterday', 'tomorrow',
            'week', 'month', 'year', 'day',  # e.g., "several months", "x weeks ago"
            'ago', 'last', 'next', 'recent', 'prior', 'previous',
            'current', 'now', 'past'
        ]
        
        if any(keyword in date_lower for keyword in relative_keywords):
            return False
        
        # Accept if contains a 4-digit year (2000-2099)
        # This catches formats like:
        # - "January 9, 2026"
        # - "09/01/2026"
        # - "2026-01-09"
        if re.search(r'\b20\d{2}\b', date_text):
            return True
        
        return False
    
    def _find_date_via_temporal_relations(self, time_mentions, events, temporal_relations, keywords):
        """
        Tier 1: Find date linked to diagnostic procedure via temporal relations.
        
        Args:
            time_mentions: List of time mention dictionaries
            events: List of event dictionaries
            temporal_relations: List of temporal relation dictionaries
            keywords: List of diagnostic keywords to match
        
        Returns:
            Time mention dictionary if found, None otherwise
        """
        # Build a map of event IDs to events for quick lookup
        event_map = {e['id']: e for e in events}
        time_map = {t['id']: t for t in time_mentions}
        
        # Find relations where:
        # - Source or target is a time mention
        # - Other argument is an event with diagnostic keywords
        for relation in temporal_relations:
            source_id = relation['source_id']
            target_id = relation['target_id']
            
            # Check if one is a time mention and the other is a diagnostic event
            time_mention = None
            event = None
            
            if source_id in time_map and target_id in event_map:
                time_mention = time_map[source_id]
                event = event_map[target_id]
            elif target_id in time_map and source_id in event_map:
                time_mention = time_map[target_id]
                event = event_map[source_id]
            
            # Check if event contains diagnostic keywords AND date is absolute
            if time_mention and event:
                event_text_lower = event['text'].lower()
                if any(keyword in event_text_lower for keyword in keywords):
                    # Only return if it's an absolute date (not "today", "yesterday", etc.)
                    if self._is_absolute_date(time_mention['text']):
                        return time_mention
        
        return None
    
    def generate_csv(self, output_path: str, source_file: Optional[str] = None):
        """
        Generate mCODE CSV file(s) with fields as rows.
        Optionally creates a second version with CUI codes if configured.
        
        Args:
            output_path: Path to output CSV file
            source_file: Name of source clinical note file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if we should generate CUIs file
        include_cuis_file = self.config.get('output', {}).get('include_cuis_file', False)
        
        # CUI field names
        cui_fields = {'primary_cancer_cui', 'medication_cuis', 'procedure_cuis'}
        
        # Read mCODE structure to get all field names
        # For now, use a subset of key fields
        all_fields = [
            'source_file',
            'patient_name',
            'birth_date',
            'gender',
            'specimen_collection_site',
            'specimen_type',
            'tumor_body_location',
            'tumor_longest_dimension',
            'primary_cancer_body_site',
            'primary_cancer_histology_morphology',
            'staging_t_category',
            'staging_n_category',
            'staging_m_category',
            'primary_cancer_asserted_date',
            'tumor_marker_test_type',
            'tumor_marker_result_value',
            'medication_request_medication',
            'medication_administration_medication',
            'cancer_related_procedure_code',
            'cancer_related_procedure_body_site',
            'radiotherapy_total_dose',
            'radiotherapy_number_of_fractions',
            'radiotherapy_modality',
            'radiotherapy_body_site',
            'procedure_cuis',
            'negated_findings',
            'primary_cancer_cui',
            'medication_cuis',
        ]
        
        # Conditionally generate version WITH CUIs if enabled
        if include_cuis_file:
            with_cuis_path = output_path.parent / f"{output_path.stem}_with_cuis{output_path.suffix}"
            rows_with_cuis = []
            rows_with_cuis.append({'Field': 'source_file', 'Value': source_file or ''})
            for field in all_fields[1:]:
                rows_with_cuis.append({
                    'Field': field,
                    'Value': self.mcode_data.get(field, '')
                })
            
            with open(with_cuis_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Field', 'Value'])
                writer.writeheader()
                writer.writerows(rows_with_cuis)
        
        # Generate primary version WITHOUT CUIs
        rows_without_cuis = []
        rows_without_cuis.append({'Field': 'source_file', 'Value': source_file or ''})
        for field in all_fields[1:]:
            if field not in cui_fields:  # Skip CUI fields
                rows_without_cuis.append({
                    'Field': field,
                    'Value': self.mcode_data.get(field, '')
                })
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['Field', 'Value'])
            writer.writeheader()
            writer.writerows(rows_without_cuis)
        
        return output_path


def generate_mcode_csv(entities: Dict, relations: Dict, temporal_data: Dict, sentences: List, output_path: str, source_file: Optional[str] = None, document_text: str = "", config: Dict = None):
    """
    Convenience function to generate mCODE CSV from entities and relations.
    
    Args:
        entities: Dictionary of extracted entities
        relations: Dictionary of extracted relations
        temporal_data: Dictionary of temporal data (time_mentions, events, temporal_relations)
        sentences: List of sentence boundary dictionaries
        output_path: Path to output CSV file
        source_file: Name of source clinical note file
        document_text: Full document text for context-based filtering
        config: Configuration dictionary (for LLM settings)
    
    Returns:
        Path to generated CSV file
    """
    generator = MCODECSVGenerator(config)
    generator.populate_from_ctakes(entities, relations, temporal_data, sentences, document_text)
    return generator.generate_csv(output_path, source_file)
