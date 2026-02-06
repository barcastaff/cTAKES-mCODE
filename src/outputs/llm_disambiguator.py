"""
LLM-Based Date Disambiguator for mCODE

Uses small language models (e.g., DeepSeek R1:1.5b) to disambiguate diagnosis dates
when cTAKES temporal relations are insufficient.
"""

import requests
import re
import logging
from typing import Dict, List, Optional


class DateDisambiguator:
    """LLM-based disambiguator for primary cancer diagnosis dates."""
    
    def __init__(self, config: Dict):
        """
        Initialize disambiguator with configuration.
        
        Args:
            config: Configuration dictionary with LLM settings
        """
        self.config = config
        self.llm_config = config.get('llm', {})
        self.ollama_config = self.llm_config.get('ollama', {})
        self.sentence_window = self.llm_config.get('sentence_window', 1)
        self.logger = logging.getLogger(__name__)
    
    def disambiguate_date(
        self,
        text: str,
        time_mentions: List[Dict],
        sentences: List[Dict],
        diseases: List[Dict] = None,
        events: List[Dict] = None
    ) -> Optional[str]:
        """
        Disambiguate primary cancer diagnosis date using LLM.
        
        Args:
            text: Full clinical note text
            time_mentions: List of time mention dictionaries from cTAKES
            sentences: List of sentence dictionaries from cTAKES
            diseases: Optional disease entities (for future use)
            events: Optional event entities (for future use)
        
        Returns:
            Diagnosis date string (e.g., "January 9, 2026") or None
        """
        # Filter for DATE time mentions only
        date_mentions = [
            tm for tm in time_mentions 
            if tm.get('time_class', '').upper() == 'DATE'
        ]
        
        if not date_mentions:
            return None
        
        # Single date: simple classification
        if len(date_mentions) == 1:
            date = date_mentions[0]
            context = self._extract_context(date, sentences)
            if self._is_diagnosis_date(date['text'], context):
                return date['text']
            return None
        
        # Multiple dates: ask LLM to rank them
    
    def _extract_context(self, date_mention: Dict, sentences: List[Dict]) -> str:
        """
        Extract sentence context around a date mention.
        
        Args:
            date_mention: Date mention dictionary with begin/end positions
            sentences: List of sentence dictionaries
        
        Returns:
            Context string (sentence(s) around the date)
        """
        date_begin = date_mention['begin']
        date_end = date_mention['end']
        
        # Find the sentence containing the date
        containing_sentence_idx = None
        for idx, sent in enumerate(sentences):
            if sent['begin'] <= date_begin < sent['end']:
                containing_sentence_idx = idx
                break
        
        if containing_sentence_idx is None:
            # Fallback: return just the date mention text
            return date_mention['text']
        
        # Extract window of sentences
        start_idx = max(0, containing_sentence_idx - self.sentence_window)
        end_idx = min(len(sentences), containing_sentence_idx + self.sentence_window + 1)
        
        # Concatenate sentences in the window
        context_sentences = sentences[start_idx:end_idx]
        context = ' '.join(s['text'] for s in context_sentences)
        
        return context
    
    def _is_diagnosis_date(self, date_text: str, context: str) -> bool:
        """
        Ask LLM if a date is the primary cancer diagnosis date.
        
        Args:
            date_text: The date string (e.g., "January 9, 2026")
            context: Surrounding sentence context
        
        Returns:
            True if LLM confirms this is diagnosis date
        """
        prompt = self._build_classification_prompt(date_text, context)
        
        try:
            response = self._call_ollama(prompt)
            # Strip thinking tags if present
            answer = self._strip_thinking(response).strip().upper()
            
            return answer.startswith('YES')
        
        except Exception as e:
            self.logger.error(f"LLM classification error: {e}")
            return False
    
    def _rank_dates(self, date_mentions: List[Dict], sentences: List[Dict]) -> Optional[str]:
        """
        Ask LLM to rank multiple dates and select the primary diagnosis date.
        
        Args:
            date_mentions: List of date mention dictionaries
            sentences: List of sentence dictionaries
        
        Returns:
            Selected diagnosis date string or None
        """
        # Build contexts for each date
        date_contexts = []
        for idx, date in enumerate(date_mentions):
            context = self._extract_context(date, sentences)
            date_contexts.append({
                'number': idx + 1,
                'date': date['text'],
                'context': context
            })
        
        prompt = self._build_ranking_prompt(date_contexts)
        
        try:
            response = self._call_ollama(prompt)
            answer = self._strip_thinking(response).strip()
            
            # Parse response: expect a number (1, 2, 3, ...) or "NONE"
            match = re.search(r'\b(\d+)\b', answer)
            if match:
                selected_num = int(match.group(1))
                if 1 <= selected_num <= len(date_mentions):
                    selected_date = date_mentions[selected_num - 1]['text']
                    return selected_date
            
            return None
        
        except Exception as e:
            self.logger.error(f"LLM ranking error: {e}")
            return None
    
    def _build_classification_prompt(self, date_text: str, context: str) -> str:
        """Build prompt for single-date classification."""
        return f"""You are a clinical date classifier. Analyze if the date is when the primary cancer was diagnosed.

Clinical text:
"{context}"

Question: Is "{date_text}" when the cancer was first diagnosed/confirmed?

Answer YES if:
- The text says diagnostic/biopsy/procedure confirmed cancer on or around this date
- The date is linked to initial diagnosis, pathology result, or cancer detection
- Keywords: "diagnosed", "confirmed", "detected", "identified", "biopsy"

Answer NO if:
- This is a staging scan, treatment start, surgery, or follow-up visit date
- This is family/past medical history (not this patient's cancer)
- Date is for imaging, labs, or tests unrelated to initial diagnosis

Answer with ONE word only: YES or NO

Answer:"""
    
    def _build_ranking_prompt(self, date_contexts: List[Dict]) -> str:
        """Build prompt for multi-date ranking."""
        # Format dates with context
        date_blocks = []
        for dc in date_contexts:
            date_blocks.append(
                f"Date {dc['number']}: {dc['date']}\n"
                f"Context: \"{dc['context']}\"\n"
            )
        
        dates_text = "\n".join(date_blocks)
        
        return f"""You are a clinical date classifier. Multiple dates are mentioned in a clinical note. Identify which one is the PRIMARY CANCER DIAGNOSIS date.

{dates_text}

Question: Which date (if any) represents the PRIMARY CANCER DIAGNOSIS (initial diagnosis, biopsy confirmation)?

Instructions:
- Respond with ONLY the number (1, 2, 3, etc.) of the diagnosis date
- If none are diagnosis dates, respond with "NONE"
- Do NOT provide explanations or other text

Answer:"""
    
    def _call_ollama(self, prompt: str) -> str:
        """
        Call Ollama API with the given prompt.
        
        Args:
            prompt: The prompt to send to the LLM
        
        Returns:
            LLM response text
        
        Raises:
            requests.RequestException: If API call fails
        """
        base_url = self.ollama_config.get('base_url', 'http://localhost:11434')
        model = self.ollama_config.get('model', 'deepseek-r1:1.5b')
        temperature = self.ollama_config.get('temperature', 0.0)
        timeout = self.ollama_config.get('timeout', 30)
        
        response = requests.post(
            f"{base_url}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": 500,  # Allow plenty of room for DeepSeek R1 thinking + answer
                }
            },
            timeout=timeout
        )
        response.raise_for_status()
        
        json_response = response.json()
        result = json_response.get('response', '')
        thinking = json_response.get('thinking', '')
        
        # DeepSeek R1 may put the actual answer in 'thinking' field when reasoning
        # If response is empty but thinking has content, use thinking
        if not result and thinking:
            result = thinking
        
        return result
    
    def _strip_thinking(self, response: str) -> str:
        """
        Strip DeepSeek R1 thinking tags from response.
        
        Args:
            response: Raw LLM response potentially containing <think>...</think> tags
        
        Returns:
            Response with thinking tags removed
        """
        # Check if thinking is enabled in config
        if not self.llm_config.get('enable_thinking', False):
            # Thinking disabled: strip all thinking tags
            return re.sub(r'<think>.*?</think>', '', response, flags=re.DOTALL).strip()
        
        # Thinking enabled: keep tags but they won't be logged (silent thinking)
        # For now, still strip for clean output parsing
        return re.sub(r'<think>.*?</think>', '', response, flags=re.DOTALL).strip()
