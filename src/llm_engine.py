"""
LLM Engine Module for the Multimodal RCA Engine.
Provides a unified interface to both local (Ollama) and cloud (Gemini) LLMs.
"""

import os
import json
import time
import requests
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class OllamaClient:
    """Client for local Ollama instances (e.g., Qwen, Mistral)."""
    
    def __init__(self, base_url=None, default_model=None):
        self.base_url = base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.model = default_model or os.getenv("DEFAULT_OLLAMA_MODEL", "qwen2.5:7b")
        
    def generate(self, system_prompt, user_prompt, model=None, temperature=0.1):
        """Generate response via Ollama Chat API."""
        model_to_use = model or self.model
        url = f"{self.base_url}/api/chat"
        
        payload = {
            "model": model_to_use,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_ctx": 4096
            }
        }
        
        try:
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            result = response.json()
            content = result.get("message", {}).get("content", "")
            return self._extract_json(content)
        except requests.exceptions.RequestException as e:
            # Try to read error body for more info
            error_detail = str(e)
            try:
                error_detail = e.response.text if e.response else str(e)
            except Exception:
                pass
            print(f"❌ Ollama error: {error_detail[:200]}")
            return {"error": error_detail[:200]}

    def _extract_json(self, text):
        """Attempt to extract JSON from the LLM text output, with smart fallback."""
        text = text.strip()
        # Find JSON block markers if present
        if "```json" in text:
            try:
                content = text.split("```json")[1].split("```")[0].strip()
                return json.loads(content)
            except Exception:
                pass
        
        if "```" in text:
            try:
                content = text.split("```")[1].strip()
                return json.loads(content)
            except Exception:
                pass
        
        # Direct parsing
        try:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1:
                return json.loads(text[start:end+1])
        except Exception:
            pass
        
        # === FALLBACK: Extract structured info from raw text ===
        text_upper = text.upper()
        result = {}
        
        # Extract classification
        if "ANOMALY" in text_upper and "NORMAL" in text_upper:
            # Check which comes after "classify" or "classification"
            classify_pos = max(text_upper.find("CLASSIFY"), text_upper.find("CLASSIFICATION"))
            if classify_pos != -1:
                after = text_upper[classify_pos:]
                anom_pos = after.find("ANOMALY")
                norm_pos = after.find("NORMAL")
                if anom_pos != -1 and (norm_pos == -1 or anom_pos < norm_pos):
                    result["classification"] = "ANOMALY"
                else:
                    result["classification"] = "NORMAL"
            else:
                result["classification"] = "ANOMALY"
        elif "ANOMALY" in text_upper:
            result["classification"] = "ANOMALY"
        elif "NORMAL" in text_upper:
            result["classification"] = "NORMAL"
        
        # Extract confidence
        import re
        conf_match = re.search(r'confidence[:\s]+(?:level\s+)?(?:of\s+)?(?:\()?(0\.\d+)', text.lower())
        if conf_match:
            result["confidence"] = float(conf_match.group(1))
        
        # Extract root cause if mentioned
        rc_match = re.search(r'root\s*cause[:\s]+(.+?)(?:\n|$)', text, re.IGNORECASE)
        if rc_match:
            result["root_cause"] = rc_match.group(1).strip()
        
        if result:
            result["reasoning"] = text[:500]
            print(f"📋 Parsed from text: classification={result.get('classification', '?')}, confidence={result.get('confidence', '?')}")
            return result
            
        print("⚠️ Warning: Could not parse response.")
        return {"raw_text": text, "classification": "UNKNOWN"}


class GeminiClient:
    """Client for Google Gemini API."""
    
    def __init__(self, api_key=None, default_model=None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.model_name = default_model or os.getenv("DEFAULT_GEMINI_MODEL", "gemini-2.0-flash")
        
        if not self.api_key:
            print("⚠️ WARNING: GEMINI_API_KEY not found in environment.")
        else:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(self.model_name)
            
    def generate(self, system_prompt, user_prompt, temperature=0.1):
        """Generate response via Gemini API."""
        if not self.api_key:
            return {"error": "API key missing"}
            
        formatted_prompt = f"{system_prompt}\n\n---\n\n{user_prompt}\n\nIMPORTANT: Respond ONLY with valid JSON. No markdown, no explanation outside JSON."
        
        try:
            response = self.model.generate_content(
                formatted_prompt,
                generation_config={"temperature": temperature}
            )
            # Try to parse JSON from response
            text = response.text.strip()
            # Remove markdown code block if present
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            return json.loads(text)
        except json.JSONDecodeError:
            # Fallback: use Ollama's smart parser
            return OllamaClient()._extract_json(response.text)
        except Exception as e:
            print(f"❌ Gemini API error: {e}")
            return {"error": str(e)}


class LLMEngine:
    """Unified engine to route requests to Ollama or Gemini."""
    
    def __init__(self, backend=None):
        self.backend = backend or os.getenv("DEFAULT_LLM_BACKEND", "ollama")
        
        print(f"🚀 Initializing LLMEngine (Backend: {self.backend.upper()})")
        
        if self.backend.lower() == "ollama":
            self.client = OllamaClient()
        elif self.backend.lower() == "gemini":
            self.client = GeminiClient()
        else:
            raise ValueError(f"Unknown backend: {self.backend}")
            
    def set_backend(self, backend_name):
        """Switch the backend at runtime."""
        self.backend = backend_name.lower()
        if self.backend == "ollama":
            self.client = OllamaClient()
        elif self.backend == "gemini":
            self.client = GeminiClient()
        print(f"🔄 Switched backend to {self.backend.upper()}")
        
    def analyze(self, system_prompt, user_prompt, temperature=0.1):
        """Run analysis on the active backend with rate-limit awareness."""
        max_retries = 5
        for attempt in range(max_retries):
            result = self.client.generate(system_prompt, user_prompt, temperature=temperature)
            
            # If valid response is returned
            if not isinstance(result, dict) or "error" not in result:
                # Rate limit: wait between Gemini calls (free tier = 5/min)
                if self.backend == "gemini":
                    time.sleep(13)  # ~4.6 req/min, stays under limit
                return result
            
            # Check if it's a rate limit error (429)
            error_msg = str(result.get("error", ""))
            if "429" in error_msg or "quota" in error_msg.lower():
                wait = 65
                print(f"⏳ Rate limited. Waiting {wait}s before retry {attempt+1}/{max_retries}...")
                time.sleep(wait)
            else:
                print(f"⚠️ Attempt {attempt+1}/{max_retries} failed. Retrying in 3s...")
                time.sleep(3)
            
        print("❌ All attempts failed.")
        return result

