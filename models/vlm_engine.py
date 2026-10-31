"""
VLM Engine Module for the Multimodal RCA Engine.
Provides a unified dual-engine interface (local Ollama/LLaVA + cloud Gemini Vision)
for direct visual analysis of Dashboard PNG images.

This implements the "VLM integration" future-work item: pixel-level anomaly
detection from Grafana-style dashboard screenshots, run alongside the
existing metric + log detection pipeline, mirroring the dual-engine pattern
already used for text (models/gemini_explainer.py, src/llm_engine.py).

Usage:
    from models.vlm_engine import VLMEngine

    engine = VLMEngine(backend="gemini")           # or backend="ollama"
    result = engine.analyze_dashboard("data/multimodal_dataset/dashboards/sample_005001.png")
    print(result)
"""

import os
import re
import json
import time
import base64
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


VLM_SYSTEM_PROMPT = """You are a Site Reliability Engineer reading a Grafana-style monitoring \
dashboard screenshot. The dashboard shows one or more time-series panels for a single \
infrastructure layer. A red dashed vertical line and a translucent red background mark \
where an anomaly begins, if one is present; a "NORMAL" or "ANOMALY DETECTED" status badge \
may also appear in the top-right corner.

Look only at the image. Decide whether the panels show anomalous behaviour (sudden spikes, \
gradual rises, sustained high plateaus, oscillations, step changes, or a red anomaly region) \
or normal, stable/diurnal behaviour.

Respond with ONLY a valid JSON object (no markdown, no commentary) with exactly these fields:
{
  "visual_status": "ANOMALY" or "NORMAL",
  "visual_confidence": <float 0.0-1.0>,
  "visual_pattern": one of ["spike", "gradual_rise", "sustained_high", "oscillation", "step_increase", "none"],
  "visual_explanation": "<one or two sentences describing what you see in the chart that supports your decision>"
}"""


def _extract_json(text: str) -> dict:
    """Best-effort JSON extraction from a raw VLM text response."""
    text = text.strip()
    if "```json" in text:
        try:
            return json.loads(text.split("```json")[1].split("```")[0].strip())
        except Exception:
            pass
    if "```" in text:
        try:
            return json.loads(text.split("```")[1].strip())
        except Exception:
            pass
    try:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            return json.loads(text[start:end + 1])
    except Exception:
        pass

    # Keyword fallback, mirrors src/llm_engine.py OllamaClient._extract_json
    upper = text.upper()
    result = {}
    if "ANOMALY" in upper:
        result["visual_status"] = "ANOMALY"
    elif "NORMAL" in upper:
        result["visual_status"] = "NORMAL"
    conf_match = re.search(r'confiden\w*[:\s]+(?:of\s+)?\(?(0\.\d+)', text.lower())
    if conf_match:
        result["visual_confidence"] = float(conf_match.group(1))
    result["visual_pattern"] = "none"
    result["visual_explanation"] = text[:300]
    return result if result else {"raw_text": text, "visual_status": "UNKNOWN"}


class OllamaVisionClient:
    """Client for a local Ollama vision model (default: llava:7b)."""

    def __init__(self, base_url=None, model=None):
        self.base_url = base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.model = model or os.getenv("DEFAULT_OLLAMA_VISION_MODEL", "llava:7b")

    def analyze(self, image_path, prompt=None):
        prompt = prompt or "Analyze this monitoring dashboard and respond with the required JSON."
        image_path = Path(image_path)
        with open(image_path, "rb") as f:
            image_b64 = base64.b64encode(f.read()).decode("utf-8")

        url = f"{self.base_url}/api/chat"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": VLM_SYSTEM_PROMPT},
                {"role": "user", "content": prompt, "images": [image_b64]},
            ],
            "stream": False,
            "options": {"temperature": 0.1, "num_ctx": 4096},
        }

        try:
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            content = response.json().get("message", {}).get("content", "")
            return _extract_json(content)
        except requests.exceptions.RequestException as e:
            detail = str(e)
            try:
                detail = e.response.text if e.response else str(e)
            except Exception:
                pass
            print(f" Ollama Vision error: {detail[:200]}")
            return {"error": detail[:200]}


class GeminiVisionClient:
    """Client for the Google Gemini Vision API (default: gemini-2.5-flash)."""

    def __init__(self, api_key=None, model=None):
        if not GEMINI_AVAILABLE:
            raise ImportError("google-generativeai not installed. Run: pip install google-generativeai")
        if not PIL_AVAILABLE:
            raise ImportError("Pillow not installed. Run: pip install pillow")

        self.api_key = api_key or os.getenv("GEMINI_API_KEY", "")
        self.model_name = model or os.getenv("DEFAULT_GEMINI_VISION_MODEL", "gemini-3.5-flash")
        if not self.api_key:
            print(" WARNING: GEMINI_API_KEY not found in environment.")
        else:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(self.model_name)

    def analyze(self, image_path, prompt=None):
        if not self.api_key:
            return {"error": "API key missing"}

        prompt = prompt or "Analyze this monitoring dashboard and respond with the required JSON."
        full_prompt = f"{VLM_SYSTEM_PROMPT}\n\n{prompt}"

        try:
            image = Image.open(image_path)
            response = self.model.generate_content(
                [full_prompt, image],
                generation_config={"temperature": 0.1},
            )
            return _extract_json(response.text.strip())
        except Exception as e:
            print(f" Gemini Vision error: {e}")
            return {"error": str(e)}


class VLMEngine:
    """Unified dual-engine VLM interface, mirroring src/llm_engine.py's LLMEngine."""

    def __init__(self, backend=None):
        self.backend = (backend or os.getenv("DEFAULT_VLM_BACKEND", "gemini")).lower()
        print(f" Initializing VLMEngine (Backend: {self.backend.upper()})")

        if self.backend == "ollama":
            self.client = OllamaVisionClient()
        elif self.backend == "gemini":
            self.client = GeminiVisionClient()
        else:
            raise ValueError(f"Unknown VLM backend: {self.backend}")

        # Set once a quota error persists across retries, so subsequent calls in the
        # same loop fail immediately instead of each burning up to max_retries*wait
        # seconds (a free-tier daily/project quota does not reset within a run).
        self.quota_exhausted = False

    def set_backend(self, backend_name):
        self.backend = backend_name.lower()
        if self.backend == "ollama":
            self.client = OllamaVisionClient()
        elif self.backend == "gemini":
            self.client = GeminiVisionClient()
        else:
            raise ValueError(f"Unknown VLM backend: {backend_name}")
        print(f" Switched VLM backend to {self.backend.upper()}")

    def analyze_dashboard(self, image_path, prompt=None):
        """
        Run pixel-level anomaly analysis on a dashboard PNG.

        Returns a dict with visual_status, visual_confidence, visual_pattern,
        visual_explanation (plus 'latency_seconds' and 'backend' for benchmarking),
        with rate-limit-aware retry on the Gemini backend. Once a quota error is seen
        twice in a row, further calls on this engine instance fail fast (no API call,
        no wait) instead of retrying — a free-tier daily/project quota will not reset
        mid-run, so retrying every remaining sample would just waste
        max_retries * wait seconds each for a guaranteed failure.
        """
        start = time.time()

        if self.quota_exhausted:
            return {
                "error": "Gemini quota already exhausted earlier in this run — "
                         "skipping API call (see VLMEngine.quota_exhausted).",
                "latency_seconds": round(time.time() - start, 3),
                "backend": self.backend,
            }

        max_retries = 5
        result = {}
        quota_hits = 0
        for attempt in range(max_retries):
            result = self.client.analyze(image_path, prompt=prompt)

            if not isinstance(result, dict) or "error" not in result:
                break

            error_msg = str(result.get("error", ""))
            if "429" in error_msg or "quota" in error_msg.lower():
                quota_hits += 1
                if quota_hits >= 2:
                    print(" Quota error persisted across retries — treating as "
                          "exhausted for the rest of this run.")
                    self.quota_exhausted = True
                    break
                wait = 65
                print(f" Rate limited. Waiting {wait}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait)
            else:
                print(f" Attempt {attempt + 1}/{max_retries} failed. Retrying in 3s...")
                time.sleep(3)

        result["latency_seconds"] = round(time.time() - start, 3)
        result["backend"] = self.backend

        # Stay under the Gemini free-tier rate limit on successive calls
        if self.backend == "gemini" and "error" not in result:
            time.sleep(13)

        return result
