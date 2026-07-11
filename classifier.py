"""
Usage as a library:
    from classifier import IsicClassifier
    clf = IsicClassifier("data/isic_taxonomy.json")
    primary, secondary, tags = clf.classify("some text describing the project")
"""
import json
import re
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


class IsicClassifier:
    def __init__(self, taxonomy_path="data/isic_taxonomy.json",
                 secondary_min_ratio=0.6):
        """
        secondary_min_ratio: the 2nd-best division is only reported as
        `secondary_class` if its similarity score is at least this fraction
        of the best score (otherwise it's noise, not a real runner-up).
        """
        taxonomy = json.loads(Path(taxonomy_path).read_text(encoding="utf-8"))
        self.codes = list(taxonomy.keys())
        self.titles = {c: taxonomy[c]["title"] for c in self.codes}
        corpus = [taxonomy[c]["text"] for c in self.codes]

        self.vectorizer = TfidfVectorizer(
            stop_words="english", max_df=0.6, min_df=1, ngram_range=(1, 2)
        )
        self.division_matrix = self.vectorizer.fit_transform(corpus)
        self.secondary_min_ratio = secondary_min_ratio

        # Global vocabulary (for tag extraction) sorted by IDF descending
        # (rarer / more specific terms make better tags than generic words)
        self.feature_names = self.vectorizer.get_feature_names_out()
        self.idf = self.vectorizer.idf_

    def classify(self, text: str, top_tags: int = 5):
        text = clean_text(text)
        if not text.strip():
            return "", "", []

        vec = self.vectorizer.transform([text])
        sims = cosine_similarity(vec, self.division_matrix)[0]
        order = sims.argsort()[::-1]

        best_idx = order[0]
        primary_class = self.codes[best_idx]
        primary_score = sims[best_idx]

        secondary_class = ""
        if primary_score > 0 and len(order) > 1:
            second_idx = order[1]
            if sims[second_idx] >= self.secondary_min_ratio * primary_score:
                secondary_class = self.codes[second_idx]

        tags = self._extract_tags(vec, top_tags)
        return primary_class, secondary_class, tags

    def title_of(self, code: str) -> str:
        return self.titles.get(code, "")

    def _extract_tags(self, doc_vec, top_n):
        arr = doc_vec.toarray()[0]
        if not arr.any():
            return []
        top_idx = arr.argsort()[::-1][:top_n]
        return [self.feature_names[i] for i in top_idx if arr[i] > 0]


def clean_text(s: str) -> str:
    s = re.sub(r"[_\-/\\\.]+", " ", s or "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def best_effort_extract_text(file_path: str, max_chars: int = 3000) -> str:
    p = Path(file_path)
    if not p.exists():
        return ""
    ext = p.suffix.lower()
    try:
        if ext == ".txt":
            return p.read_text(errors="ignore")[:max_chars]
        if ext == ".pdf":
            import fitz  # PyMuPDF
            doc = fitz.open(p)
            text = ""
            for page in doc:
                text += page.get_text()
                if len(text) >= max_chars:
                    break
            return text[:max_chars]
        if ext == ".docx":
            import docx
            d = docx.Document(p)
            text = "\n".join(par.text for par in d.paragraphs)
            return text[:max_chars]
    except Exception:
        return ""
    return ""
