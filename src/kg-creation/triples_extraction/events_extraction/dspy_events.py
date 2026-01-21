# event_extraction/dspy_events.py

import dspy
import hashlib
from typing import List, Optional, Dict, Any
import nltk
from nltk.tokenize import sent_tokenize

lm = dspy.LM(
    model="google/flan-t5-large",
    device="cuda" # GPU in Lightning AI
)
dspy.configure(lm=lm)


# ------------------ DSPy schema ------------------ #
class EventExtraction(dspy.Signature):
    """
    You are an information extraction system.

    Given a news article, extract ONE concrete real-world event.

    Rules:
    - If no event is present, output event_type = "NONE"
    - Participants must be named entities (people, orgs, locations)
    - Use short phrases
    """

    text: str = dspy.InputField()

    event_type: str = dspy.OutputField(
        desc="One word: Attack, Protest, Election, Arrest, Acquisition, Disaster, etc."
    )

    trigger: str = dspy.OutputField(
        desc="Exact verb phrase from the text that signals the event"
    )

    participants: List[str] = dspy.OutputField(
        desc="List of named entities directly involved in the event"
    )

    location: Optional[str] = dspy.OutputField(
        desc="City or country where the event occurred"
    )

    time: Optional[str] = dspy.OutputField(
        desc="Explicit date or time reference from the text"
    )

# ------------------ Model init ------------------ #
def load_event_extractor():
    return dspy.Predict(EventExtraction)

# ------------------ Helpers ------------------ #
def chunk_text(text, max_chars=2000):
    sentences = sent_tokenize(text)
    chunks = []
    current = ""
    
    for i in range(len(sentences)):
        s = sentences[i]
        if len(current) + len(s) <= max_chars:
            current += " " + s
        else:
            if current.strip():
                chunks.append(current.strip())
            current = s

    if current.strip():
        chunks.append(current.strip())

    # Sliding window version (optional, for more events)
    sliding_chunks = []
    for i in range(len(sentences) - 2):
        sliding_chunks.append(" ".join(sentences[i:i+3]))
    
    return chunks + sliding_chunks



def hash_trigger(trigger):
    return hashlib.sha1(trigger.encode("utf-8")).hexdigest()[:16]

def extract_events(text, extractor, max_events=10):
    """
    Extract multiple events from an article (sentence-level).
    """
    events = []
    seen_triggers = set()

    for sent in sent_tokenize(text):
        if len(sent) < 40:
            continue  # skip short junk sentences

        try:
            r = extractor(text=sent)

            if not r.event_type or r.event_type.strip().upper() == "NONE":
                continue

            trigger = r.trigger.strip()
            if not trigger or trigger in seen_triggers:
                continue

            seen_triggers.add(trigger)

            events.append({
                "event_type": r.event_type.strip(),
                "trigger_sentence": trigger,
                "participants": r.participants or [],
                "location": r.location,
                "time": r.time,
            })

            if len(events) >= max_events:
                break

        except Exception:
            continue

    return events

