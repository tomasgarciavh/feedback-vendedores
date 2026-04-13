import json
import logging
import os
import re
from typing import Optional, Tuple

import config
import database
import gemini_analyzer

logger = logging.getLogger(__name__)


def _extract_scores(feedback_text: str) -> Tuple[Optional[float], Optional[str], str]:
    """
    Extract the JSON scores block from the feedback text.
    Returns (score_general, section_scores_json, clean_feedback_text).
    """
    pattern = r"```json_scores\s*(\{.*?\})\s*```"
    match = re.search(pattern, feedback_text, re.DOTALL)
    if not match:
        return None, None, feedback_text

    raw_json = match.group(1)
    clean_feedback = feedback_text[: match.start()].rstrip()

    # Remove the trailing "### SCORES" header too if present
    clean_feedback = re.sub(r"\n+---\n+### SCORES\s*$", "", clean_feedback).rstrip()

    try:
        data = json.loads(raw_json)
        score_general = float(data.get("score_general", 0)) or None
        section_keys = [
            "diagnostico_desapego", "descubrimiento_acuerdos", "empatia_escucha",
            "ingenieria_preguntas", "gestion_creencias", "storytelling",
            "pitch_personalizado", "mentalidad",
        ]
        sections = {k: float(data.get(k, 0)) for k in section_keys}
        return score_general, json.dumps(sections), clean_feedback
    except Exception as e:
        logger.warning("Could not parse scores JSON: %s | raw: %s", e, raw_json[:200])
        return None, None, clean_feedback


def process_uploaded_file(file_path: str, vendor_name: str, file_name: str, file_id: str) -> None:
    logger.info("--- Processing: %s | Vendor: %s ---", file_name, vendor_name)
    database.mark_processing(file_id, file_name, vendor_name, None)

    try:
        logger.info("Analyzing video with Gemini...")
        raw_feedback = gemini_analyzer.analyze_video(
            file_path=file_path,
            vendor_name=vendor_name,
            criteria=config.FEEDBACK_CRITERIA,
        )
        logger.info("Feedback generated. %d chars.", len(raw_feedback))

        score, section_scores, clean_feedback = _extract_scores(raw_feedback)
        logger.info("Scores extracted — general: %s | sections: %s", score, section_scores)

        database.mark_done(file_id, clean_feedback, score=score, section_scores=section_scores)
        logger.info("Done: %s", file_name)

    except Exception as exc:
        logger.error("Error processing %s: %s", file_name, exc, exc_info=True)
        database.mark_error(file_id, str(exc))

    finally:
        # Move video to persistent roleplays/ folder instead of deleting
        if os.path.exists(file_path):
            try:
                import shutil
                roleplays_dir = os.path.join(os.path.dirname(file_path), "roleplays")
                os.makedirs(roleplays_dir, exist_ok=True)
                dest = os.path.join(roleplays_dir, os.path.basename(file_path))
                shutil.move(file_path, dest)
                logger.info("Video saved to roleplays/: %s", os.path.basename(file_path))
            except OSError as e:
                logger.warning("Could not move video file %s: %s", file_path, e)
