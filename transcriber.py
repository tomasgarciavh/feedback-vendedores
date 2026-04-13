import logging

import assemblyai as aai

import config

logger = logging.getLogger(__name__)

# Map FEEDBACK_LANGUAGE to AssemblyAI language codes
LANGUAGE_MAP = {
    "español": "es",
    "spanish": "es",
    "english": "en",
    "inglés": "en",
    "ingles": "en",
    "português": "pt",
    "portugues": "pt",
    "portuguese": "pt",
    "français": "fr",
    "francais": "fr",
    "french": "fr",
    "deutsch": "de",
    "german": "de",
    "italiano": "it",
    "italian": "it",
}


def _get_language_code() -> str:
    lang = config.FEEDBACK_LANGUAGE.strip().lower()
    return LANGUAGE_MAP.get(lang, lang)


def transcribe(file_path: str) -> str:
    """
    Transcribe an audio/video file using AssemblyAI.
    Returns the full transcript text.
    Raises an exception with a descriptive message on failure.
    """
    if not config.ASSEMBLYAI_API_KEY:
        raise RuntimeError("ASSEMBLYAI_API_KEY is not configured.")

    aai.settings.api_key = config.ASSEMBLYAI_API_KEY
    language_code = _get_language_code()
    logger.info("Starting transcription of %s (language: %s)", file_path, language_code)

    transcription_config = aai.TranscriptionConfig(
        language_code=language_code,
        punctuate=True,
        format_text=True,
    )

    transcriber = aai.Transcriber(config=transcription_config)

    try:
        transcript = transcriber.transcribe(file_path)
    except Exception as exc:
        raise RuntimeError(f"AssemblyAI transcription failed: {exc}") from exc

    if transcript.status == aai.TranscriptStatus.error:
        raise RuntimeError(
            f"AssemblyAI transcription error: {transcript.error}"
        )

    if not transcript.text:
        raise RuntimeError("Transcription returned empty text. The audio may be silent or too short.")

    logger.info(
        "Transcription complete. Characters: %d, words approx: %d",
        len(transcript.text),
        len(transcript.text.split()),
    )
    return transcript.text
