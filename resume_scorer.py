import os
import io
import logging
import PyPDF2
import requests
from typing import Optional
import google.genai as genai
from dotenv import load_dotenv
from utils import GOOGLE_GEMINI_API_KEY

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





class ResumeScorer:
    """Resume scorer using Google Gemini AI to evaluate resume fit against job descriptions."""

    def __init__(self):
        api_key = GOOGLE_GEMINI_API_KEY
        if not api_key:
            raise ValueError("GOOGLE_GEMINI_API_KEY environment variable not set!")
        self.client = genai.Client(api_key=api_key)
        self.model = "models/gemini-2.5-flash-lite"
        self.resume_text = None

    def load_resume_from_pdf(self, pdf_path: str) -> str:
        """Extract text from local PDF resume file."""
        try:
            with open(pdf_path, "rb") as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = "".join(page.extract_text() or "" for page in pdf_reader.pages)
                self.resume_text = text.strip()
                logger.info(f"Successfully loaded resume from {pdf_path}")
                return self.resume_text
        except Exception as e:
            logger.error(f"Error reading local PDF resume: {str(e)}")
            raise

    def load_resume_from_url(self, pdf_url: str) -> str:
        """Load and extract text from online PDF URL."""
        try:
            if "github.com" in pdf_url and "blob" in pdf_url:
                pdf_url = pdf_url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
            logger.info(f"Fetching online resume from {pdf_url}")
            response = requests.get(pdf_url, timeout=20)
            response.raise_for_status()
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(response.content))
            text = "".join(page.extract_text() or "" for page in pdf_reader.pages)
            self.resume_text = text.strip()
            logger.info("Successfully loaded resume from online PDF.")
            return self.resume_text
        except Exception as e:
            logger.error(f"Error fetching online resume: {str(e)}")
            raise

    def load_resume_from_text(self, resume_text: str):
        """Load resume directly from text."""
        self.resume_text = resume_text.strip()
        logger.info("Successfully loaded resume from text")

    def score_job_match(self, job_title: str, job_description: str, company_name: str = "") -> int:
        """Score how well the resume matches a job description (0-100)."""
        if not self.resume_text:
            raise ValueError("No resume loaded. Please load a resume first.")

        prompt = f"""
                    You are an expert resume reviewer. Score how well this resume matches the job posting.

                    RESUME:
                    {self.resume_text}

                    JOB POSTING:
                    Title: {job_title}
                    Company: {company_name}
                    Description: {job_description}

                    Format:
                    SCORE: [number 0-100]
                """
        try:
            response = self.client.models.generate_content(model=self.model, contents=prompt)
            response_text = getattr(response, "output_text", getattr(response, "text", str(response))).strip()
            score = int("".join(filter(str.isdigit, response_text.split("\n")[0].replace("SCORE:", "").strip())))
            return max(0, min(100, score))
        except Exception as e:
            logger.error(f"Error scoring job match: {str(e)}")
            return 0

# Global instance
resume_scorer = None

def initialize_resume_scorer(resume_path: Optional[str] = None, resume_url: Optional[str] = None, resume_text: Optional[str] = None) -> ResumeScorer:
    global resume_scorer
    resume_scorer = ResumeScorer()
    if resume_url or (resume_path and resume_path.startswith(("http://", "https://"))):
        resume_scorer.load_resume_from_url(resume_url or resume_path)
    elif resume_path:
        resume_scorer.load_resume_from_pdf(resume_path)
    elif resume_text:
        resume_scorer.load_resume_from_text(resume_text)
    else:
        logger.warning("No resume provided.")
    return resume_scorer

def get_job_score(job_title: str, job_description: str, company_name: str = "") -> int:
    global resume_scorer
    if not resume_scorer:
        logger.warning("Resume scorer not initialized. Returning 0.")
        return 0
    return resume_scorer.score_job_match(job_title, job_description, company_name)
