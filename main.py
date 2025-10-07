import logging, redis, uuid, json
from fastapi import FastAPI, HTTPException
from concurrent.futures import ThreadPoolExecutor
from utils import REDIS_URL
from shared.tasks import process_recent_jobs_background





logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





app = FastAPI(
    title="Grad INTERN List v1",
    version="0.0.1",
)





# Thread pool for background jobs
EXECUTOR = ThreadPoolExecutor(max_workers=3)
# Redis connection
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
# keep job data for 2 days
JOB_TTL_SECONDS = 2 * 24 * 60 * 60  





@app.on_event("startup")
async def startup_event():
    logger.info("App startup: ThreadPoolExecutor ready")




@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down executor...")
    EXECUTOR.shutdown(wait=True)
    try:
        redis_client.close()
    except Exception:
        pass
    logger.info("Shutdown complete")





@app.get("/health")
async def health():
    return {"message": "alive"}




@app.get("/recent_jobs")
async def recent_jobs(minutes: int = 60):
    """
    Start the long-running job in a background thread.
    Returns a job_id which can be used to check progress/status/result.
    """
    job_id = str(uuid.uuid4())

    # initialize status/progress in Redis
    redis_client.setex(f"status:{job_id}", JOB_TTL_SECONDS, "queued")
    redis_client.setex(f"progress:{job_id}", JOB_TTL_SECONDS, 0)

    # submit background job; the function updates Redis
    EXECUTOR.submit(process_recent_jobs_background, job_id, minutes)

    logger.info(f"Submitted threaded job {job_id}")
    return {"status": "queued", "job_id": job_id}




@app.get("/job_status/{job_id}")
async def job_status(job_id: str):
    """
    Return status, progress (as %), and result if available.
    """
    status = redis_client.get(f"status:{job_id}")
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")

    progress = redis_client.get(f"progress:{job_id}")
    result_raw = redis_client.get(f"result:{job_id}")
    result = json.loads(result_raw) if result_raw else None

    return {
        "job_id": job_id,
        "status": status,
        "progress": f"{progress}%" if progress is not None else "unknown",
        "result": result if status == "finished" else None,
    }
