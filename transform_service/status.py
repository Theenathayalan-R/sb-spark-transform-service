from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class JobStatusTracker:
    def __init__(self, connector: Any, job_id: str, config: Dict[str, Any]):
        self.connector = connector
        self.job_id = job_id
        self.config = config
        self.status_table = config.get("status_table", "job_execution_status")
        self.step_status_table = config.get(
            "step_status_table", "step_execution_status"
        )

    def start_job(self, job_name: str, total_steps: int, config_hash: str) -> None:
        logger.info("Job %s started", self.job_id)

    def update_job_status(
        self,
        status: str,
        completed_steps: int = 0,
        failed_steps: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        logger.info("Job %s status=%s", self.job_id, status)

    def start_step(
        self, step_name: str, depends_on: List[str], parallel_group: Optional[str] = None
    ) -> None:
        logger.info("Step %s started", step_name)

    def complete_step(
        self, step_name: str, records_processed: Optional[int] = None
    ) -> None:
        logger.info("Step %s completed", step_name)

    def fail_step(self, step_name: str, error_message: str, retry_count: int = 0) -> None:
        logger.info("Step %s failed: %s", step_name, error_message)

    def get_completed_steps(self) -> List[str]:
        return []

    def get_failed_steps(self) -> List[str]:
        return []
