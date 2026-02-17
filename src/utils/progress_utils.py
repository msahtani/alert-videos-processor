"""
Progress bar utilities with logging support
"""
from tqdm import tqdm


class LoggingTqdm(tqdm):
    """Custom tqdm that logs progress updates to resume log file"""
    
    def __init__(self, *args, resume_logger=None, **kwargs):
        self.resume_logger = resume_logger
        super().__init__(*args, **kwargs)
        if self.resume_logger:
            self.resume_logger.info(f"Started: {self.desc}")
    
    def update(self, n=1):
        result = super().update(n)
        if self.resume_logger and self.n > 0:
            # Log progress update - use str() representation which tqdm provides safely
            try:
                # Use tqdm's string representation which handles all attributes safely
                progress_str = str(self)
                if progress_str:
                    self.resume_logger.info(progress_str.strip())
            except Exception:
                # Fallback to simple logging if formatting fails
                try:
                    elapsed = getattr(self, 'elapsed', 0)
                    if elapsed == 0 and hasattr(self, 'start_t') and hasattr(self, '_time'):
                        elapsed = self._time() - self.start_t
                    elapsed_str = f"{int(elapsed//60):02d}:{int(elapsed%60):02d}" if elapsed > 0 else "00:00"
                    total_str = f"/{self.total}" if self.total else ""
                    self.resume_logger.info(f"{self.desc}: {self.n}{total_str} {self.unit} [{elapsed_str}]")
                except Exception:
                    # Ultimate fallback
                    self.resume_logger.info(f"{self.desc}: {self.n} {self.unit}")
        return result
    
    def set_description(self, desc=None, refresh=True):
        result = super().set_description(desc, refresh=refresh)
        if self.resume_logger and desc:
            self.resume_logger.info(f"Status: {desc}")
        return result
    
    def set_postfix(self, ordered_dict=None, refresh=True, **kwargs):
        result = super().set_postfix(ordered_dict, refresh=refresh, **kwargs)
        if self.resume_logger and (ordered_dict or kwargs):
            postfix_str = self.postfix if hasattr(self, 'postfix') else ""
            if postfix_str:
                self.resume_logger.info(f"Postfix: {postfix_str}")
        return result
    
    def close(self):
        if self.resume_logger:
            self.resume_logger.info(f"Completed: {self.desc}")
        return super().close()

