"""
Scheduler for NSE Oi Spurts Bot
Handles automated data collection timing and task management
"""

import logging
import schedule
import time
from datetime import datetime, time as dt_time
import threading
from typing import Optional

from config import Config
from scraper import NSEScraper
from data_processor import DataProcessor

logger = logging.getLogger(__name__)

class DataScheduler:
    """Handles scheduling of automated data collection tasks"""
    
    def __init__(self):
        self.config = Config()
        self.scraper = NSEScraper()
        self.processor = DataProcessor()
        self.running = False
        self.last_run_time = None
        self.next_run_time = None
        
        self._setup_schedule()
    
    def _setup_schedule(self):
        """Setup the data collection schedule"""
        try:
            # Clear any existing jobs
            schedule.clear()
            
            # Schedule data collection every 20 minutes between 10:00 AM and 2:30 PM
            start_time = self.config.MONITORING_START_TIME
            end_time = self.config.MONITORING_END_TIME
            
            logger.info(f"⏰ Setting up schedule: {start_time} to {end_time} every {self.config.SCRAPING_INTERVAL_MINUTES} minutes")
            
            # Create time slots for data collection
            current_time = datetime.combine(datetime.today(), start_time)
            end_datetime = datetime.combine(datetime.today(), end_time)
            
            while current_time <= end_datetime:
                time_str = current_time.strftime('%H:%M')
                
                # Schedule the job for this time
                schedule.every().day.at(time_str).do(self._collect_data_job)
                logger.debug(f"📅 Scheduled data collection at {time_str}")
                
                # Move to next interval
                current_time = current_time.replace(
                    minute=(current_time.minute // self.config.SCRAPING_INTERVAL_MINUTES + 1) * self.config.SCRAPING_INTERVAL_MINUTES
                )
                
                # Handle hour overflow
                if current_time.minute >= 60:
                    current_time = current_time.replace(
                        hour=current_time.hour + current_time.minute // 60,
                        minute=current_time.minute % 60
                    )
            
            # Schedule daily cleanup at market close
            schedule.every().day.at("15:00").do(self._daily_cleanup_job)
            schedule.every().day.at("00:01").do(self._reset_daily_data_job)
            
            # Schedule periodic maintenance
            schedule.every().hour.do(self._maintenance_job)
            
            logger.info(f"✅ Scheduled {len(schedule.jobs)} total jobs")
            
        except Exception as e:
            logger.error(f"❌ Error setting up schedule: {e}")
    
    def _collect_data_job(self):
        """Job function for data collection"""
        try:
            logger.info("🔄 Starting scheduled data collection...")
            self.last_run_time = datetime.now()
            
            # Check if we're in market hours
            current_time = datetime.now().time()
            if not (self.config.MONITORING_START_TIME <= current_time <= self.config.MONITORING_END_TIME):
                logger.info("⏰ Outside market hours, skipping data collection")
                return
            
            # Scrape data from NSE
            scrape_result = self.scraper.scrape_oi_spurts_data()
            
            if not scrape_result or not scrape_result.get('success'):
                logger.warning("⚠️  Primary scraping failed, trying fallback method...")
                scrape_result = self.scraper.get_fallback_data()
                
                if not scrape_result or not scrape_result.get('success'):
                    logger.error("❌ All scraping methods failed")
                    return
            
            # Process the downloaded file
            file_path = scrape_result.get('file_path')
            if file_path:
                process_result = self.processor.process_excel_file(file_path)
                
                if process_result and process_result.get('success'):
                    stocks_count = process_result.get('stocks_processed', 0)
                    logger.info(f"✅ Successfully processed {stocks_count} stocks")
                else:
                    logger.error("❌ Failed to process downloaded file")
            else:
                logger.error("❌ No file path in scrape result")
                
        except Exception as e:
            logger.error(f"❌ Error in data collection job: {e}")
    
    def _daily_cleanup_job(self):
        """Job function for daily cleanup"""
        try:
            logger.info("🧹 Starting daily cleanup...")
            
            # Clean up old Excel files
            self.scraper.cleanup_old_files()
            
            # Clean up old processed data
            self.processor.cleanup_old_data()
            
            logger.info("✅ Daily cleanup completed")
            
        except Exception as e:
            logger.error(f"❌ Error in daily cleanup job: {e}")
    
    def _reset_daily_data_job(self):
        """Job function to reset daily data at midnight"""
        try:
            logger.info("🔄 Resetting daily data for new day...")
            
            # Force reload of daily data (which will clear old data)
            self.processor.load_daily_data()
            
            logger.info("✅ Daily data reset completed")
            
        except Exception as e:
            logger.error(f"❌ Error in daily data reset job: {e}")
    
    def _maintenance_job(self):
        """Job function for periodic maintenance"""
        try:
            logger.debug("🔧 Running maintenance tasks...")
            
            # Update next run time
            self._update_next_run_time()
            
            # Log status
            status = self.processor.get_bot_status()
            logger.debug(f"📊 Current status: {status['total_stocks']} stocks, {status['successful_updates']} successful updates")
            
        except Exception as e:
            logger.error(f"❌ Error in maintenance job: {e}")
    
    def _update_next_run_time(self):
        """Update the next scheduled run time"""
        try:
            now = datetime.now()
            upcoming_jobs = []
            
            for job in schedule.jobs:
                if hasattr(job, 'next_run'):
                    upcoming_jobs.append(job.next_run)
            
            if upcoming_jobs:
                self.next_run_time = min(upcoming_jobs)
            else:
                self.next_run_time = None
                
        except Exception as e:
            logger.debug(f"Error updating next run time: {e}")
            self.next_run_time = None
    
    def run_pending(self):
        """Run pending scheduled jobs"""
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f"❌ Error running pending jobs: {e}")
    
    def start(self):
        """Start the scheduler in a separate thread"""
        self.running = True
        scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("🚀 Scheduler started")
    
    def _run_scheduler(self):
        """Main scheduler loop"""
        try:
            while self.running:
                self.run_pending()
                time.sleep(60)  # Check every minute
        except Exception as e:
            logger.error(f"❌ Scheduler error: {e}")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        schedule.clear()
        logger.info("🛑 Scheduler stopped")
    
    def get_schedule_info(self) -> dict:
        """Get information about scheduled jobs"""
        try:
            jobs_info = []
            for job in schedule.jobs:
                jobs_info.append({
                    'function': str(job.job_func),
                    'next_run': job.next_run.isoformat() if hasattr(job, 'next_run') and job.next_run else None,
                    'interval': str(job.interval) if hasattr(job, 'interval') else None,
                    'unit': str(job.unit) if hasattr(job, 'unit') else None
                })
            
            return {
                'total_jobs': len(schedule.jobs),
                'jobs': jobs_info,
                'last_run': self.last_run_time.isoformat() if self.last_run_time else None,
                'next_run': self.next_run_time.isoformat() if self.next_run_time else None,
                'running': self.running
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting schedule info: {e}")
            return {'error': str(e)}
    
    def force_run_now(self) -> bool:
        """Force run data collection immediately"""
        try:
            logger.info("🚀 Forcing immediate data collection...")
            self._collect_data_job()
            return True
        except Exception as e:
            logger.error(f"❌ Error in forced run: {e}")
            return False
    
    def is_market_hours(self) -> bool:
        """Check if current time is within market hours"""
        try:
            current_time = datetime.now().time()
            return self.config.MONITORING_START_TIME <= current_time <= self.config.MONITORING_END_TIME
        except Exception as e:
            logger.error(f"❌ Error checking market hours: {e}")
            return False
