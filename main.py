#!/usr/bin/env python3
"""
NSE Oi Spurts Telegram Bot - Main Entry Point
Monitors NSE India Oi Spurts data and provides Telegram bot interface
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from threading import Thread
import time

from bot_handler import TelegramBotHandler
from scheduler import DataScheduler
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class NSEOIBot:
    """Main application class for NSE OI Spurts Telegram Bot"""
    
    def __init__(self):
        self.config = Config()
        self.bot_handler = TelegramBotHandler(self.config.BOT_TOKEN)
        self.scheduler = DataScheduler()
        self.running = False
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False
        
    async def start(self):
        """Start the bot and scheduler"""
        try:
            logger.info("🚀 Starting NSE Oi Spurts Telegram Bot...")
            
            # Set up signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
            
            self.running = True
            
            # Start the scheduler in a separate thread
            scheduler_thread = Thread(target=self.run_scheduler, daemon=True)
            scheduler_thread.start()
            
            # Start the Telegram bot
            await self.bot_handler.start()
            
            logger.info("✅ Bot started successfully!")
            
            # Keep the main thread alive
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Error starting bot: {e}")
            raise
        finally:
            await self.stop()
            
    def run_scheduler(self):
        """Run the data scheduler in a separate thread"""
        try:
            logger.info("📊 Starting data scheduler...")
            while self.running:
                self.scheduler.run_pending()
                time.sleep(60)  # Check every minute
        except Exception as e:
            logger.error(f"❌ Scheduler error: {e}")
            
    async def stop(self):
        """Stop the bot and cleanup"""
        logger.info("🛑 Stopping bot...")
        try:
            await self.bot_handler.stop()
            self.scheduler.stop()
        except Exception as e:
            logger.error(f"❌ Error during shutdown: {e}")
        logger.info("✅ Bot stopped successfully")

async def main():
    """Main entry point"""
    try:
        bot = NSEOIBot()
        await bot.start()
    except KeyboardInterrupt:
        logger.info("🛑 Bot interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Run the bot
    asyncio.run(main())
