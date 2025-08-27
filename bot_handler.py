"""
Telegram Bot Handler for NSE OI Spurts Bot
Handles all Telegram bot interactions and commands
"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Dict, Optional
import json

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

from data_processor import DataProcessor
from utils import format_stock_data, format_error_message

logger = logging.getLogger(__name__)

class TelegramBotHandler:
    """Handles Telegram bot interactions"""
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.application = None
        self.data_processor = DataProcessor()
        
    async def start(self):
        """Start the Telegram bot"""
        try:
            # Create application
            self.application = Application.builder().token(self.bot_token).build()
            
            # Add handlers
            self._add_handlers()
            
            # Start the bot
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            logger.info("🤖 Telegram bot started successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to start Telegram bot: {e}")
            raise
    
    async def stop(self):
        """Stop the Telegram bot"""
        if self.application:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            logger.info("🤖 Telegram bot stopped")
    
    def _add_handlers(self):
        """Add command and message handlers"""
        # Command handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("query", self.query_command))
        self.application.add_handler(CommandHandler("list", self.list_stocks_command))
        self.application.add_handler(CommandHandler("history", self.history_command))
        
        # Message handlers
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, self.handle_stock_query
        ))
        
        # Callback query handler for inline keyboards
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        
        # Error handler
        self.application.add_error_handler(self.error_handler)
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        welcome_message = """
🚀 **Welcome to NSE OI Spurts Monitor Bot!**

I help you track Options Interest (OI) spurts data from NSE India.

📊 **What I do:**
• Monitor OI spurts data every 20 minutes (10:00 AM - 2:30 PM)
• Download and process stock data automatically
• Provide instant stock queries and historical data

🔍 **How to use me:**
• Send me a stock name to get its data
• Use /query <stock_name> for specific queries
• Use /list to see all available stocks
• Use /history <stock_name> for historical data
• Use /status to check my current status

💡 **Example queries:**
• `RELIANCE`
• `TCS`
• `/query HDFC`
• `/history INFY`

Type /help for more detailed information!
        """
        
        keyboard = [
            [
                InlineKeyboardButton("📊 Check Status", callback_data="status"),
                InlineKeyboardButton("📋 List Stocks", callback_data="list")
            ],
            [
                InlineKeyboardButton("❓ Help", callback_data="help"),
                InlineKeyboardButton("📈 Sample Query", callback_data="sample")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            welcome_message,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        help_message = """
📖 **NSE OI Spurts Bot - Help Guide**

🤖 **Available Commands:**

📊 **Data Commands:**
• `/status` - Check bot status and last data update
• `/list` - Show all available stocks for today
• `/query <stock_name>` - Get specific stock data
• `/history <stock_name>` - Get stock's serial number history

🔍 **Query Examples:**
