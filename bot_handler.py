"""
Telegram Bot Handler for NSE Oi Spurts Bot
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
🚀 **Welcome to NSE Oi Spurts Monitor Bot!**

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
📖 **NSE Oi Spurts Bot - Help Guide**

🤖 **Available Commands:**

📊 **Data Commands:**
• `/status` - Check bot status and last data update
• `/list` - Show all available stocks for today
• `/query <stock_name>` - Get specific stock data
• `/history <stock_name>` - Get stock's serial number history

🔍 **Query Examples:**
• `/query RELIANCE` - Get RELIANCE stock data
• `/query TCS` - Get TCS stock data
• `HDFC` - Direct stock name query
• `/history INFY` - Get INFY serial number history
• `/list` - See all available stocks
• `/status` - Check bot status

📊 **How it works:**
The bot monitors NSE OI Spurts data every 20 minutes during market hours (10:00 AM - 2:30 PM) and tracks each stock's position (serial number) in the ranking.

💡 **Tips:**
• Use exact stock symbols for best results
• Bot is most active during market hours
• Historical data shows how stock rankings change

⚙️ **Bot Features:**
• Real-time data collection
• Historical tracking
• Smart stock search
• Market hours monitoring
        """
        
        await update.message.reply_text(
            help_message,
            parse_mode='Markdown'
        )
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        try:
            status = self.data_processor.get_bot_status()
            
            status_message = f"""
📊 **NSE Oi Spurts Bot Status**

🔄 **Current Status:** {'🟢 Active' if status.get('is_active', False) else '🔴 Inactive'}
⏰ **Market Hours:** {'Yes' if status.get('in_market_hours', False) else 'No'}
📈 **Total Stocks:** {status.get('total_stocks', 0)}
📄 **Files Today:** {status.get('files_today', 0)}
✅ **Successful Updates:** {status.get('successful_updates', 0)}
❌ **Failed Updates:** {status.get('failed_updates', 0)}
⏱️ **Uptime:** {status.get('uptime', 'Unknown')}
🕐 **Last Update:** {status.get('last_update', 'Never') if status.get('last_update') else 'Never'}
🔄 **Next Update:** {status.get('next_update', 'Unknown')}
📅 **Date:** {status.get('current_date', 'Unknown')}
            """.strip()
            
            await update.message.reply_text(
                status_message,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            error_msg = format_error_message("Status Check Failed", str(e))
            await update.message.reply_text(error_msg, parse_mode='Markdown')
    
    async def query_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /query command"""
        try:
            if not context.args:
                await update.message.reply_text(
                    "❓ **Please provide a stock name**\n\nExample: `/query RELIANCE`",
                    parse_mode='Markdown'
                )
                return
            
            stock_name = ' '.join(context.args).upper().strip()
            await self._process_stock_query(update, stock_name)
            
        except Exception as e:
            error_msg = format_error_message("Query Failed", str(e))
            await update.message.reply_text(error_msg, parse_mode='Markdown')
    
    async def list_stocks_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /list command"""
        try:
            stocks = self.data_processor.get_all_stocks_today()
            
            if not stocks:
                await update.message.reply_text(
                    "📭 **No stocks available**\n\nNo data has been collected today yet.",
                    parse_mode='Markdown'
                )
                return
            
            # Format stock list
            from utils import format_stock_list
            pages = format_stock_list(stocks)
            
            # Send first page
            if pages:
                await update.message.reply_text(
                    pages[0],
                    parse_mode='Markdown'
                )
            
        except Exception as e:
            error_msg = format_error_message("List Stocks Failed", str(e))
            await update.message.reply_text(error_msg, parse_mode='Markdown')
    
    async def history_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /history command"""
        try:
            if not context.args:
                await update.message.reply_text(
                    "❓ **Please provide a stock name**\n\nExample: `/history RELIANCE`",
                    parse_mode='Markdown'
                )
                return
            
            stock_name = ' '.join(context.args).upper().strip()
            history = self.data_processor.get_stock_history(stock_name)
            
            if not history:
                await update.message.reply_text(
                    f"📭 **No history found for {stock_name}**\n\nMake sure the stock name is correct.",
                    parse_mode='Markdown'
                )
                return
            
            # Format history
            history_lines = []
            for i, entry in enumerate(history[-10:]):  # Last 10 entries
                timestamp = entry.get('timestamp', '')
                serial = entry.get('serial_number', 'N/A')
                change = entry.get('change', 0)
                
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    time_str = dt.strftime('%H:%M')
                except:
                    time_str = timestamp[:5] if timestamp else 'N/A'
                
                change_indicator = ""
                if change > 0:
                    change_indicator = f" ⬆️(+{change})"
                elif change < 0:
                    change_indicator = f" ⬇️({change})"
                
                history_lines.append(f"`{time_str}` - Serial: `{serial}`{change_indicator}")
            
            history_message = f"""
📈 **History for {stock_name}**

{chr(10).join(history_lines)}

📊 **Total Entries:** {len(history)}
⏰ **Showing:** Last {min(10, len(history))} entries
            """.strip()
            
            await update.message.reply_text(
                history_message,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            error_msg = format_error_message("History Failed", str(e))
            await update.message.reply_text(error_msg, parse_mode='Markdown')
    
    async def handle_stock_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle direct stock name queries"""
        try:
            stock_name = update.message.text.upper().strip()
            await self._process_stock_query(update, stock_name)
            
        except Exception as e:
            error_msg = format_error_message("Stock Query Failed", str(e))
            await update.message.reply_text(error_msg, parse_mode='Markdown')
    
    async def _process_stock_query(self, update: Update, stock_name: str):
        """Process stock query and send response"""
        try:
            # Search for stock
            stock_data = self.data_processor.search_stock(stock_name)
            
            if stock_data:
                # Format and send stock data
                formatted_data = format_stock_data(stock_data)
                await update.message.reply_text(
                    formatted_data,
                    parse_mode='Markdown'
                )
            else:
                # Stock not found, provide suggestions
                suggestions = self.data_processor.get_stock_suggestions(stock_name)
                
                if suggestions:
                    suggestion_text = "\n".join([f"• `{s}`" for s in suggestions[:5]])
                    message = f"""
❓ **Stock '{stock_name}' not found**

🔍 **Did you mean:**
{suggestion_text}

💡 Use /list to see all available stocks
                    """.strip()
                else:
                    message = f"""
❓ **Stock '{stock_name}' not found**

Make sure:
• The stock name is correct
• Data has been collected today
• Use /list to see available stocks
                    """.strip()
                
                await update.message.reply_text(
                    message,
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            raise e
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline keyboard button callbacks"""
        try:
            query = update.callback_query
            await query.answer()
            
            if query.data == "status":
                await self.status_command(update, context)
            elif query.data == "list":
                await self.list_stocks_command(update, context)
            elif query.data == "help":
                await self.help_command(update, context)
            elif query.data == "sample":
                # Send a sample query
                sample_message = """
📈 **Sample Stock Query**

Try sending me any of these:
• `RELIANCE`
• `TCS`
• `HDFC`
• `INFY`
• `/query WIPRO`
• `/history SBIN`

Just type the stock name or use the commands!
                """.strip()
                
                await query.edit_message_text(
                    sample_message,
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            logger.error(f"Button callback error: {e}")
    
    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors"""
        try:
            logger.error(f"Update {update} caused error {context.error}")
            
            if update and update.effective_message:
                error_msg = format_error_message(
                    "Bot Error", 
                    "An unexpected error occurred. Please try again."
                )
                await update.effective_message.reply_text(
                    error_msg,
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            logger.error(f"Error in error handler: {e}")
