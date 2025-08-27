"""
Utility functions for NSE Oi Spurts Bot
Common helper functions and formatting utilities
"""

import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import html

logger = logging.getLogger(__name__)

def normalize_stock_name(name: str) -> str:
    """
    Normalize stock name for consistent storage and comparison
    """
    if not name:
        return ""
    
    # Convert to uppercase and strip whitespace
    normalized = str(name).upper().strip()
    
    # Remove common suffixes and prefixes
    suffixes_to_remove = ['-EQ', '-BE', '-SM', '-ST', '.EQ', '.BE', '.SM', '.ST']
    for suffix in suffixes_to_remove:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
            break
    
    # Remove special characters except hyphens and underscores
    normalized = re.sub(r'[^\w\-_]', '', normalized)
    
    # Remove extra spaces and replace with single space
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def format_stock_data(stock_data: Dict) -> str:
    """
    Format stock data for Telegram display with proper markdown
    """
    try:
        name = stock_data.get('name', 'Unknown')
        serial = stock_data.get('serial_number', 'N/A')
        timestamp_str = stock_data.get('timestamp', '')
        change = stock_data.get('change', 0)
        
        # Format timestamp
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                time_str = timestamp.strftime('%H:%M:%S')
                date_str = timestamp.strftime('%Y-%m-%d')
            except:
                time_str = timestamp_str
                date_str = "Today"
        else:
            time_str = "Unknown"
            date_str = "Today"
        
        # Format change indicator
        change_emoji = ""
        change_text = ""
        if change > 0:
            change_emoji = "⬆️"
            change_text = f"(↗️ +{change})"
        elif change < 0:
            change_emoji = "⬇️"
            change_text = f"(↘️ {change})"
        else:
            change_emoji = "➡️"
            change_text = "(no change)"
        
        # Create formatted message
        message = f"""
📊 **Stock Data for {name}**

🏷️ **Serial Number:** `{serial}` {change_emoji}
📈 **Position Change:** {change_text}
⏰ **Last Updated:** `{time_str}`
📅 **Date:** `{date_str}`

💡 *Serial number indicates the stock's position in OI spurts ranking*
        """.strip()
        
        # Add additional data if available
        additional_info = []
        for key, value in stock_data.items():
            if key.startswith('additional_') and value:
                field_name = key.replace('additional_', '').replace('_', ' ').title()
                additional_info.append(f"📋 **{field_name}:** `{value}`")
        
        if additional_info:
            message += "\n\n**📊 Additional Data:**\n" + "\n".join(additional_info)
        
        return message
        
    except Exception as e:
        logger.error(f"❌ Error formatting stock data: {e}")
        return f"❌ **Error displaying data for {stock_data.get('name', 'Unknown Stock')}**"

def format_error_message(title: str, error: str) -> str:
    """
    Format error message for Telegram display
    """
    try:
        # Escape special characters for Markdown
        escaped_error = html.escape(str(error))
        
        message = f"""
❌ **{title}**

🔍 **Error Details:**
`{escaped_error[:200]}{'...' if len(escaped_error) > 200 else ''}`

💡 **Possible Solutions:**
• Try again in a few moments
• Check if the market is open
• Use /status to check bot health
• Contact support if issue persists

⏰ **Timestamp:** `{datetime.now().strftime('%H:%M:%S')}`
        """.strip()
        
        return message
        
    except Exception as e:
        logger.error(f"Error formatting error message: {e}")
        return f"❌ **{title}**\n\nAn error occurred. Please try again."

def calculate_time_difference(start_time: datetime, end_time: datetime) -> str:
    """
    Calculate and format time difference in human-readable format
    """
    try:
        diff = end_time - start_time
        
        days = diff.days
        hours, remainder = divmod(diff.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        
        if not parts:  # Less than a minute
            parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
        
        return ", ".join(parts)
        
    except Exception as e:
        logger.error(f"Error calculating time difference: {e}")
        return "Unknown"

def format_stock_list(stocks: List[Dict], page_size: int = 20) -> List[str]:
    """
    Format list of stocks into paginated pages
    """
    try:
        if not stocks:
            return ["📭 **No stocks available**\n\nNo data has been collected today yet."]
        
        # Sort stocks alphabetically
        sorted_stocks = sorted(stocks, key=lambda x: x.get('name', ''))
        
        pages = []
        for i in range(0, len(sorted_stocks), page_size):
            page_stocks = sorted_stocks[i:i + page_size]
            
            stock_lines = []
            for j, stock in enumerate(page_stocks, i + 1):
                name = stock.get('name', 'Unknown')
                serial = stock.get('serial_number', 'N/A')
                change = stock.get('change', 0)
                
                change_indicator = ""
                if change > 0:
                    change_indicator = " ⬆️"
                elif change < 0:
                    change_indicator = " ⬇️"
                
                stock_lines.append(f"`{j:2d}.` **{name}** - Serial: `{serial}`{change_indicator}")
            
            page_num = (i // page_size) + 1
            total_pages = (len(sorted_stocks) + page_size - 1) // page_size
            
            page_content = f"""
📋 **Available Stocks** (Page {page_num}/{total_pages})

{chr(10).join(stock_lines)}

📊 **Total Stocks:** {len(sorted_stocks)}
⏰ **Last Updated:** {datetime.now().strftime('%H:%M:%S')}
            """.strip()
            
            pages.append(page_content)
        
        return pages
        
    except Exception as e:
        logger.error(f"Error formatting stock list: {e}")
        return [f"❌ **Error formatting stock list**\n\n`{str(e)}`"]

def validate_stock_name(name: str) -> bool:
    """
    Validate if a stock name is properly formatted
    """
    if not name or not isinstance(name, str):
        return False
    
    name = name.strip()
    
    # Check length
    if len(name) < 2 or len(name) > 20:
        return False
    
    # Check for invalid characters
    if not re.match(r'^[A-Z0-9\-_&]+$', name.upper()):
        return False
    
    # Check for common invalid patterns
    invalid_patterns = [
        r'^\d+$',  # Only numbers
        r'^[^A-Z]+$',  # No letters
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, name.upper()):
            return False
    
    return True

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe file system storage
    """
    # Remove or replace dangerous characters
    safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove control characters
    safe_filename = ''.join(char for char in safe_filename if ord(char) >= 32)
    
    # Limit length
    if len(safe_filename) > 200:
        name, ext = safe_filename.rsplit('.', 1) if '.' in safe_filename else (safe_filename, '')
        safe_filename = name[:200-len(ext)-1] + ('.' + ext if ext else '')
    
    return safe_filename

def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    Parse timestamp string with multiple format support
    """
    if not timestamp_str:
        return None
    
    formats = [
        '%Y-%m-%dT%H:%M:%S.%f',  # ISO format with microseconds
        '%Y-%m-%dT%H:%M:%S',     # ISO format
        '%Y-%m-%d %H:%M:%S',     # Standard format
        '%Y-%m-%d %H:%M',        # Without seconds
        '%H:%M:%S',              # Time only
        '%H:%M'                  # Time only without seconds
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    # Try ISO format parsing
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except:
        pass
    
    logger.warning(f"Could not parse timestamp: {timestamp_str}")
    return None

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format
    """
    try:
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/(1024**2):.1f} MB"
        else:
            return f"{size_bytes/(1024**3):.1f} GB"
    except:
        return "Unknown size"

def truncate_text(text: str, max_length: int = 100) -> str:
    """
    Truncate text to specified length with ellipsis
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length-3] + "..."

def escape_markdown(text: str) -> str:
    """
    Escape markdown special characters
    """
    if not text:
        return ""
    
    # Characters that need escaping in Telegram markdown
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    
    escaped = str(text)
    for char in special_chars:
        escaped = escaped.replace(char, f'\\{char}')
    
    return escaped
