"""
Data Processor for NSE OI Spurts Bot
Handles Excel file processing, data extraction, and storage
"""

import logging
import os
import json
import pandas as pd
from datetime import datetime, date, time
from typing import Dict, List, Optional, Any, Tuple
import glob
from collections import defaultdict

from config import Config
from utils import normalize_stock_name, calculate_time_difference

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles processing and storage of NSE OI spurts data"""
    
    def __init__(self):
        self.config = Config()
        self.daily_data = defaultdict(list)  # Stock name -> list of entries
        self.current_date = date.today().isoformat()
        self.stats = {
            'successful_updates': 0,
            'failed_updates': 0,
            'total_stocks_processed': 0,
            'start_time': datetime.now().isoformat()
        }
        self.load_daily_data()
    
    def process_excel_file(self, file_path: str) -> Optional[Dict]:
        """
        Process downloaded Excel file and extract stock data
        Returns dict with processing results
        """
        try:
            logger.info(f"📊 Processing Excel file: {os.path.basename(file_path)}")
            
            # Check if file exists and is not empty
            if not os.path.exists(file_path):
                logger.error(f"❌ File not found: {file_path}")
                return None
                
            file_size = os.path.getsize(file_path)
            if file_size < 100:
                logger.error(f"❌ File too small ({file_size} bytes): {file_path}")
                return None
            
            # Try to read the Excel file
            df = self._read_excel_file(file_path)
            if df is None or df.empty:
                logger.error("❌ Could not read Excel file or file is empty")
                self.stats['failed_updates'] += 1
                return None
            
            # Extract stock data
            stock_entries = self._extract_stock_data(df, file_path)
            if not stock_entries:
                logger.warning("⚠️  No stock data extracted from file")
                self.stats['failed_updates'] += 1
                return None
            
            # Store the processed data
            timestamp = datetime.now().isoformat()
            processed_count = self._store_stock_data(stock_entries, timestamp)
            
            # Update statistics
            self.stats['successful_updates'] += 1
            self.stats['total_stocks_processed'] += processed_count
            
            # Save daily data
            self._save_daily_data()
            
            result = {
                'success': True,
                'file_path': file_path,
                'timestamp': timestamp,
                'stocks_processed': processed_count,
                'total_rows': len(df),
                'file_size': file_size
            }
            
            logger.info(f"✅ Successfully processed {processed_count} stocks from Excel file")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error processing Excel file {file_path}: {e}")
            self.stats['failed_updates'] += 1
            return None
    
    def _read_excel_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Read Excel file with multiple fallback strategies"""
        try:
            # First, try to read as Excel
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                if not df.empty:
                    logger.debug("📋 Successfully read as Excel file")
                    return df
            except Exception as e:
                logger.debug(f"Failed to read as Excel: {e}")
            
            # Try with xlrd engine for older Excel files
            try:
                df = pd.read_excel(file_path, engine='xlrd')
                if not df.empty:
                    logger.debug("📋 Successfully read as legacy Excel file")
                    return df
            except Exception as e:
                logger.debug(f"Failed to read as legacy Excel: {e}")
            
            # Try to read as CSV (sometimes NSE serves CSV with .xlsx extension)
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    logger.debug("📋 Successfully read as CSV file")
                    return df
            except Exception as e:
                logger.debug(f"Failed to read as CSV: {e}")
            
            # Try to read with different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    if not df.empty:
                        logger.debug(f"📋 Successfully read as CSV with {encoding} encoding")
                        return df
                except Exception:
                    continue
            
            logger.error("❌ Could not read file with any method")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error reading file: {e}")
            return None
    
    def _extract_stock_data(self, df: pd.DataFrame, file_path: str) -> List[Dict]:
        """Extract stock names and serial numbers from DataFrame"""
        try:
            stock_entries = []
            
            logger.debug(f"📊 DataFrame shape: {df.shape}")
            logger.debug(f"📊 DataFrame columns: {list(df.columns)}")
            
            # Print first few rows for debugging
            logger.debug("📊 Sample data:")
            for i, row in df.head(3).iterrows():
                logger.debug(f"  Row {i}: {dict(row)}")
            
            # Common patterns for NSE OI spurts data columns
            symbol_columns = ['SYMBOL', 'Symbol', 'symbol', 'STOCK', 'Stock', 'stock', 
                            'SCRIP', 'Scrip', 'scrip', 'NAME', 'Name', 'name',
                            'COMPANY', 'Company', 'company', 'INSTRUMENT', 'Instrument']
            
            # Find the symbol/stock name column
            symbol_col = None
            for col in df.columns:
                if any(pattern in str(col).upper() for pattern in ['SYMBOL', 'STOCK', 'SCRIP', 'NAME']):
                    symbol_col = col
                    break
                    
            if symbol_col is None and len(df.columns) > 0:
                # Use first column as symbol if no match found
                symbol_col = df.columns[0]
                logger.warning(f"⚠️  No symbol column found, using first column: {symbol_col}")
            
            if symbol_col is None:
                logger.error("❌ No suitable symbol column found")
                return []
            
            logger.info(f"📈 Using column '{symbol_col}' for stock symbols")
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    # Get stock symbol/name
                    stock_name = str(row[symbol_col]).strip().upper()
                    
                    # Skip invalid entries
                    if (not stock_name or 
                        stock_name in ['NAN', 'NULL', '', 'NONE'] or 
                        len(stock_name) < 2 or
                        stock_name.isdigit()):
                        continue
                    
                    # Clean stock name
                    stock_name = normalize_stock_name(stock_name)
                    
                    # Serial number is the row index + 1 (1-based indexing)
                    serial_number = index + 1
                    
                    entry = {
                        'name': stock_name,
                        'serial_number': serial_number,
                        'row_index': index,
                        'source_file': os.path.basename(file_path)
                    }
                    
                    # Try to extract additional data if available
                    for col in df.columns:
                        col_name = str(col).upper()
                        if any(keyword in col_name for keyword in ['OI', 'INTEREST', 'VOLUME', 'PRICE', 'CHANGE']):
                            try:
                                value = row[col]
                                if pd.notna(value) and value != '':
                                    entry[f'additional_{col_name.lower()}'] = str(value)
                            except:
                                pass
                    
                    stock_entries.append(entry)
                    
                except Exception as e:
                    logger.debug(f"⚠️  Error processing row {index}: {e}")
                    continue
            
            logger.info(f"✅ Extracted {len(stock_entries)} stock entries")
            return stock_entries
            
        except Exception as e:
            logger.error(f"❌ Error extracting stock data: {e}")
            return []
    
    def _store_stock_data(self, stock_entries: List[Dict], timestamp: str) -> int:
        """Store processed stock data in daily storage"""
        try:
            processed_count = 0
            current_date = date.today().isoformat()
            
            # Clear old data if date changed
            if self.current_date != current_date:
                logger.info(f"📅 Date changed from {self.current_date} to {current_date}, clearing old data")
                self.daily_data.clear()
                self.current_date = current_date
            
            for entry in stock_entries:
                stock_name = entry['name']
                
                # Create storage entry
                storage_entry = {
                    'timestamp': timestamp,
                    'serial_number': entry['serial_number'],
                    'source_file': entry['source_file'],
                    'date': current_date
                }
                
                # Add additional data if available
                for key, value in entry.items():
                    if key.startswith('additional_'):
                        storage_entry[key] = value
                
                # Calculate change from previous entry if available
                if stock_name in self.daily_data and self.daily_data[stock_name]:
                    prev_serial = self.daily_data[stock_name][-1]['serial_number']
                    storage_entry['change'] = entry['serial_number'] - prev_serial
                else:
                    storage_entry['change'] = 0
                
                self.daily_data[stock_name].append(storage_entry)
                processed_count += 1
            
            logger.info(f"💾 Stored data for {processed_count} stocks")
            return processed_count
            
        except Exception as e:
            logger.error(f"❌ Error storing stock data: {e}")
            return 0
    
    def search_stock(self, query: str) -> Optional[Dict]:
        """Search for stock data by name"""
        try:
            query = normalize_stock_name(query)
            
            # Exact match first
            if query in self.daily_data and self.daily_data[query]:
                latest_entry = self.daily_data[query][-1]
                return self._format_stock_result(query, latest_entry)
            
            # Partial match
            for stock_name in self.daily_data:
                if query in stock_name or stock_name.startswith(query):
                    latest_entry = self.daily_data[stock_name][-1]
                    return self._format_stock_result(stock_name, latest_entry)
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error searching for stock '{query}': {e}")
            return None
    
    def _format_stock_result(self, stock_name: str, entry: Dict) -> Dict:
        """Format stock data for display"""
        try:
            result = {
                'name': stock_name,
                'serial_number': entry['serial_number'],
                'timestamp': entry['timestamp'],
                'date': entry['date'],
                'change': entry.get('change', 0),
                'source_file': entry['source_file']
            }
            
            # Add additional data
            for key, value in entry.items():
                if key.startswith('additional_'):
                    result[key] = value
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error formatting stock result: {e}")
            return {}
    
    def get_stock_history(self, stock_name: str) -> List[Dict]:
        """Get historical data for a stock"""
        try:
            stock_name = normalize_stock_name(stock_name)
            return self.daily_data.get(stock_name, [])
        except Exception as e:
            logger.error(f"❌ Error getting stock history: {e}")
            return []
    
    def get_all_stocks_today(self) -> List[Dict]:
        """Get all stocks with their latest data"""
        try:
            stocks = []
            for stock_name, entries in self.daily_data.items():
                if entries:
                    latest = entries[-1]
                    stocks.append({
                        'name': stock_name,
                        'serial_number': latest['serial_number'],
                        'timestamp': latest['timestamp'],
                        'change': latest.get('change', 0)
                    })
            return stocks
        except Exception as e:
            logger.error(f"❌ Error getting all stocks: {e}")
            return []
    
    def get_stock_suggestions(self, query: str) -> List[str]:
        """Get stock name suggestions based on partial query"""
        try:
            query = normalize_stock_name(query)
            suggestions = []
            
            for stock_name in self.daily_data:
                if query in stock_name:
                    suggestions.append(stock_name)
            
            return sorted(suggestions)[:10]  # Return top 10 matches
            
        except Exception as e:
            logger.error(f"❌ Error getting stock suggestions: {e}")
            return []
    
    def get_bot_status(self) -> Dict:
        """Get comprehensive bot status information"""
        try:
            now = datetime.now()
            current_time = now.time()
            
            # Check if in market hours
            in_market_hours = (self.config.MONITORING_START_TIME <= current_time <= self.config.MONITORING_END_TIME)
            
            # Count files today
            today_str = date.today().strftime('%Y%m%d')
            excel_files = glob.glob(os.path.join(self.config.EXCEL_DIR, f"*{today_str}*.xlsx"))
            excel_files.extend(glob.glob(os.path.join(self.config.EXCEL_DIR, f"*{today_str}*.xls")))
            excel_files.extend(glob.glob(os.path.join(self.config.EXCEL_DIR, f"*{today_str}*.csv")))
            
            # Calculate uptime
            start_time = datetime.fromisoformat(self.stats['start_time'])
            uptime = calculate_time_difference(start_time, now)
            
            # Get last update time
            last_update = None
            if self.daily_data:
                all_entries = []
                for entries in self.daily_data.values():
                    all_entries.extend(entries)
                if all_entries:
                    latest_entry = max(all_entries, key=lambda x: x['timestamp'])
                    last_update = latest_entry['timestamp']
            
            return {
                'is_active': in_market_hours,
                'in_market_hours': in_market_hours,
                'last_update': last_update,
                'files_today': len(excel_files),
                'total_stocks': len(self.daily_data),
                'successful_updates': self.stats['successful_updates'],
                'failed_updates': self.stats['failed_updates'],
                'uptime': uptime,
                'current_date': self.current_date,
                'next_update': self._calculate_next_update() if in_market_hours else 'Market closed'
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting bot status: {e}")
            return {
                'is_active': False,
                'error': str(e)
            }
    
    def _calculate_next_update(self) -> str:
        """Calculate when the next update should occur"""
        try:
            now = datetime.now()
            next_update = now.replace(minute=(now.minute // 20 + 1) * 20, second=0, microsecond=0)
            
            # If next update is after market hours, return end of market message
            if next_update.time() > self.config.MONITORING_END_TIME:
                return "After market hours"
            
            return next_update.strftime('%H:%M:%S')
            
        except Exception as e:
            return "Unknown"
    
    def _save_daily_data(self):
        """Save daily data to file"""
        try:
            data_file = os.path.join(self.config.PROCESSED_DATA_DIR, f"daily_data_{self.current_date}.json")
            
            # Convert defaultdict to regular dict for JSON serialization
            data_to_save = {
                'date': self.current_date,
                'stats': self.stats,
                'stocks': dict(self.daily_data)
            }
            
            with open(data_file, 'w') as f:
                json.dump(data_to_save, f, indent=2)
                
            logger.debug(f"💾 Saved daily data to {data_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving daily data: {e}")
    
    def load_daily_data(self):
        """Load daily data from file"""
        try:
            current_date = date.today().isoformat()
            data_file = os.path.join(self.config.PROCESSED_DATA_DIR, f"daily_data_{current_date}.json")
            
            if os.path.exists(data_file):
                with open(data_file, 'r') as f:
                    data = json.load(f)
                
                if data.get('date') == current_date:
                    self.daily_data = defaultdict(list, data.get('stocks', {}))
                    self.stats.update(data.get('stats', {}))
                    logger.info(f"📂 Loaded daily data with {len(self.daily_data)} stocks")
                else:
                    logger.info("📅 Data file is from different date, starting fresh")
            else:
                logger.info("📂 No existing daily data file found, starting fresh")
                
        except Exception as e:
            logger.error(f"❌ Error loading daily data: {e}")
            # Continue with empty data
            self.daily_data = defaultdict(list)
    
    def cleanup_old_data(self, days_to_keep: int = 7):
        """Clean up old data files"""
        try:
            current_date = date.today()
            data_files = glob.glob(os.path.join(self.config.PROCESSED_DATA_DIR, "daily_data_*.json"))
            
            for file_path in data_files:
                try:
                    filename = os.path.basename(file_path)
                    date_str = filename.replace('daily_data_', '').replace('.json', '')
                    file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    if (current_date - file_date).days > days_to_keep:
                        os.remove(file_path)
                        logger.info(f"🗑️  Removed old data file: {filename}")
                        
                except Exception as e:
                    logger.warning(f"⚠️  Error processing file {file_path}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error during data cleanup: {e}")
