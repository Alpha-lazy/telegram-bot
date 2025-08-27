# Overview

This is a Telegram bot that monitors NSE (National Stock Exchange) Options Interest (OI) spurts data from the official NSE India website. The bot automatically scrapes data at scheduled intervals during market hours (10:00 AM to 2:30 PM) and provides real-time stock market insights through Telegram commands and automated notifications.

The application downloads Excel files from NSE, processes the data to track stock movements and changes over time, and allows users to query this information through an interactive Telegram interface.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Backend Architecture
- **Python-based microservice architecture** with separate modules for different concerns
- **Event-driven design** using Telegram bot webhooks and scheduled tasks
- **Session-based web scraping** with proper request handling and retry mechanisms
- **In-memory data processing** with file-based persistence for daily stock data

## Core Components
- **Bot Handler (`bot_handler.py`)** - Manages all Telegram bot interactions, commands, and user interface
- **Data Processor (`data_processor.py`)** - Handles Excel file processing, data extraction, and storage with pandas
- **Scheduler (`scheduler.py`)** - Manages automated data collection during market hours using the schedule library
- **Scraper (`scraper.py`)** - Web scraper for NSE website with session management and error handling
- **Configuration (`config.py`)** - Centralized configuration management with environment variable support

## Data Processing Pipeline
- **Web scraping** from NSE India OI spurts page during market hours
- **Excel file download and parsing** using pandas for structured data extraction
- **Data normalization** with stock name standardization and duplicate handling
- **Time-series tracking** of stock changes throughout the trading day
- **File-based storage** in organized directory structure (data/excel_files, data/processed)

## Scheduling System
- **Market hours monitoring** (10:00 AM to 2:30 PM) with configurable intervals
- **20-minute scraping cycles** during active trading hours
- **Background task management** using threading for concurrent operations
- **Graceful shutdown handling** with signal management

## Error Handling & Resilience
- **Retry mechanisms** for failed web requests with exponential backoff
- **Request timeout management** (30-second default) to prevent hanging
- **File validation** to ensure downloaded Excel files are valid and non-empty
- **Comprehensive logging** with both file and console output

# External Dependencies

## Third-party Services
- **NSE India Website** - Primary data source for OI spurts information at https://www.nseindia.com/market-data/oi-spurts
- **Telegram Bot API** - For bot functionality and user interactions

## Python Libraries
- **python-telegram-bot** - Telegram bot framework for handling commands and callbacks
- **pandas** - Excel file processing and data manipulation
- **requests** - HTTP client for web scraping with session management
- **schedule** - Task scheduling for automated data collection
- **asyncio** - Asynchronous programming for bot operations

## Data Storage
- **Local file system** - Excel files stored in organized directory structure
- **JSON serialization** - For processed data persistence and configuration
- **No external database** - Uses in-memory storage with file-based persistence

## Configuration
- **Environment variables** - Bot token and other sensitive configuration
- **File-based configuration** - Structured settings in config.py
- **Directory auto-creation** - Automatic setup of required folder structure