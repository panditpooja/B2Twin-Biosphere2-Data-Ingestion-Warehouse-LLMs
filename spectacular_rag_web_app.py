# Spectacular Biosphere 2 RAG Web App with Amazing Colors
# Ultra-Vibrant Neon Cyberpunk Design

from flask import Flask, render_template_string, request, jsonify, send_from_directory
import json
import os
import threading
import time
from rag_database import Biosphere2RAGDatabase
from simple_interface import load_all_sensor_data, create_comprehensive_context
from anthropic import Anthropic
from dotenv import load_dotenv

# Load API key
load_dotenv()
api_key = os.getenv("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
if not api_key:
    print("[WARNING] ANTHROPIC_API_KEY not found in environment variables!")
    print("[INFO] Please set ANTHROPIC_API_KEY in Vercel/Render environment variables")
claude_client = Anthropic(api_key=api_key) if api_key else None

app = Flask(__name__)

# Create static folder if it doesn't exist
os.makedirs('static', exist_ok=True)

# Route to serve static files (images)
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

# Global variables
sensor_data = {}
context_summary = ""
data_loaded = False
rag_database = None
rag_ready = False

# Professional HTML Template
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RAG Analysis System</title>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        :root {
            --primary-color: #3b82f6;
            --primary-dark: #2563eb;
            --primary-light: #60a5fa;
            --secondary-color: #64748b;
            --success-color: #10b981;
            --warning-color: #f59e0b;
            --error-color: #ef4444;
            --bg-primary: #ffffff;
            --bg-secondary: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 50%, #f0f9ff 100%);
            --bg-tertiary: #f1f5f9;
            --text-primary: #0f172a;
            --text-secondary: #475569;
            --text-tertiary: #94a3b8;
            --border-color: #e2e8f0;
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
            --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
            --gradient-primary: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
            --gradient-success: linear-gradient(135deg, #10b981 0%, #059669 100%);
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-secondary);
            background-size: 200% 200%;
            animation: gradientShift 15s ease infinite;
            color: var(--text-primary);
            line-height: 1.6;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            min-height: 100vh;
        }
        
        @keyframes gradientShift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }
        
        /* Header */
        .header {
            background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
            border-bottom: 2px solid var(--border-color);
            padding: 40px 0;
            margin-bottom: 32px;
            box-shadow: var(--shadow-md);
            position: relative;
            overflow: hidden;
        }
        
        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: var(--gradient-primary);
        }
        
        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            padding: 0 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: relative;
            z-index: 1;
        }
        
        .header h1 {
            font-size: 36px;
            font-weight: 700;
            background: var(--gradient-primary);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            display: flex;
            align-items: center;
            gap: 22px;
            letter-spacing: -0.5px;
            transition: all 0.3s ease;
        }
        
        .header h1 i {
            background: var(--gradient-primary);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            font-size: 36px;
        }
        
        .header h1 .logo-image {
            height: 85px;
            width: auto;
            max-width: 600px;
            margin-right: 28px;
            vertical-align: middle;
            object-fit: contain;
            filter: drop-shadow(0 4px 12px rgba(0, 0, 0, 0.2));
            transition: transform 0.3s ease;
        }
        
        .header h1:hover .logo-image {
            transform: scale(1.02);
        }
        
        .header h1 .logo-fallback {
            display: none;
        }
        
        .header p {
            font-size: 15px;
            color: var(--text-secondary);
            margin-top: 6px;
            font-weight: 500;
        }
        
        /* Main Layout */
        .main-content {
            display: grid;
            grid-template-columns: 1fr 380px;
            gap: 24px;
            margin-bottom: 24px;
        }
        
        /* Cards */
        .card {
            background: var(--bg-primary);
            border-radius: 16px;
            padding: 28px;
            box-shadow: var(--shadow-lg);
            border: 1px solid var(--border-color);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        
        .card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: var(--gradient-primary);
            transform: scaleX(0);
            transition: transform 0.3s ease;
        }
        
        .card:hover {
            transform: translateY(-4px);
            box-shadow: var(--shadow-xl);
            border-color: var(--primary-light);
        }
        
        .card:hover::before {
            transform: scaleX(1);
        }
        
        .card h3 {
            font-size: 17px;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 24px;
            display: flex;
            align-items: center;
            gap: 10px;
            letter-spacing: -0.3px;
        }
        
        .card h3 i {
            background: var(--gradient-primary);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            font-size: 20px;
        }
        
        /* Chat Section */
        .chat-section {
            min-height: 600px;
            display: flex;
            flex-direction: column;
        }
        
        .chat-container {
            flex: 1;
            overflow-y: auto;
            padding: 24px;
            background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
            border-radius: 12px;
            margin-bottom: 20px;
            max-height: 600px;
            border: 1px solid var(--border-color);
            box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.05);
        }
        
        .chat-container::-webkit-scrollbar {
            width: 8px;
        }
        
        .chat-container::-webkit-scrollbar-track {
            background: var(--bg-tertiary);
            border-radius: 4px;
        }
        
        .chat-container::-webkit-scrollbar-thumb {
            background: var(--secondary-color);
            border-radius: 4px;
        }
        
        .chat-container::-webkit-scrollbar-thumb:hover {
            background: var(--text-secondary);
        }
        
        .message {
            margin-bottom: 24px;
            display: flex;
            flex-direction: column;
        }
        
        .message.user {
            align-items: flex-end;
        }
        
        .message.assistant {
            align-items: flex-start;
        }
        
        .message-content {
            max-width: 75%;
            padding: 12px 16px;
            border-radius: 12px;
            font-size: 14px;
            line-height: 1.6;
            word-wrap: break-word;
        }
        
        .message.user .message-content {
            background: var(--gradient-primary);
            color: white;
            border-bottom-right-radius: 4px;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        }
        
        .message.assistant .message-content {
            background: var(--bg-primary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            border-bottom-left-radius: 4px;
            box-shadow: var(--shadow-sm);
        }
        
        .message-time {
            font-size: 11px;
            color: var(--text-tertiary);
            margin-top: 6px;
            padding: 0 4px;
        }
        
        .message.user .message-time {
            text-align: right;
        }
        
        /* Input */
        .input-container {
            display: flex;
            gap: 12px;
            align-items: center;
        }
        
        .message-input {
            flex: 1;
            padding: 14px 18px;
            border: 2px solid var(--border-color);
            border-radius: 12px;
            font-size: 15px;
            font-family: inherit;
            background: var(--bg-primary);
            color: var(--text-primary);
            transition: all 0.3s ease;
            box-shadow: var(--shadow-sm);
        }
        
        .message-input:focus {
            outline: none;
            border-color: var(--primary-color);
            box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.15), var(--shadow-md);
            transform: translateY(-1px);
        }
        
        .message-input::placeholder {
            color: var(--text-tertiary);
        }
        
        .send-button {
            padding: 14px 28px;
            background: var(--gradient-primary);
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 16px;
            cursor: pointer;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            gap: 8px;
            font-weight: 600;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
        }
        
        .send-button:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
        }
        
        .send-button:active:not(:disabled) {
            transform: translateY(0);
        }
        
        .send-button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
        
        /* Status Indicator */
        .status-indicator {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 12px;
            background: var(--bg-secondary);
            border-radius: 8px;
            font-size: 14px;
        }
        
        .status-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--text-tertiary);
        }
        
        .status-dot.ready {
            background: var(--gradient-success);
            box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.2), 0 0 12px rgba(16, 185, 129, 0.4);
        }
        
        .status-dot.loading {
            background: linear-gradient(135deg, #f59e0b 0%, #f97316 100%);
            animation: pulse 2s infinite;
            box-shadow: 0 0 0 4px rgba(245, 158, 11, 0.2);
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
        }
        
        .stat-item {
            padding: 20px;
            background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
            border-radius: 12px;
            text-align: center;
            border: 1px solid rgba(59, 130, 246, 0.2);
            transition: all 0.3s ease;
        }
        
        .stat-item:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 16px rgba(59, 130, 246, 0.2);
            border-color: var(--primary-light);
        }
        
        .stat-value {
            font-size: 28px;
            font-weight: 800;
            background: var(--gradient-primary);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 6px;
            letter-spacing: -0.5px;
        }
        
        .stat-label {
            font-size: 12px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        /* Quick Questions */
        .quick-questions {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .quick-question {
            padding: 12px 16px;
            background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
            border: 1.5px solid var(--border-color);
            border-radius: 10px;
            font-size: 13px;
            color: var(--text-secondary);
            cursor: pointer;
            transition: all 0.3s ease;
            font-weight: 500;
        }
        
        .quick-question:hover {
            background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
            border-color: var(--primary-color);
            color: var(--primary-color);
            transform: translateX(4px);
            box-shadow: 0 4px 8px rgba(59, 130, 246, 0.15);
        }
        
        /* Typing Indicator */
        .typing-indicator {
            display: none;
            padding: 12px 16px;
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            border-bottom-left-radius: 4px;
            margin-bottom: 24px;
            max-width: 80px;
        }
        
        .typing-indicator.show {
            display: block;
        }
        
        .typing-dots {
            display: flex;
            gap: 6px;
        }
        
        .typing-dot {
            width: 8px;
            height: 8px;
            background: var(--text-tertiary);
            border-radius: 50%;
            animation: typing 1.4s infinite ease-in-out;
        }
        
        .typing-dot:nth-child(1) { animation-delay: -0.32s; }
        .typing-dot:nth-child(2) { animation-delay: -0.16s; }
        
        @keyframes typing {
            0%, 80%, 100% {
                transform: scale(0.8);
                opacity: 0.5;
            }
            40% {
                transform: scale(1);
                opacity: 1;
            }
        }
        
        /* Sources */
        .sources {
            margin-top: 20px;
            padding-top: 20px;
            border-top: 2px solid var(--border-color);
        }
        
        .sources-title {
            font-size: 13px;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .sources-title i {
            color: var(--primary-color);
        }
        
        .source-item {
            padding: 12px 16px;
            background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
            border-radius: 10px;
            font-size: 13px;
            color: var(--text-primary);
            margin-bottom: 10px;
            border-left: 4px solid var(--primary-color);
            transition: all 0.2s ease;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
        }
        
        .source-item:hover {
            background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
            transform: translateX(4px);
            box-shadow: 0 2px 8px rgba(59, 130, 246, 0.15);
        }
        
        .source-info {
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        
        .source-name {
            font-weight: 600;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .source-name i {
            color: var(--primary-color);
            font-size: 12px;
        }
        
        .source-type {
            font-size: 11px;
            color: var(--text-secondary);
            font-style: italic;
        }
        
        .source-score {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            background: var(--primary-color);
            color: white;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
            white-space: nowrap;
        }
        
        .source-score.high {
            background: var(--gradient-success);
        }
        
        .source-score.medium {
            background: linear-gradient(135deg, #f59e0b 0%, #f97316 100%);
        }
        
        .source-score.low {
            background: var(--secondary-color);
        }
        
        /* Loading Spinner */
        .loading-spinner {
            width: 16px;
            height: 16px;
            border: 2px solid var(--border-color);
            border-top-color: var(--primary-color);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        /* Responsive */
        @media (max-width: 1024px) {
            .main-content {
                grid-template-columns: 1fr;
            }
        }
        
        @media (max-width: 768px) {
            .container {
                padding: 16px;
            }
            
            .header-content {
                flex-direction: column;
                align-items: flex-start;
                gap: 12px;
            }
            
            .header h1 {
                font-size: 24px;
            }
            
            .message-content {
                max-width: 85%;
            }
            
            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
        <div class="header">
        <div class="header-content">
            <div>
                <h1>
                    <img src="/static/Biosphere_3_Wordmark_CLEAN.png" alt="Logo" class="logo-image" onerror="this.style.display='none'; this.nextElementSibling.style.display='inline-block';">
                    <i class="fas fa-brain logo-fallback" style="display: none;"></i> RAG Analysis
                </h1>
                <p>Retrieval-Augmented Generation System for Sensor Data Intelligence</p>
            </div>
        </div>
        </div>
        
    <div class="container">
        <div class="main-content">
            <div class="card chat-section">
                <div class="chat-container" id="chatContainer">
                    <div class="message assistant">
                        <div class="message-content">
                            Welcome to the RAG Analysis System. I'm your AI assistant powered by advanced retrieval-augmented generation technology.
                            <br><br>
                            <strong>I can help you with:</strong>
                            <ul style="margin: 12px 0; padding-left: 24px; color: var(--text-secondary);">
                                <li>Temperature and humidity analysis</li>
                                <li>Fan and valve system monitoring</li>
                                <li>Environmental data insights</li>
                                <li>Sensor data trends and patterns</li>
                                <li>System performance metrics</li>
                            </ul>
                            Ask me anything about your sensor data.
                        </div>
                        <div class="message-time" id="welcomeTime"></div>
                    </div>
                    
                    <div class="typing-indicator" id="typingIndicator">
                        <div class="typing-dots">
                            <div class="typing-dot"></div>
                            <div class="typing-dot"></div>
                            <div class="typing-dot"></div>
                        </div>
                    </div>
                </div>
                
                <div class="input-container">
                    <input type="text" class="message-input" id="messageInput" placeholder="Ask about your sensor data..." autocomplete="off" onkeypress="if(event.key==='Enter') sendMessage()">
                    <button class="send-button" id="sendButton" onclick="sendMessage()">
                        <i class="fas fa-paper-plane"></i>
                        <span>Send</span>
                    </button>
                </div>
            </div>
            
            <div class="sidebar">
                <div class="card">
                    <h3><i class="fas fa-circle-check"></i> System Status</h3>
                    <div class="status-indicator" id="systemStatus">
                        <div class="status-dot loading"></div>
                        <span>Initializing...</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3><i class="fas fa-chart-line"></i> Statistics</h3>
                    <div class="stats-grid" id="ragStats">
                        <div class="stat-item">
                            <div class="stat-value">-</div>
                            <div class="stat-label">Documents</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">-</div>
                            <div class="stat-label">Embeddings</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">-</div>
                            <div class="stat-label">Sensors</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">-</div>
                            <div class="stat-label">Readings</div>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3><i class="fas fa-lightbulb"></i> Quick Questions</h3>
                    <div class="quick-questions">
                        <div class="quick-question" onclick="askQuickQuestion('What is the average temperature?')">
                            Average temperature?
                        </div>
                        <div class="quick-question" onclick="askQuickQuestion('What is the temperature range?')">
                            Temperature range?
                        </div>
                        <div class="quick-question" onclick="askQuickQuestion('How many sensors are monitoring the system?')">
                            How many sensors?
                        </div>
                        <div class="quick-question" onclick="askQuickQuestion('What is the monitoring period?')">
                            Monitoring period?
                        </div>
                        <div class="quick-question" onclick="askQuickQuestion('How many sensor readings are there?')">
                            Total readings?
                        </div>
                        <div class="quick-question" onclick="askQuickQuestion('What is the highest temperature recorded?')">
                            Highest temperature?
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let isProcessing = false;
        
        document.addEventListener('DOMContentLoaded', function() {
            document.getElementById('welcomeTime').textContent = new Date().toLocaleTimeString();
            updateSystemStatus();
            updateRAGStats();
            
            setInterval(updateSystemStatus, 5000);
            setInterval(updateRAGStats, 10000);
        });
        
        function updateSystemStatus() {
            fetch('/api/system-status')
                .then(response => response.json())
                .then(data => {
                    const statusEl = document.getElementById('systemStatus');
                    const dot = statusEl.querySelector('.status-dot');
                    const text = statusEl.querySelector('span');
                    
                    if (data.rag_ready && data.data_loaded) {
                        dot.className = 'status-dot ready';
                        text.textContent = `Ready (${data.sensor_count} sensors)`;
                    } else if (data.data_loaded) {
                        dot.className = 'status-dot loading';
                        text.textContent = 'Building RAG index...';
                    } else {
                        dot.className = 'status-dot loading';
                        text.textContent = 'Loading data...';
                    }
                })
                .catch(error => console.log('Status update failed:', error));
        }
        
        function updateRAGStats() {
            fetch('/api/rag-stats')
                .then(response => response.json())
                .then(data => {
                    const statsEl = document.getElementById('ragStats');
                    const statItems = statsEl.querySelectorAll('.stat-value');
                    if (statItems.length >= 4) {
                        statItems[0].textContent = data.documents || 0;
                        statItems[1].textContent = data.embeddings || 0;
                        statItems[2].textContent = data.sensor_types || 0;
                        statItems[3].textContent = (data.total_readings || 0).toLocaleString();
                    }
                })
                .catch(error => console.log('Stats update failed:', error));
        }
        
        function sendMessage() {
            const input = document.getElementById('messageInput');
            const message = input.value.trim();
            
            if (!message || isProcessing) return;
            
            addMessage(message, 'user');
            input.value = '';
            showTypingIndicator();
            isProcessing = true;
            document.getElementById('sendButton').disabled = true;
            
            fetch('/api/ask', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: message })
            })
            .then(response => response.json())
            .then(data => {
                hideTypingIndicator();
                addMessage(data.answer, 'assistant', data.sources);
                isProcessing = false;
                document.getElementById('sendButton').disabled = false;
            })
            .catch(error => {
                hideTypingIndicator();
                addMessage('Sorry, I encountered an error. Please try again.', 'assistant');
                isProcessing = false;
                document.getElementById('sendButton').disabled = false;
            });
        }
        
        function addMessage(content, type, sources = []) {
            const container = document.getElementById('chatContainer');
            const messageDiv = document.createElement('div');
            messageDiv.className = `message ${type}`;
            
            const contentDiv = document.createElement('div');
            contentDiv.className = 'message-content';
            contentDiv.textContent = content;
            
            const timeDiv = document.createElement('div');
            timeDiv.className = 'message-time';
            timeDiv.textContent = new Date().toLocaleTimeString();
            
            messageDiv.appendChild(contentDiv);
            messageDiv.appendChild(timeDiv);
            
            if (sources && sources.length > 0) {
                const sourcesDiv = document.createElement('div');
                sourcesDiv.className = 'sources';
                sourcesDiv.innerHTML = '<div class="sources-title"><i class="fas fa-file-alt"></i> Sources (' + sources.length + ')</div>';
                
                sources.slice(0, 3).forEach((source, index) => {
                    const sourceDiv = document.createElement('div');
                    sourceDiv.className = 'source-item';
                    
                    // Extract readable sensor name from doc_id
                    let sensorName = source.doc_id || 'Unknown';
                    let chunkType = '';
                    
                    // Parse doc_id to get sensor name and chunk type
                    if (source.doc_id) {
                        // Format: sensor_name_chunk_type or sensor_name_summary
                        const parts = source.doc_id.split('_');
                        if (parts.length > 1) {
                            // Get chunk type (last part)
                            chunkType = parts[parts.length - 1];
                            // Get sensor name (everything except last part) and capitalize
                            sensorName = parts.slice(0, -1).join(' ').split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
                        } else {
                            sensorName = source.doc_id.replace(/_/g, ' ').split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
                        }
                    }
                    
                    // Get chunk type from metadata if available
                    if (source.metadata && source.metadata.chunk_type) {
                        chunkType = source.metadata.chunk_type;
                    }
                    
                    // Format chunk type for display
                    const chunkTypeLabels = {
                        'summary': 'Summary',
                        'sample': 'Sample Data',
                        'statistics': 'Statistics',
                        'overview': 'System Overview'
                    };
                    const displayChunkType = chunkTypeLabels[chunkType] || chunkType.charAt(0).toUpperCase() + chunkType.slice(1) || 'Data';
                    
                    // Get similarity score
                    const score = source.similarity_score || 0;
                    const scorePercent = Math.round(score * 100);
                    
                    // Determine score class
                    let scoreClass = 'low';
                    if (scorePercent >= 70) scoreClass = 'high';
                    else if (scorePercent >= 50) scoreClass = 'medium';
                    
                    // Create source info
                    const sourceInfo = document.createElement('div');
                    sourceInfo.className = 'source-info';
                    
                    const sourceName = document.createElement('div');
                    sourceName.className = 'source-name';
                    sourceName.innerHTML = '<i class="fas fa-sensor"></i> ' + sensorName;
                    
                    const sourceType = document.createElement('div');
                    sourceType.className = 'source-type';
                    sourceType.textContent = displayChunkType;
                    
                    sourceInfo.appendChild(sourceName);
                    sourceInfo.appendChild(sourceType);
                    
                    // Create score badge
                    const scoreBadge = document.createElement('div');
                    scoreBadge.className = 'source-score ' + scoreClass;
                    scoreBadge.innerHTML = '<i class="fas fa-check-circle"></i> ' + scorePercent + '%';
                    
                    sourceDiv.appendChild(sourceInfo);
                    sourceDiv.appendChild(scoreBadge);
                    sourcesDiv.appendChild(sourceDiv);
                });
                
                messageDiv.appendChild(sourcesDiv);
            }
            
            container.appendChild(messageDiv);
            
            // Scroll to start of assistant messages, bottom for user messages
            if (type === 'assistant') {
                // Scroll to the top of the assistant message
                setTimeout(() => {
                    messageDiv.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }, 100);
            } else {
                // Scroll to bottom for user messages
                container.scrollTop = container.scrollHeight;
            }
        }
        
        function showTypingIndicator() {
            document.getElementById('typingIndicator').classList.add('show');
            const container = document.getElementById('chatContainer');
            container.scrollTop = container.scrollHeight;
        }
        
        function hideTypingIndicator() {
            document.getElementById('typingIndicator').classList.remove('show');
        }
        
        function askQuickQuestion(question) {
            document.getElementById('messageInput').value = question;
            sendMessage();
        }
    </script>
</body>
</html>
"""

def load_data_background():
    """Load sensor data and initialize RAG database in background"""
    global sensor_data, context_summary, data_loaded, rag_database, rag_ready
    
    try:
        print("Loading Biosphere 2 sensor data...")
        sensor_data = load_all_sensor_data()
        context_summary = create_comprehensive_context(sensor_data)
        data_loaded = True
        print("[SUCCESS] Sensor data loaded!")
        
        print("Initializing RAG database...")
        rag_database = Biosphere2RAGDatabase()
        
        print("Creating document chunks...")
        chunks = rag_database.create_document_chunks(sensor_data)
        
        print("Adding documents to RAG database...")
        rag_database.add_documents(chunks)
        
        print("Building vector index...")
        rag_database.build_vector_index()
        
        rag_ready = True
        print("[SUCCESS] RAG database ready!")
        
    except Exception as e:
        print(f"[ERROR] Failed to initialize RAG system: {e}")
        rag_ready = False

@app.route('/')
def index():
    """Main page"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/system-status')
def get_system_status():
    """Get system status"""
    return jsonify({
        'data_loaded': data_loaded,
        'rag_ready': rag_ready,
        'sensor_count': len(sensor_data) if sensor_data else 0
    })

@app.route('/api/rag-stats')
def get_rag_stats():
    """Get RAG database statistics"""
    try:
        if rag_database:
            stats = rag_database.get_database_stats()
            
            # Calculate accurate sensor count and total readings from actual sensor data
            if sensor_data:
                # Count unique sensors (excluding errors)
                unique_sensors = len([s for s in sensor_data.keys() if 'error' not in sensor_data.get(s, {})])
                
                # Sum total readings from all sensors
                total_sensor_readings = sum(
                    sensor_data[s].get('total_readings', 0) 
                    for s in sensor_data.keys() 
                    if 'error' not in sensor_data.get(s, {})
                )
                
                # Update stats with accurate values
                stats['sensor_types'] = unique_sensors
                stats['total_readings'] = total_sensor_readings
            
            return jsonify(stats)
        else:
            return jsonify({
                'documents': 0,
                'embeddings': 0,
                'sensor_types': 0,
                'total_readings': 0
            })
    except Exception as e:
        print(f"[ERROR] Failed to get RAG stats: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'documents': 0,
            'embeddings': 0,
            'sensor_types': 0,
            'total_readings': 0
        })

@app.route('/api/ask', methods=['POST'])
def ask_question():
    """Ask a question using RAG system"""
    try:
        data = request.get_json()
        question = data.get('question', '')
        
        if not question:
            return jsonify({'answer': 'Please provide a question.', 'sources': []})
        
        if not rag_ready or not rag_database:
            return jsonify({
                'answer': 'RAG system is still initializing. Please wait a moment and try again.',
                'sources': []
            })
        
        # Use RAG system to answer with Claude API
        # Get RAG context - search more documents for better coverage
        search_results = rag_database.search(question, top_k=10)
        sources = search_results
        
        # If question is about humidity and initial search didn't find humidity data, do a fallback search
        question_lower = question.lower()
        if 'humidity' in question_lower or 'hum' in question_lower:
            # Check if we found humidity data
            has_humidity = any('hum' in str(result.get('doc_id', '')).lower() or 
                              'humidity' in str(result.get('content', '')).lower() 
                              for result in search_results)
            
            if not has_humidity:
                # Do a fallback search specifically for humidity
                humidity_search = rag_database.search("humidity sensor data mnthum lowlndhum", top_k=10)
                if humidity_search:
                    # Merge results, prioritizing original search but adding humidity results
                    existing_ids = {r.get('doc_id') for r in search_results}
                    for result in humidity_search:
                        if result.get('doc_id') not in existing_ids:
                            search_results.append(result)
                            sources.append(result)
        
        if search_results:
            # Build context from search results - increase context length
            # Rebuild context with updated search results
            rag_context = rag_database.get_context_for_question(question, max_context_length=5000)
            
            # If still no humidity in context for humidity questions, force include humidity summaries
            if ('humidity' in question_lower or 'hum' in question_lower) and 'hum' not in rag_context.lower():
                # Get all humidity-related documents directly
                cursor = rag_database.conn.cursor()
                cursor.execute('''
                    SELECT content, metadata FROM documents 
                    WHERE doc_id LIKE '%hum%' OR content LIKE '%humidity%' OR content LIKE '%mnthum%' OR content LIKE '%lowlndhum%'
                    LIMIT 5
                ''')
                humidity_docs = cursor.fetchall()
                if humidity_docs:
                    humidity_text = "\n\n".join([doc[0] for doc in humidity_docs[:3]])
                    rag_context = humidity_text + "\n\n" + rag_context
            
            # Enhanced prompt with RAG context
            prompt = f"""You're a Biosphere 2 environmental analyst. Give conversational, informative answers using the data provided.

DATA AVAILABLE:
{rag_context}

QUESTION: {question}

CRITICAL RULES:
1. ALWAYS search the DATA AVAILABLE section for the answer - look for numbers, values, ranges, averages
2. For humidity questions: The DATA AVAILABLE section MUST contain humidity data. Look for ANY text containing: "humidity", "hum", "mnthum", "lowlndhum", "tigrpndhum", "rftescosuphum", "tigrpndhum" - these are ALL humidity sensors. Search the ENTIRE DATA AVAILABLE section, not just the first few lines.
3. For temperature questions: Search for "temperature", "tmp", "satmp", "mnttmp", "lowlndtmp", "rftescosuptmp" - these are temperature sensors
4. If you find ANY relevant data with numbers, use it immediately - give the exact values
5. ROUND ALL NUMBERS for readability: temperatures to 1 decimal place (e.g., 76.1°F not 76.099999°F), humidity to whole numbers or 1 decimal (e.g., 87% or 87.3%), other values to 1-2 decimal places
6. Be conversational and natural - like explaining to a colleague, but keep it concise (2-3 sentences)
7. ABSOLUTELY FORBIDDEN: Never say "unfortunately", "data does not contain", "without direct measurements", "I cannot", "not enough information" - if you can't find it in the first search, search again in the DATA AVAILABLE section
8. Provide context - include both the answer and relevant supporting details (like ranges when discussing averages)

ANSWER STYLE:
- Be conversational and friendly, but professional
- Start with the direct answer, then add context
- Include relevant details like ranges, time periods, or patterns
- Use natural language, not robotic lists
- 2-3 sentences is ideal - informative but not verbose

HUMIDITY QUESTIONS - CRITICAL:
- The DATA AVAILABLE section below CONTAINS humidity data - you MUST find it
- Search for ANY mention of: "humidity", "hum", "mnthum", "lowlndhum", "tigrpndhum", "rftescosuphum", "tigrpndhum"
- These sensor names indicate humidity sensors: uab2_bio1_b4000_miscrf1_mnthum, uab2_bio1_b4000_miscrf1_lowlndhum, etc.
- Extract min, max, average values from the humidity sensor summaries or statistics
- ROUND values: humidity to whole numbers or 1 decimal place (e.g., 87% or 87.3%, not 87.345678%)
- Answer format: "Humidity ranges from [rounded_min]% to [rounded_max]%, averaging [rounded_avg]% according to the sensor data."
- If multiple sensors: "Humidity ranges from [rounded_lowest]% to [rounded_highest]% across sensors, with averages between [rounded_low_avg]% and [rounded_high_avg]%."
- If you truly cannot find humidity data after searching thoroughly, pivot gracefully: "The system monitors 51 sensors including temperature, valve controls, and fan systems. For humidity analysis, I can help you explore the temperature patterns which often correlate with environmental conditions."

TEMPERATURE QUESTIONS:
- Find temperature sensor data in DATA AVAILABLE
- Extract min, max, average values
- ROUND values: temperatures to 1 decimal place (e.g., 76.1°F not 76.099999°F, 61.4°F not 61.371917724609375°F)
- Answer format: "The average temperature is [rounded_avg]°F. The temperature ranges from [rounded_min]°F to [rounded_max]°F according to the sensor data provided."
- For "highest temperature": "The highest temperature recorded is [rounded_max]°F."
- For "lowest temperature": "The lowest temperature recorded is [rounded_min]°F."

SENSOR TYPES QUESTIONS:
- If asked about sensor types or what sensors are available, look for the system overview or summary in DATA AVAILABLE
- Answer should mention: temperature sensors, valve sensors, fan sensors, humidity sensors, CO2 sensors, etc.
- Format: "The system monitors [number] sensors including temperature, valve controls, fan systems, humidity, and CO2 sensors across the Biosphere 2 facility."
- DO NOT focus on one specific sensor - give an overview of all sensor categories

COMPARISON QUESTIONS WITH EXTERNAL DATA:
- If the question asks to compare Biosphere 2 with external locations (Amazon rainforest, Brazilian rainforest, other ecosystems, cities, etc.):
  1. FIRST: Provide the Biosphere 2 data from DATA AVAILABLE section with specific metrics (temperature, humidity, etc.)
  2. THEN: Clearly state: "I don't have access to [external location] sensor data in this database to make a direct comparison."
  3. EXPLAIN: "This system is specifically designed to analyze Biosphere 2 environmental monitoring data."
  4. OFFER: "I can help you explore the Biosphere 2 environmental patterns in more detail, including temperature trends, humidity variations, and correlations between different sensor systems."
  5. FORMAT: Be honest and helpful - provide what you can, acknowledge what you can't, never make up external data
- EXAMPLES OF GOOD COMPARISON ANSWERS:
  ✓ "Based on the Biosphere 2 sensor data, the rainforest area maintains temperatures ranging from 61.4°F to 94.5°F, averaging 76.1°F, with humidity levels between 65% and 95%. However, I don't have access to sensor data from the Amazon or Brazilian rainforests in this database to make a direct comparison. This system is specifically designed to analyze Biosphere 2 environmental monitoring data. I can help you explore the Biosphere 2 environmental patterns in more detail if that would be helpful."
  ✓ "The Biosphere 2 rainforest shows [specific metrics from data]. For comparison with natural rainforests like the Amazon or Brazilian rainforest, I'd need access to their sensor data, which isn't available in this system. Would you like me to provide more detailed analysis of the Biosphere 2 conditions?"
- EXAMPLES OF BAD COMPARISON ANSWERS:
  ✗ Making up Amazon/Brazilian data that doesn't exist (hallucination)
  ✗ Only giving Biosphere 2 data without acknowledging the comparison request
  ✗ Providing general knowledge about external locations without stating it's not from the database
  ✗ Ignoring the comparison part of the question entirely

EXAMPLES OF GOOD ANSWERS:
✓ "The average temperature is 76.1°F. The temperature ranges from 61.4°F to 94.5°F according to the sensor data provided."
✓ "The highest temperature recorded is 94.5°F."
✓ "There are 51 sensors monitoring the system, collecting a total of 697,453 readings from September 21-28, 2025."
✓ "The system monitors 51 sensors including temperature sensors, valve controls, fan systems, humidity sensors, and CO2 sensors across the Biosphere 2 facility."

ROUNDING RULES:
- Temperatures: Round to 1 decimal place (61.37°F → 61.4°F, 94.47°F → 94.5°F)
- Humidity: Round to whole number or 1 decimal (87.34% → 87.3% or 87%)
- Other percentages: Round to 1 decimal place
- Large numbers: Use appropriate precision (697,453 is fine as is)

EXAMPLES OF BAD ANSWERS (DON'T DO THIS):
✗ "The humidity range is not directly provided. However, we can infer..."
✗ "Unfortunately, the data does not contain..."
✗ "While this does not give us the exact range, we can make observations..."
✗ "Without direct measurements, we cannot provide precise values..."
✗ Just "76.10°F" (too short, no context)

REMEMBER: The data IS there - search harder, look for sensor names with "hum" or "tmp" in them, find the numbers, and give a natural, informative answer.
"""
            
            try:
                if not claude_client:
                    return jsonify({
                        'answer': 'Error: ANTHROPIC_API_KEY is not configured. Please set it in your environment variables.',
                        'sources': sources[:3] if sources else []
                    })
                
                # Get answer from Claude with RAG context
                response = claude_client.messages.create(
                    model="claude-3-haiku-20240307",  # Anthropic Claude model (Haiku - available with your API key)
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}]
                )
                answer = response.content[0].text
            except Exception as e:
                print(f"[ERROR] Claude API call failed: {e}")
                # Fallback to simple answer
                answer = f"Based on the sensor data, I found {len(search_results)} relevant sources. "
                answer += f"The top result shows: {search_results[0]['content'][:200]}..."
                answer += f"\n\n(Note: AI enhancement unavailable - {str(e)})"
        else:
            answer = "I couldn't find specific information about that in the sensor data. Please try rephrasing your question."
        
        return jsonify({
            'answer': answer,
            'sources': sources[:3] if sources else []
        })
        
    except Exception as e:
        print(f"[ERROR] Question processing failed: {e}")
        return jsonify({
            'answer': 'Sorry, I encountered an error processing your question. Please try again.',
            'sources': []
        })

if __name__ == '__main__':
    print("Starting Spectacular Biosphere 2 RAG-Powered Web Interface...")
    print("Loading sensor data and initializing RAG database...")
    print("Web interface will be available at: http://localhost:5000")
    print("Amazing neon cyberpunk visual effects loading...")
    
    # Start background data loading
    threading.Thread(target=load_data_background, daemon=True).start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)



