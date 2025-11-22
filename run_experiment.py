import google.generativeai as genai
import sqlite3
import json
import os
import time
from datetime import datetime

# --- CONFIGURATION ---
API_KEY = "YOUR_GEMINI_API_KEY" # Replace this
DB_PATH = "experiment_data/sms.db" # Target DB
LOG_DIR = "experiment_logs"

# Configure Gemini
genai.configure(api_key=API_KEY)

# Define the Tool (Non-destructive SQL)
def execute_sqlite_query(query):
    """Executes a read-only SQL query on the target database."""
    # Safety: Prevent modification commands
    if any(x in query.lower() for x in ['drop', 'delete', 'insert', 'update', 'alter']):
        return "Error: Destructive commands are prohibited."
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        
        # Return formatted results (limit to first 20 rows to prevent overflow)
        result_str = f"Columns: {columns}\nRows (First 20): {rows[:20]}"
        return result_str
    except Exception as e:
        return f"SQL Error: {str(e)}"

# Define the Tool for Gemini
tools = [execute_sqlite_query]

def run_agent_trial(workflow_name, system_prompt, user_query, trial_id):
    """Runs a single agent trial and saves the log."""
    
    model = genai.GenerativeModel(
        model_name='gemini-2.5-flash',
        tools=tools,
        system_instruction=system_prompt
    )
    
    chat = model.start_chat(enable_automatic_function_calling=True)
    
    # Metadata for the Log
    session_log = {
        "workflow": workflow_name,
        "trial_id": trial_id,
        "timestamp": datetime.now().isoformat(),
        "steps": []
    }
    
    print(f"\n--- Starting Trial {trial_id} ({workflow_name}) ---")
    
    # Send Query
    response = chat.send_message(user_query)
    
    # EXTRACT HISTORY FOR LOGGING
    # We iterate through the chat history to capture Thoughts vs Tools
    for message in chat.history:
        role = message.role
        for part in message.parts:
            step_data = {"role": role}
            
            # Capture Text (Reasoning/Planning)
            if part.text:
                step_data["type"] = "reasoning"
                step_data["content"] = part.text
            
            # Capture Function Calls (Tool Use)
            if part.function_call:
                step_data["type"] = "tool_execution"
                step_data["tool_name"] = part.function_call.name
                step_data["tool_args"] = dict(part.function_call.args)
                
            # Capture Function Responses (Observation)
            if part.function_response:
                step_data["type"] = "tool_output"
                step_data["content"] = part.function_response.response
                
            session_log["steps"].append(step_data)

    # Save to JSON
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        
    filename = f"{LOG_DIR}/{workflow_name}_trial_{trial_id}.json"
    with open(filename, 'w') as f:
        json.dump(session_log, f, indent=4)
        
    print(f"[+] Log saved to {filename}")
    return response.text

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    # Example: Loading Prompt W1 (You would load from file normally)
    prompt_w1 = "You are a forensic assistant. Find the evidence." 
    
    # Run 1 Trial
    result = run_agent_trial("W1_Baseline", prompt_w1, "Find all email addresses in the database.", 1)

    # PAUSE to respect rate limits
    print("Sleeping to respect API limits...")
    time.sleep(5) # Wait 5 seconds between trials
    
    print("Agent Final Answer:", result)
