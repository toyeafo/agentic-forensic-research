import google.generativeai as genai
import sqlite3
import json
import os
import time
from datetime import datetime

# --- CONFIGURATION ---
API_KEY = "YOUR_GEMINI_API_KEY" # Replace this
DB_PATH = "experiment_data/sms.db" # Target DB (Make sure this path is correct)
LOG_DIR = "experiment_logs"

# Configure Gemini
genai.configure(api_key=API_KEY)

# --- HELPER: Fix JSON Serialization Error ---
def make_serializable(obj):
    """Recursively converts Google's MapComposite/RepeatedComposite to dict/list."""
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_serializable(x) for x in obj]
    else:
        # Catch-all for Google's internal types (MapComposite, etc.)
        try:
            return dict(obj)
        except (ValueError, TypeError):
            return str(obj)

# Define the Tool (Non-destructive SQL)
def execute_sqlite_query(query):
    """Executes a read-only SQL query on the target database."""
    # Safety: Prevent modification commands
    if any(x in query.lower() for x in ['drop', 'delete', 'insert', 'update', 'alter']):
        return {"error": "Destructive commands are prohibited."}
    
    try:
        # Check if DB exists
        if not os.path.exists(DB_PATH):
            return {"error": f"Database file not found at {DB_PATH}"}

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        
        # Return formatted results as a structured dict
        # Limit to 20 rows to avoid token overflow
        return {
            "columns": columns,
            "row_count": len(rows),
            "data": rows[:20]
        }
    except Exception as e:
        return {"error": str(e)}

# Define the Tool for Gemini
tools = [execute_sqlite_query]

def run_agent_trial(workflow_name, system_prompt, user_query, trial_id):
    """Runs a single agent trial and saves the log."""
    
    # Use Flash to save quota, or Pro for better reasoning
    model = genai.GenerativeModel(
        model_name='gemini-1.5-flash', 
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
    
    try:
        # Send Query
        response = chat.send_message(user_query)
        final_answer = response.text
    except Exception as e:
        print(f"Error during API call: {e}")
        final_answer = f"CRASH: {str(e)}"

    # EXTRACT HISTORY FOR LOGGING
    # We iterate through the chat history to capture Thoughts vs Tools
    for message in chat.history:
        role = message.role
        for part in message.parts:
            step_data = {"role": role}
            
            # 1. Capture Text (Reasoning/Planning)
            if part.text:
                step_data["type"] = "reasoning"
                step_data["content"] = part.text.strip()
            
            # 2. Capture Function Calls (Tool Use)
            if part.function_call:
                step_data["type"] = "tool_execution"
                step_data["tool_name"] = part.function_call.name
                # FIX: Convert MapComposite to standard dict
                step_data["tool_args"] = make_serializable(part.function_call.args)
                
            # 3. Capture Function Responses (Observation)
            if part.function_response:
                step_data["type"] = "tool_output"
                step_data["tool_name"] = part.function_response.name
                # FIX: Convert MapComposite to standard dict
                step_data["content"] = make_serializable(part.function_response.response)
                
            session_log["steps"].append(step_data)

    # Save to JSON
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        
    filename = f"{LOG_DIR}/{workflow_name}_trial_{trial_id}.json"
    with open(filename, 'w') as f:
        json.dump(session_log, f, indent=4)
        
    print(f"[+] Log saved to {filename}")
    return final_answer

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    file_path = "prompt_w1.txt"
    with open(file_path, 'r') as file:
        prompt_w1 = file.read() 
    
    # Run 1 Trial
    # Remember to update the prompt with W1, W2, or W3 text as needed
    result = run_agent_trial("W1_Baseline", prompt_w1, "Find all email addresses in the database.", 1)

    print("Sleeping to respect rate limits")
    time.sleep(5)
    
    print("\nAgent Final Answer:")
    print(result)
