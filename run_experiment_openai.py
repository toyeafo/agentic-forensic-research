import os
import time
import json
import sqlite3
from datetime import datetime
from openai import OpenAI  # Use standard OpenAI client for broad compatibility

# --- CONFIGURATION ---
API_KEY = "YOUR_API_KEY" # Set this or use os.environ.get("OPENAI_API_KEY")
DB_PATH = "experiment_data/sms.db"
LOG_DIR = "experiment_logs"
MODEL_NAME = "gpt-4o-mini" # CHEAPEST option (~$0.15 per 1M input tokens)

client = OpenAI(api_key=API_KEY)

# --- COST SAVING LIMITS ---
MAX_TOOL_OUTPUT_CHARS = 1000  # Truncate SQL results to save input tokens
MAX_RESPONSE_TOKENS = 300     # Prevent agent from writing long essays

def execute_sqlite_query(query):
    """Executes SQL with strict safety and truncation limits."""
    # Safety: Block modifications
    if any(x in query.lower() for x in ['drop', 'delete', 'insert', 'update', 'alter']):
        return "Error: Destructive commands prohibited."
    
    try:
        if not os.path.exists(DB_PATH):
            return f"Error: DB not found at {DB_PATH}"

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()
        
        # Format results
        result_str = f"Columns: {columns}\nData: {rows[:5]}" # Limit to 5 rows
        
        # TRUNCATE to save money
        if len(result_str) > MAX_TOOL_OUTPUT_CHARS:
            return result_str[:MAX_TOOL_OUTPUT_CHARS] + "...[TRUNCATED]"
        return result_str
        
    except Exception as e:
        return f"SQL Error: {str(e)}"

# Define Tools Schema for OpenAI
tools_schema = [
    {
        "type": "function",
        "function": {
            "name": "execute_sqlite_query",
            "description": "Run a read-only SQL query on the database.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute (e.g., SELECT * FROM users)"
                    }
                },
                "required": ["query"]
            }
        }
    }
]

def run_trial(workflow_name, system_prompt, user_query, trial_id):
    """Runs a minimal agent loop."""
    print(f"\n--- Starting Trial {trial_id}: {workflow_name} ---")
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_query}
    ]
    
    session_log = {
        "workflow": workflow_name,
        "trial_id": trial_id,
        "timestamp": datetime.now().isoformat(),
        "steps": []
    }

    # Turn 1: Agent thinks & calls tool
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            tools=tools_schema,
            tool_choice="auto",
            max_tokens=MAX_RESPONSE_TOKENS # Cap cost
        )
        
        response_msg = response.choices[0].message
        
        # Log Thought
        if response_msg.content:
            session_log["steps"].append({
                "type": "reasoning",
                "content": response_msg.content
            })
            print(f"Agent Thought: {response_msg.content}")

        # Check for Tool Call
        if response_msg.tool_calls:
            tool_call = response_msg.tool_calls[0] # Handle first tool only to save complexity
            fn_name = tool_call.function.name
            fn_args = json.loads(tool_call.function.arguments)
            
            session_log["steps"].append({
                "type": "tool_execution",
                "tool": fn_name,
                "args": fn_args
            })
            print(f"Tool Call: {fn_name}({fn_args})")
            
            # Execute Tool
            if fn_name == "execute_sqlite_query":
                tool_result = execute_sqlite_query(fn_args.get("query"))
                
                # Append result to conversation
                messages.append(response_msg)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_result
                })
                
                session_log["steps"].append({
                    "type": "tool_output",
                    "content": tool_result
                })

                # Turn 2: Agent Final Answer
                final_resp = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=messages,
                    max_tokens=MAX_RESPONSE_TOKENS
                )
                final_text = final_resp.choices[0].message.content
                session_log["final_answer"] = final_text
                print(f"Final Answer: {final_text}")

    except Exception as e:
        print(f"Error: {e}")
        session_log["error"] = str(e)

    # Save Log
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    with open(f"{LOG_DIR}/{workflow_name}_{trial_id}.json", "w") as f:
        json.dump(session_log, f, indent=4)

if __name__ == "__main__":
    # --- EXPERIMENT SETUP ---
    # Load your prompts here
    p_w1 = "You are a forensic assistant. Find the evidence directly using SQL."
    
    # Run 1 Trial
    run_trial("W1_Baseline", p_w1, "Find all email addresses in the db.", 1)
