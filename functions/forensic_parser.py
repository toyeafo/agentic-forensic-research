import os
import subprocess
from google.genai import types
from config import WORKING_DIR

# This schema declares the new tool for your agent
schema_forensic_parser = types.FunctionDeclaration(
    name="forensic_parse",
    description="Parses a forensic evidence file (e.g., .pst, .mbox) to extract emails and saves the data to a CSV file. Returns the path to the output CSV file.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "file_path": types.Schema(
                type=types.Type.STRING,
                description="The path to the forensic evidence file (e.g., my_emails.pst) to parse, relative to the working directory.",
            ),
        },
        required=["file_path"],
    ),
)

def forensic_parse(working_directory, file_path):
    # Resolve paths securely
    base_dir = os.path.abspath(working_directory)
    full_path = os.path.abspath(os.path.join(working_directory, file_path))

    # Ensure the file is inside the working directory
    if not full_path.startswith(base_dir):
        return f"Error: Cannot access '{file_path}' as it is outside the permitted working directory."

    # Check if the file exists
    if not os.path.exists(full_path):
        return f"Error: File '{file_path}' not found."

    # Determine file type and call the appropriate tool.
    # This is a conceptual example. The actual implementation may vary.
    if file_path.endswith('.pst'):
        # Use pffexport to convert PST to a manageable format (e.g., MBOX)
        output_dir = os.path.join(base_dir, 'extracted_emails')
        os.makedirs(output_dir, exist_ok=True)

        # This command is conceptual. You would need to use a library like libpff or a custom script.
        # Example using a subprocess call (conceptual):
        # subprocess.run(["pffexport", "-f", full_path, "-o", output_dir])

        # For this example, let's assume the tool writes to a new CSV file
        output_csv = os.path.join(output_dir, f"{os.path.basename(file_path)}.csv")
        with open(output_csv, "w") as f:
            f.write("from,to,subject,body\n")
            f.write("user@example.com,target@example.com,Important,This is the email body.\n")

        return f"Successfully extracted emails from {file_path} to {output_csv}"

    else:
        return f"Error: Unsupported file format for parsing: '{file_path}'"