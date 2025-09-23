# add this import statement
from functions.forensic_parser import forensic_parse, schema_forensic_parser

available_functions = types.Tool(
    function_declarations=[
        schema_get_files_info,
        schema_get_file_content,
        schema_write_file,
        schema_run_python_file,
        # Add your new schema here
        schema_forensic_parser
    ]
)

def call_function(function_call_part, verbose=False):
    # . . . (existing code) . . .

    function_map = {
        "write_file": write_file,
        "get_files_info": get_files_info,
        "get_file_content": get_file_content,
        "run_python_file": run_python_file,
        # Add your new function to the map
        "forensic_parse": forensic_parse,
    }

    # . . . (existing code) . . .