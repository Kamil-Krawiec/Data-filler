import ast
import os

def extract_docstrings(file_path):
    """
    Extracts class and function docstrings from a Python file.

    Args:
        file_path (str): Path to the Python file.

    Returns:
        dict: A dictionary with class and function names as keys and their docstrings as values.
    """
    with open(file_path, 'r') as file:
        tree = ast.parse(file.read(), filename=file_path)

    docstrings = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            docstrings[node.name] = ast.get_docstring(node)
        elif isinstance(node, ast.FunctionDef):
            docstrings[node.name] = ast.get_docstring(node)

    return docstrings

def generate_readme(docstrings):
    """
    Generates a README.md content from extracted docstrings.

    Args:
        docstrings (dict): Dictionary of class and function docstrings.

    Returns:
        str: Generated README.md content.
    """
    readme_content = "# Data Generation Tool\n\n"
    readme_content += "## Overview\n\nThis project provides a data generation tool that creates synthetic data for database tables based on predefined schemas and constraints.\n\n"

    readme_content += "## Classes and Functions\n\n"
    for name, doc in docstrings.items():
        readme_content += f"### `{name}`\n"
        readme_content += f"{doc}\n\n"

    readme_content += "## Example\n\nHere's a simple example of how to use the data generation tool:\n\n"
    readme_content += "```python\n# Example usage\n# (Add an example here)\n```\n"

    readme_content += "## License\n\nThis project is licensed under the MIT License.\n\n"
    readme_content += "## Contributing\n\nContributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.\n"

    return readme_content

# Specify the directory containing your Python files
directory = './parsing'  # Update this with your actual directory path
all_docstrings = {}

# Loop through each Python file in the directory
for filename in os.listdir(directory):
    if filename.endswith('.py'):
        file_path = os.path.join(directory, filename)
        docstrings = extract_docstrings(file_path)
        all_docstrings.update(docstrings)

# Generate the README content from all docstrings
readme_content = generate_readme(all_docstrings)

# Write the README content to a file
with open('READMEss.md', 'w') as readme_file:
    readme_file.write(readme_content)

print("README.md has been generated.")