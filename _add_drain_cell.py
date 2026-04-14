"""Script to insert a %pip install drain3 cell into the 02_log_parsing notebook."""
import json

notebook_path = r"d:\multimodal-rca-engine\notebooks\02_log_parsing.ipynb"

with open(notebook_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find the Drain parsing cell (the one with "Drain Parsing on HDFS Content")
drain_cell_index = None
for i, cell in enumerate(nb["cells"]):
    if cell["cell_type"] == "code":
        source_text = "".join(cell["source"])
        if "Drain Parsing on HDFS Content" in source_text:
            drain_cell_index = i
            break

if drain_cell_index is None:
    print("ERROR: Could not find the Drain parsing cell!")
else:
    # Create the new pip install cell
    pip_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Install drain3 in the notebook kernel's Python environment\n",
            "%pip install drain3"
        ]
    }
    
    # Insert before the Drain cell
    nb["cells"].insert(drain_cell_index, pip_cell)
    
    # Write back
    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    
    print(f"SUCCESS: Inserted %pip install drain3 cell at index {drain_cell_index}")
    print(f"The Drain parsing cell is now at index {drain_cell_index + 1}")
