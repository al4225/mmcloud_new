import pandas as pd
import os
from io import StringIO

# Parse the table from the provided content
table_content = """# Complete eQTL Data Table with Synapse IDs
| Data Type | Cohort | Modality | Method | Path | Synapse ID |
|-----------|--------|----------|--------|------|------------|
| pQTL | KNIGHT | Brain | LR | analysis_result/marginal_significant_qtl/cis_association/KNIGHT/pQTL/Brain/LR/ | syn66506122 |
| pQTL | MSBB | - | LR | analysis_result/marginal_significant_qtl/cis_association/MSBB/pQTL/LR/ | syn66506119 |
| pQTL | ROSMAP | DLPFC | LR | analysis_result/marginal_significant_qtl/cis_association/ROSMAP/pQTL/DLPFC/LR/ | syn66506111 |
"""

# Convert the markdown table to a pandas DataFrame
lines = table_content.strip().split('\n')
headers = []
for line in lines:
    if line.startswith('|') and not line.startswith('|-'):
        # Extract header from the first row that has pipe symbols
        headers = [h.strip() for h in line.split('|') if h.strip()]
        break

rows = []
table_start = False
for line in lines:
    # Skip non-table rows and separator row
    if not line.startswith('|'):
        continue
    if line.startswith('|-'):
        table_start = True
        continue
    if table_start:
        row = [cell.strip() for cell in line.split('|') if cell.strip()]
        if len(row) == len(headers):
            rows.append(row)

df = pd.DataFrame(rows, columns=headers)

# Save as TSV file
df.to_csv("xqtl_results_summary.tsv", sep="\t", index=False)
print(f"Saved table to xqtl_results_summary.tsv")

# Create a mapping of path to synapse ID for quick lookup
path_to_synapse = dict(zip(df['Path'], df['Synapse ID']))

# Command template with optional synapse_id parameter
template = """python3 s3_handler_to_synapse.py --synid {synapse_id} --bucket statfungen \\
--path ftp_fgc_xqtl/{aws_path} --extensions gz txt --recursive --token-file synapse_token.txt"""
# Define output file name for shell script
output_file = "s3_to_synapse_commands_LR.sh"

# Generate shell script with commands
with open(output_file, "w") as f:
    f.write("#!/bin/bash\n\n")
    f.write("# Auto-generated commands for S3 to Synapse transfer\n\n")
    
    for index, row in df.iterrows():
        path = row['Path']
        synapse_id = row['Synapse ID']
        command = template.format(aws_path=path, synapse_id=synapse_id)
        f.write(f"{command}\n\n")

# Make the shell script executable
os.chmod(output_file, 0o755)

print(f"\nGenerated shell script: {output_file}")
print(f"The script contains {len(df)} commands")
print("You can run it with: ./s3_to_synapse_commands_LR.sh")