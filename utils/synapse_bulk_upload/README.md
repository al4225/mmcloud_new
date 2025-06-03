Here’s a polished version of your documentation for clarity, consistency, and professionalism:

---

# 📦 Bulk Upload of Data to Synapse

This script simplifies the creation of a `manifest.tsv` file, which serves as a metadata file indicating:

* The **local file paths** (on your computer or HPC)
* The **Synapse folder `parentId`** where each file should be uploaded

For complete details, refer to the [Synapse documentation](https://python-docs.synapse.org/en/stable/tutorials/python/upload_data_in_bulk/).

---

## 🔧 Usage Instructions

### 1. **Generate the Manifest File**

You can generate a manifest file by running:

```bash
python generate_manifest.py /path/to/data syn123
```

This scans the specified directory **recursively** and maps files to the given Synapse folder ID (`syn123`).

---

#### 🛠️ Example Commands

* **Default (recursive):**
  Scan all files in the folder and its subfolders.

  ```bash
  python generate_manifest.py /path/to/data syn123
  ```

* **Non-recursive:**
  Only include files in the specified folder (exclude subfolders).

  ```bash
  python generate_manifest.py /path/to/data syn123 --no-recursive
  ```

* **Append to existing manifest (non-recursive):**

  ```bash
  python generate_manifest.py /path/to/data syn456 manifest.tsv --append --no-recursive
  ```

* **Custom output filename (recursive by default):**

  ```bash
  python generate_manifest.py /path/to/data syn789 my_manifest.tsv
  ```


#### 📁 Example `manifest.tsv`

| path                                                                                     | parent      |
| ---------------------------------------------------------------------------------------- | ----------- |
| /home/user\_name/my\_ad\_project/single\_cell\_RNAseq\_batch\_2/SRR12345678\_R2.fastq.gz | syn60109537 |
| /home/user\_name/my\_ad\_project/single\_cell\_RNAseq\_batch\_2/SRR12345678\_R1.fastq.gz | syn60109537 |
| /home/user\_name/my\_ad\_project/biospecimen\_experiment\_2/fileD.txt                    | syn60109543 |
| /home/user\_name/my\_ad\_project/biospecimen\_experiment\_2/fileC.txt                    | syn60109543 |

---

### 2. **Upload Files in Bulk to Synapse**

Once your `manifest.tsv` is ready, upload the files using:

```bash
synapse sync manifest.tsv
```

This command will validate the manifest and upload all listed files to their respective Synapse folders.