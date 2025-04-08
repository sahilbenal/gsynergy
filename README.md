
# gsynergy

**Video Link:** [Link for the explanation video](https://drive.google.com/file/d/133hB3g202zpZ0QViZwDGMfJRM7xjoyWg/view?usp=drive_link)


# 🚀 Data Pipeline Deployment Guide

This guide provides a step-by-step walkthrough for setting up a data pipeline using **Azure Data Lake Storage Gen2**, **Azure Databricks**, and **Azure Data Factory (ADF)**.

---

## 📦 1. Set Up Azure Data Lake Storage (ADLS) Gen2

1. Create an **ADLS Gen2 storage account**.
2. Inside the storage account, create two containers:
   - `metadata`: Upload the provided `metadata.json` file.
   - `raw`: Upload all `.gz` data files into this container.

---

## 🛠️ 2. Update `metadata.json`

Update the `metadata.json` file with the **account key value** from the created storage account:
- Only the account key needs to be updated.
- Save and upload the updated `metadata.json` back into the `metadata` container.

---

## 💻 3. Set Up Databricks

1. Create a **Databricks Workspace**.
2. Launch a **cluster**.
3. Deploy the following notebooks:
   - `raw_to_stage`
   - `normalize_tables`
   - `final_table`

---

## 🔗 4. Set Up Azure Data Factory (ADF)

1. Create an **ADF workspace**.
2. Set up **linked services** for:
   - The **ADLS Gen2 storage account**
   - The **Databricks workspace**

🔍 Refer to the folder `pl_mstr_raw_to_stg_support_live` for:
- Linked services JSON files
- Dataset JSON definitions

---

## 🧪 5. Create ADF Pipeline

Create the pipeline named `pl_mstr_raw_to_stg` using the pipeline JSON from:
- `pipeline` folder inside `pl_mstr_raw_to_stg_support_live`

---

## ▶️ 6. Run Initial Pipeline

Run the `pl_mstr_raw_to_stg` pipeline with the following parameters:
- `container_name`: `metadata`
- `file_name`: `metadata.json`

✅ After execution, all **stage tables** will be created.

---

## 🧹 7. Normalize Dimension Tables

Use the `normalize_tables` notebook to normalize dimension tables.

Example Parameters:

new_table_primary_key: fsclwk_id  
secondary_dim_table: hier_wk  
src_table_foriegn_ke: fscldt_id  
src_table_name: hier_clnd  
unique_columns: fsclwk_id, fsclwk_label, fsclwoy

📌 Note: This step depends on the mview_weekly_sales gold table.

## 🧾 8. Create Final Output Table

Run all the cells in the `final_table` notebook to generate the final output table:

- **Final Table Name:** `mview_weekly_sales`
