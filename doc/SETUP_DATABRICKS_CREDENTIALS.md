# Setting Up Databricks SQL Credentials

## ⚠️ Error Message

```
⚠️ --write-to-db flag set but Databricks SQL credentials not provided
   Set: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN
```

This means you're using `--write-to-db` but haven't set the required environment variables.

---

## 🔧 Solution

### **Step 1: Get Your Databricks SQL Warehouse Information**

1. **Go to your Databricks workspace**
2. **Navigate to SQL → SQL Warehouses**
3. **Click on your SQL Warehouse**
4. **Copy the Connection Details:**

#### **Server Hostname:**
```
adb-308529863339615.15.azuredatabricks.net
```
(This is your workspace URL)

#### **HTTP Path:**
```
/sql/1.0/warehouses/e02afccaa05e160c
```
(This is found in the connection details)

#### **Access Token:**
```
dapi...
```
(This is your Databricks personal access token or service principal token)

---

## 🔑 How to Get Your Access Token

### **Option 1: Personal Access Token**

1. Go to Databricks Workspace
2. Click on your profile → **User Settings**
3. Go to **Access Tokens** tab
4. Click **Generate New Token**
5. Copy the token (you can't see it again!)

### **Option 2: Service Principal Token**

If using service principal, use the token from Azure Key Vault or Azure Secrets.

---

## 📝 Setting Environment Variables

### **Linux/Mac:**

```bash
export DATABRICKS_SERVER_HOSTNAME="<DATABRICKS_SERVER_HOSTNAME>"
export DATABRICKS_HTTP_PATH="<DATABRICKS_HTTP_PATH>"se
export DATABRICKS_ACCESS_TOKEN="<DATABRICKS_ACCESS_TOKEN>"

# Verify they're set
echo $DATABRICKS_SERVER_HOSTNAME
echo $DATABRICKS_HTTP_PATH
echo $DATABRICKS_ACCESS_TOKEN

# Now run the executor
nuvyn-executor metadata_extraction /path/to/data csv tenant123 --write-to-db
```

### **Windows PowerShell:**

```powershell
$env:DATABRICKS_SERVER_HOSTNAME="<DATABRICKS_SERVER_HOSTNAME>"
$env:DATABRICKS_HTTP_PATH="<DATABRICKS_HTTP_PATH>"
$env:DATABRICKS_ACCESS_TOKEN="<DATABRICKS_ACCESS_TOKEN>"

nuvyn-executor metadata_extraction /path/to/data csv tenant123 --write-to-db
```

### **Windows CMD:**

```cmd
set DATABRICKS_SERVER_HOSTNAME=<DATABRICKS_SERVER_HOSTNAME>
set DATABRICKS_HTTP_PATH=<DATABRICKS_HTTP_PATH>
set DATABRICKS_ACCESS_TOKEN=<DATABRICKS_ACCESS_TOKEN>

nuvyn-executor metadata_extraction /path/to/data csv tenant123 --write-to-db
```

---

## 🔐 Using Databricks Secrets (Recommended for Production)

Instead of environment variables, use Databricks Secrets:

### **1. Create Secrets in Databricks**

```python
# In Databricks notebook
from databricks import sql
import os

# The secrets are already available in Databricks environment
# No need to set environment variables
```

### **2. Use in Databricks Job**

When creating a Databricks job, pass the credentials:

```json
{
  "existing_cluster_id": "cluster-id",
  "spark_python_task": {
    "python_file": "dbfs:/executor/main.py",
    "parameters": [
      "metadata_extraction",
      "/path/to/data",
      "csv",
      "tenant123",
      "--write-to-db"
    ]
  },
  "environment": {
    "DATABRICKS_SERVER_HOSTNAME": "{{secrets/scope/db-hostname}}",
    "DATABRICKS_HTTP_PATH": "{{secrets/scope/db-http-path}}",
    "DATABRICKS_ACCESS_TOKEN": "{{secrets/scope/db-token}}"
  }
}
```

---

## ✅ Verification

After setting the variables, the executor should show:

```
✅ Databricks SQL connection established
✅ Schema created: hive_metastore._executor_metadata
✅ Table created: _executor_metadata.sources
✅ Table created: _executor_metadata.tables
✅ Table created: _executor_metadata.columns
✅ Databricks SQL writer initialized
```

If you still see the warning, check:
1. Are the variable names correct? (case-sensitive)
2. Do they have the correct values?
3. Are they set in the same terminal session where you're running the command?

---

## 🎯 Complete Example

```bash
# Set credentials
export DATABRICKS_SERVER_HOSTNAME="<DATABRICKS_SERVER_HOSTNAME>"
export DATABRICKS_HTTP_PATH="<DATABRICKS_HTTP_PATH>"
export DATABRICKS_ACCESS_TOKEN="<DATABRICKS_ACCESS_TOKEN>"

# Run extraction with database writing
nuvyn-executor metadata_extraction \
  "https://storageaccount.blob.core.windows.net/container/file.csv?sp=r&sig=..." \
  azure_blob \
  client-123 \
  --write-to-db

# Expected output:
# ✅ Databricks SQL connection established
# ✅ Schema created: hive_metastore._executor_metadata
# ✅ Metadata written successfully
```

---

## 🚨 Security Notes

⚠️ **Never commit tokens to version control!**

- Use environment variables or secrets
- Never hardcode credentials in code
- Rotate tokens regularly
- Use service principals for production

---

## 📚 Additional Resources

- [Databricks SQL Warehouse Documentation](https://docs.databricks.com/sql/)
- [Databricks Access Tokens](https://docs.databricks.com/dev-tools/auth/pat.html)
- [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)

