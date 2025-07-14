# frm-spark-databricks-mec

This guide provides a full set of instructions to set up Python environments, Git configuration, Docker usage, and deploying Apache Spark containers using Docker. Each section follows a logical sequence of environment setup and application development.

---

## 1. Python Environment Setup

### 1.1 Installing `virtualenv`

```bash
sudo apt update
sudo apt install python3 python3-pip
sudo apt install python3-virtualenv
virtualenv --version
```

### 1.2 Using `virtualenv` in Your Project

```bash
virtualenv env_name
source env_name/bin/activate
python --version
```

In VS Code, use **Python: Select Interpreter** to choose your environment.

### 1.3 Installing and Using `pyenv`

#### Step 1: Install Dependencies

```bash
sudo apt update
sudo apt install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev \
liblzma-dev python3-openssl git
```

#### Step 2: Install `pyenv`

```bash
curl https://pyenv.run | bash
```

#### Step 3: Configure Shell

Add to `~/.bashrc`, `~/.zshrc`, or `~/.profile`:

```bash
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
source ~/.bashrc
```

#### Step 4: Install Python 3.9

```bash
pyenv install 3.9.0
pyenv global 3.9.0
python --version
```

---

## 2. Git Setup and Usage

### 2.1 Changing the Remote URL

```bash
git remote -v
git remote set-url origin <new-repository-url>
git remote -v
git push -u origin <branch-name>
```

### 2.2 Configuring User Identity

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
# Or per project:
git config user.name "Your Name"
git config user.email "your.email@example.com"
git config --list
```

### 2.3 Common Git Commands

```bash
git add .
git commit -m "Initial commit"
git push origin main
```

---

## 3. Docker Setup

### 3.1 Install Docker

```bash
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce
```

### 3.2 Post-Install (Optional)

```bash
sudo usermod -aG docker $USER
# Logout and login again
```

### 3.3 Validate Docker

```bash
docker --version
sudo systemctl status docker
```

---

## 4. Using Docker with Spark

### 4.1 Pull Spark Image

```bash
docker pull bitnami/spark:latest
```

### 4.2 Run Spark Container with Mapped Folder

```bash
docker run -d --name spark-container \
  -v /home/fabiano/workspace/frm-spark-databricks-mec/src/spark/mod-1/scripts:/app \
  -w /app bitnami/spark:latest tail -f /dev/null
```

### 4.3 Spark Submit

```bash
docker exec spark-container spark-submit pr-3-app.py
```

### 4.4 Additional Options

```bash
docker exec spark-container spark-submit --conf spark.driver.memory=2g pr-3-app.py
docker exec spark-container spark-submit --verbose pr-3-app.py
docker stop spark-container
```

---

## 5. Building a Custom Docker Image

### 5.1 Create Dockerfile in `/scripts` Directory

```dockerfile
FROM bitnami/spark:latest
WORKDIR /app
COPY pr-3-app.py /app/
COPY users.json /app/
RUN pip install --no-cache-dir numpy
CMD ["tail", "-f", "/dev/null"]
```

### 5.2 Build and Run

```bash
docker build -t my-spark-app:latest .
docker run -d --name my-spark-container my-spark-app:latest
docker exec my-spark-container spark-submit pr-3-app.py
docker stop my-spark-container
```

---

## 6. Building a Spark Cluster using Docker Compose

### 6.1 Environment File

Create `.env` file in `build/`:

```env
APP_SRC_PATH=/home/fabiano/workspace/frm-spark-databricks-mec/src
APP_STORAGE_PATH=/home/fabiano/workspace/frm-spark-databricks-mec/storage
APP_LOG_PATH=/home/fabiano/workspace/frm-spark-databricks-mec/src/build/logs
APP_METRICS_PATH=/home/fabiano/workspace/frm-spark-databricks-mec/src/build/metrics
```

### 6.2 Required Folders

```bash
mkdir -p src storage logs metrics
```

### 6.3 Build Images

```bash
cd /home/fabiano/workspace/frm-spark-databricks-mec/build
docker build -t owshq-spark:3.5 -f Dockerfile.spark .
docker build -t owshq-spark-history-server:3.5 -f Dockerfile.history .
```

### 6.4 Start Cluster

✅ Solution – Set the variable correctly
Run this before docker compose up:

```bash
export APP_STORAGE_PATH=$HOME/workspace/formacaomecspark/storage
```

Then confirm it:

# Should return: /home/youruser/workspace/formacaomecspark/storage

```bash
echo $APP_STORAGE_PATH
```

Make sure the folder exists:

```bash
mkdir -p "$APP_STORAGE_PATH"
```

Now you can check:

```bash
ls $APP_STORAGE_PATH/output
```

```
sudo chown -R 1001:1001 ${APP_STORAGE_PATH}
sudo chmod -R 775 ${APP_STORAGE_PATH}
```

# Should work after your Spark job runs

```bash
docker compose up -d
docker ps
docker logs spark-master
```

🧪 Quick test in container
After running your job, test inside the container:

Now you can check:

```bash
ls $APP_STORAGE_PATH/output
```

# Should work after your Spark job runs


```bash
docker exec -it spark-master bash
ls -l /opt/bitnami/spark/storage/output
```


✅ Solution: Fix Permissions on the Host
Let’s fix the folder permissions on the host side, assuming ${APP_STORAGE_PATH} is e.g., ~/workspace/formacaomecspark/storage.

🛠 Step-by-step fix:
Ensure folder exists on host:

```bash
mkdir -p ~/workspace/formacaomecspark/storage/output/csv
```
Change ownership of the folder to match root (since your containers run as root):

```bash
sudo chown -R 0:0 ~/workspace/formacaomecspark/storage
```

Grant full access permissions to all users (for test purposes):

```bash
sudo chmod -R 777 ~/workspace/formacaomecspark/storage
```
⚠️ If this fixes the problem, we can later refine permissions to be more secure (e.g., 775 with group).

Rebuild and restart containers:

```bash
docker compose down -v
docker compose up --build
```


### 6.5 Components and Access

* Spark Master: [http://192.168.0.50:8080](http://192.168.0.50:8080)
* Spark History Server: [http://192.168.0.50:18080](http://192.168.0.50:18080)

### 6.6 Run Job in Cluster

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/get-users-json.py
```

### 6.7 Stop Cluster

```bash
cd /home/fabiano/workspace/frm-spark-databricks-mec/build
docker compose stop
docker compose down
```

---

This document merges and organizes all steps from system setup to Spark distributed job execution using Docker. Let me know if you'd like it split into multiple smaller README files by subject (e.g., Python, Git, Docker).