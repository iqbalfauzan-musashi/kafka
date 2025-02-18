1. Update System Package
sudo apt update && sudo apt upgrade -y

2. Install Java (OpenJDK 11)
sudo apt install openjdk-11-jdk -y

3. Verifikasi instalasi Java
java -version
javac -version

4. Install Node.js dan NPM (menggunakan NodeSource repository)
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install nodejs -y

5. Verifikasi instalasi Node.js
node -v
npm -v

6. Install Kafka
# Buat direktori untuk Kafka
cd ~
mkdir kafka
cd kafka

# Download Kafka (versi terbaru yang stabil)
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz

# Extract file
tar xzf kafka_2.13-3.6.1.tgz
mv kafka_2.13-3.6.1/* .

7. Konfigurasi Kafka Server
# Edit server.properties
nano config/server.properties

# Tambahkan atau modifikasi baris berikut:
delete.topic.enable=true
auto.create.topics.enable=true

8. Buat Service untuk ZooKeeper
sudo nano /etc/systemd/system/zookeeper.service
isi dengan :
[Unit]
Description=Apache ZooKeeper server
Documentation=http://zookeeper.apache.org
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
ExecStart=/home/mapinai/kafka/bin/zookeeper-server-start.sh /home/mapinai/kafka/config/zookeeper.properties
ExecStop=/home/mapinai/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target

[Install]
WantedBy=multi-user.target

9. Buat Service untuk Kafka
sudo nano /etc/systemd/system/kafka.service
# isi dengan :
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/home/mapinai/kafka/bin/kafka-server-start.sh /home/mapinai/kafka/config/server.properties
ExecStop=/home/mapinai/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target

10. Start Services
# Reload daemon
sudo systemctl daemon-reload

# Start ZooKeeper
sudo systemctl start zookeeper
sudo systemctl enable zookeeper

# Start Kafka
sudo systemctl start kafka
sudo systemctl enable kafka

# Check status
sudo systemctl status zookeeper
sudo systemctl status kafka

11. Install MSSQL Server
# Import Microsoft GPG key
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

# Register Microsoft SQL Server Ubuntu repository
sudo add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/mssql-server-2019.list)"

# Install SQL Server
sudo apt update
sudo apt install -y mssql-server

# Configure SQL Server
sudo /opt/mssql/bin/mssql-conf setup

12. Set up project Anda
# Buat direktori project
mkdir ~/iot-kafka
cd ~/iot-kafka

# Copy semua file project Anda ke direktori ini
# Install dependencies
npm install kafkajs mssql

# Buat database dan tabel
sqlcmd -S localhost -U SA -P 'Admin123' -Q "
CREATE DATABASE IOT_HUB;
GO
USE IOT_HUB;
GO
CREATE TABLE MachineData (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    MachineCode NVARCHAR(50),
    Data NVARCHAR(MAX),
    IsUpdate BIT,
    CreatedAt DATETIME
);
GO
"
13. run
# Terminal 1 - Consumer
node consumer-app.js

# Terminal 2 - Producer
node producer-app.js
