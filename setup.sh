sudo apt update -y
sudo apt upgrade -y
sudo apt install fabric -y
sudo apt install mysql-client -y
sudo apt install python3-pip -y
sudo apt install -y firefox
sudo apt install -y redis-server
pip3 install -U pip
chmod 600 ~/.ssh/id_rsa
ssh -o StrictHostKeyChecking=no git@github.com
cd python-scraping
pip3 install -r requirements.txt