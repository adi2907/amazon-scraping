sudo apt install fabric -y
sudo apt install mysql-client -y
sudo apt install python3-pip -y
sudo apt install -y firefox
sudo apt install -y redis-server
chmod 600 ~/.ssh/id_rsa
ssh -o StrictHostKeyChecking=no git@github.com
pip3 install -U pip
git clone git@github.com:almetech/python-scraping.git
cd python-scraping
pip3 install -r requirements.txt