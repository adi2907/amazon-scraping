sudo apt update -y
sudo apt upgrade -y
sudo apt install fabric -y
sudo apt install mysql-client -y
sudo apt install python3-pip -y
sudo apt install -y firefox
sudo apt install -y redis-server
sudo apt install -y tinyproxy
pip3 install -U pip
chmod 600 ~/.ssh/id_rsa
ssh-keyscan -H github.com >> ~/.ssh/known_hosts
test -d "python-scraping" && echo "python-scraping directory already exists! Skipping git clone...\n" || git clone git@github.com:almetech/python-scraping.git
pip3 install -r python-scraping/requirements.txt
echo "Finished setup!"
echo "Testing dummy script..."
cd python-scraping && git pull && python3 scrapingtool/proxy.py
echo "Finished script Test!"