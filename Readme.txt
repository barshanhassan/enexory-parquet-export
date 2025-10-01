Prerequisites to compiling c++ program:
sudo apt update
sudo apt get g++
sudo apt get -y -V  ca-certificates lsb-release wget
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev
sudo apt install -y -V libparquet-dev
sudo apt install libsnappy-dev

-------------------------------------------------------------------------------------------------

To compile c++ program:
g++ -O3 -std=c++17 consolidate.cpp -o consolidate -I. -larrow -lparquet -lsnappy

-------------------------------------------------------------------------------------------------

Prerequisites to running python program (have python 3.10 or 3.12):
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt