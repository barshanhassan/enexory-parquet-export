1. Create a baseline parquet export using: db_extrator.py
2. For a daily sync with the database use: parse_binlogs.sh.
NOTE: parse_binlogs.sh required a compiled consolidate.cpp program as a requirement.
3. For a quick row count match with parquet export and db, to see if row counts are same or not, use: row_integrity.py

-------------------------------------------------------------------------------------------------

Prerequisites to compiling c++ program:
sudo apt update
sudo apt get g++
sudo apt install ca-certificates lsb-release wget
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install libarrow-dev
sudo apt install libparquet-dev
sudo apt install libsnappy-dev

-------------------------------------------------------------------------------------------------

To compile c++ program:
g++ -O3 -std=c++17 consolidate.cpp -o consolidate -I. -larrow -lparquet -lsnappy

-------------------------------------------------------------------------------------------------

Prerequisites to running python program (have python 3.10 or 3.12):
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt