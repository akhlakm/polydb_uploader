# Data Preparation and Loading Scripts

## Usage
1. Install the requirements. You may want to create a new venv before this.
```sh
pip install -r requirements.txt
```

2. Clone the polydb repo inside CWD.
```sh
git clone https://github.com/...
```

3. Update the data preparation `prepare.py` or uploader `loader.py` script.

4. Use the sample `.env-sample` file to create your own `.env` file.

5. Run `main.py`.

```sh
python main.py --help
```

Zip the output folder using

```sh
sudo apt-get install zip
zip -r data.zip data_directory
```
