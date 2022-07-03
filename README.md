# PySpark Football KPI visualizer

Giving football data from Transfermarkt and Kaggle datasets a twist. Using PySpark to produce some interesting data from it.

## Usage
### API
Clone repo.
```bash
    git clone https://github.com/RoachLok/football-pyspark
```

Setup python env.
```bash
    python3 -m venv .venv
```

Enable it the environment for your shell and install dependencies from requirements.txt.
```bash
    # Bash
    source football-spark-venv/Scripts/activate
    pip install -r requirements.txt
```

Start the app.
```bash
    python app.py
```
### INTERFACE

On a new terminal run the interface with the API running.
```
Start the app.
```bash
    streamlit run interface/streamlit_template.py 
```

## Sources

### Datasets

Transfermarkt Data:
https://www.kaggle.com/datasets/davidcariboo/player-scores

Football Players Market Value Prediction:
https://www.kaggle.com/datasets/akarshsinghh/football-players-market-value-prediction

