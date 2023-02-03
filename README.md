![Logo](logo.jpeg)

# GraphQL-Facebook-Marketplace-Ropa

Scrape facebook marketplace from any category into a .xlsx file from a previous day, needing a facebook account to perform it.

## Tech Stack

* **Programming language:** Python
* **IDE:** Jupyter Notebook

## Requirements
- python 3.8.5
- pip dependencies
    - python-dotenv == 0.21.0
    - openpyxl == 3.0.9
    - pandas == 1.4.2
    - selenium == 4.8.0
    - selenium-wire == 5.1.0
    - webdriver-manager == 3.8.5

## Instalation

**1. Clone the project**
```bash 
$ git clone https://github.com/JavierPortella/GraphQL-Facebook-Marketplace-Ropa.git
```

**2. Go to project path**

```bash
cd GraphQL-Facebook-Marketplace-Ropa
```

**3. Add environment variables (Similar with the .env.example file)**
```bash
$ touch .env
```

**4. Create a virtual environment. We have to options:**

* **Use Anaconda**
    
    - **Create the Anaconda environment**
        ```shell
        conda create -n name python=3.8.5
        ```

    - **Access the Anaconda environment**
        ```shell
        conda activate name
        ```

* **Use virtualenv library**
    - **Install the virtualenv library**
        ```shell
        pip install virtualenv
        ```
    
    - **Create the virtualenv environment**
        ```shell
        py -m venv venv
        ```

    - **Activate the virtualenv environment**
        ```shell
        .\env\Scripts\activate
        ```

**5. Install the dependencies in the virtual environment**
```shell
pip install -r requirements.txt
```

## Execution

**1. Execute the script**
```shell
py Facebook_MarketPlaceWS_Ropa.py
```

## License

[MIT](https://choosealicense.com/licenses/mit/)