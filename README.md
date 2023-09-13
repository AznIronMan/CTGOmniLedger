
# CTGOmniLedger
by [ClarkTribeGames, LLC](https://www.clarktribegames.com)

**Last Updated**: 2023.09.12 23:00:00 UTC

__Note__: Work in progress. Not yet ready for use.

CTGOmniLedger is a robust and scalable database solution designed to seamlessly aggregate and manage financial transactions from multiple sources. Developed by ClarkTribeGames, this platform aims to offer a centralized, accessible, and secure way to handle your diverse transactional needs.

## Features

- **Database Management**: Utilizes the `sqlite.py` script to handle SQLite database operations like table creation, reading, and writing.
- **Data Aggregation**: Efficiently gathers data from various sources using the `gather.py` script.
- **Scalability**: Designed to grow with your needs, handling large volumes of data efficiently.
- **Security**: Hashing and other security measures to ensure the confidentiality and integrity of your financial data.

## Requirements

- Python 3.10+
- Additional Python packages, which can be installed by running:

    ```bash
    pip install -r requirements.txt
    ```

## Installation and Usage

1. **Clone the Repository**

    ```bash
    git clone https://github.com/AznIronMan/CTGOmniLedger.git
    ```

2. **Navigate to the Directory**

    ```bash
    cd CTGOmniLedger
    ```

3. **Install Dependencies**

    ```bash
    pip install -r requirements.txt
    ```
4. **Run the `sqlite.py` Script** - Builds the initial database and tables.

    ```bash
    python sqlite.py
    ```
5. **Run the `gather.py` Script** - Gathers data from the various sources and writes it to the database.

    ```bash
    python gather.py
    ```



## Contributing

For contributions, please create a fork and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial 4.0 International License](LICENSE_CC_BY-NC_4.0.txt).

## Contact

For more information, please visit [ClarkTribeGames, LLC](https://www.clarktribegames.com) or reach out to us at info@clarktribegames.com.
