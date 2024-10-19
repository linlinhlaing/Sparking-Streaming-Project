import happybase
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def scan_hbase_table(table_name):
    try:
        # Connect to HBase
        connection = happybase.Connection('localhost')
        table = connection.table(table_name)

        # Scan the table and print the results
        logging.info(f"Scanning table: {table_name}")
        for key, data in table.scan():
            print(f"Row Key: {key.decode('utf-8')}")
            for column, value in data.items():
                print(f"Column: {column.decode('utf-8')}, Value: {value.decode('utf-8')}")

    except Exception as e:
        logging.error(f"Error scanning HBase table: {e}")
    finally:
        # Close the connection if it was opened
        try:
            connection.close()
            logging.info("HBase connection closed.")
        except:
            logging.warning("Failed to close HBase connection.")

if __name__ == "__main__":
    scan_hbase_table('weather_data')
