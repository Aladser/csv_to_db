import psycopg2, csv


class CSVToDBParser:
    @staticmethod
    def parse_csv(file):
        with open(file, newline='') as file:
            header = next(csv.reader(file))
            header_str = ', '.join(header)
            data = [row for row in csv.reader(file)]
            return {
                'header': header_str,
                'data': data
            }

    @staticmethod
    def insert_into_db(conn_params: dict, schema_name: str, table_name: str, data_list: dict):
        """
        Вставить массив данных в ДБ
        :param conn_params: параметры соединения
        :param schema_name: название схемы
        :param table_name:  название таблицы
        :param data_list: данные
        """
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        for item in data_list['data']:
            query = (f"insert into {schema_name}.{table_name} ({data_list['header']}) "
                     f"values({', '.join(['%s'] * len(item))}) returning *")
            cursor.execute(query, item)
        conn.commit()

        cursor.close()
        conn.close()

