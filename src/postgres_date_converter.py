class PostgresDateConverter:
    @staticmethod
    def convert(date):
        date_list = date.split('/')
        return f"{date_list[2]}-{date_list[0]}-{date_list[1]}"

