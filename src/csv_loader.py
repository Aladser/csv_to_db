import csv


class CSVLoader:
    @staticmethod
    def load(file):
        with open(file, newline='') as file:
            header = next(csv.reader(file))
            header_str = ', '.join(header)
            data = [row for row in csv.reader(file)]
            return {
                'header': header_str,
                'data': data
            }
