import json
import pandas as pd


class ConfigReader:
    def __init__(self, config_file, type='json'):
        self.config_file = config_file  # 配置文件路径
        self.type = type
        self.config_data = {}
        self.load_config()

    def load_config(self, type='json'):
        if self.type == 'json':
            self.config_data = self.load_json()
        elif self.type == 'excel':
            self.config_data = self.load_excel()

    def load_json(self):
        try:
            with open(self.config_file, 'r') as file:
                config_data = json.loads(file.read())
                return config_data
        except FileNotFoundError:
            print(f"Configuration file {self.config_file} not found.")
        except Exception as e:
            print(f"Error reading configuration file: {e}")

    def load_excel(self):
        try:
            config_data = pd.read_excel(self.config_file).to_dict(orient='records')
            return config_data
        except FileNotFoundError:
            print(f"Configuration file {self.config_file} not found.")
        except Exception as e:
            print(f"Error reading configuration file: {e}")

    def get(self, key):
        return self.config_data.get(key, None)
