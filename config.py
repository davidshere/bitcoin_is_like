import os
import yaml


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
YAML_NAME = 'config.yaml'
YAML_ADDRESS = '{root}/{config}'.format(root=ROOT_DIR, config=YAML_NAME)

with open(YAML_ADDRESS, 'r') as f:
	credentials = f.read()
	config = yaml.load(credentials)

QUANDL_API_KEY = config['quandl_key']
SECRET_KEY = config['flask_wtf_secret_key']
WTF_CSRF_ENABLED = True
