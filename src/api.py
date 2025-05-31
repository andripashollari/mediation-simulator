from flask import Flask, jsonify
from src.db_config import get_db_connection

app = Flask(__name__)
