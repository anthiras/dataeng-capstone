"""Configuration"""
import configparser
import os


def configure():
    """Configure AWS"""
    config = configparser.ConfigParser()
    config.read('config.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
