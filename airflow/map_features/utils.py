from ConfigParser import SafeConfigParser
import os

config = SafeConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'default.properties'))
