import os
import sys

PACKAGE_NAME = 'awstool'

PACKAGE_DIR = os.path.join(os.getcwd(), PACKAGE_NAME)

if PACKAGE_DIR not in sys.path:
    sys.path.append(PACKAGE_DIR)
