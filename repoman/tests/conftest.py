import sys, os

# Make sure that the application source directory (this directory's parent) is
# on sys.path (hack...)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
