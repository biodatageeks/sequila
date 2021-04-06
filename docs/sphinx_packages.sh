#!/usr/bin/env bash


mkdir -p venv/sphinx
virtualenv venv/sphinx
source venv/sphinx/bin/activate
#core
#pip install -U sphinx
pip install -U sphinx==1.7.9
#packages
pip install -U sphinx_rtd_theme
pip install -U rst2pdf
#https://github.com/rst2pdf/rst2pdf/pull/770/files
pip install -U sphinxcontrib-github_ribbon
pip install -U "sphinxcontrib-bibtex<2.0.0"

