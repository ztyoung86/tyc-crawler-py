#!/bin/bash

ENV=venv

pip install virtualenv
virtualenv $ENV
source $ENV/bin/activate

pip install -r requirements.txt