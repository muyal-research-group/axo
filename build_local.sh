#!/bin/bash
poetry lock
poetry install
rm ./dist/* 
poetry build


