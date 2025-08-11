#!/bin/bash


coverage run -m pytest ./tests/$1 -v -s
coverage report -m

