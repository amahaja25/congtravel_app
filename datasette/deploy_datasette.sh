#!/bin/bash

DB_FILE="databases/congtravel_master.db"

###
# Publish dataset
###

datasette "$DB_FILE" --cors