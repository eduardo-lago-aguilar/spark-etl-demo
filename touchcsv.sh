#!/bin/bash

# Fixes the CSV by regenerating the first column from 1 to the number of lines
#
# Params:
#      $1   : CSV file to fix
#      $2   : (optional) output duplicated file, '_fixed' is appended to file if missing
#
# Examples
#
#   $ touchcsv data/people_1K.csv data/people_1K_fixed.csv
#
#   $ touchcsv data/people_2K.csv
#   (generates data/people_2K.csv_fixed)


file=${1:?}
target_file=${2:-"${file}_fixed"}

header() {
    cat <<EOF
id,first_name,last_name,email,gender
EOF
}

fix_data() {
    awk -v FS="," -v OFS="," 'BEGIN {c=0; getline} {$1=++c; print $0}' < ${file}
}

header > ${target_file}
fix_data >> ${target_file}
