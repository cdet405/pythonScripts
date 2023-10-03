#!/bin/bash

# Change to the desired directory
cd /path/to/your/directory

# Run the awk script
awk -v OFS=, '
    NR == 1 && FNR == 1 {
        file = FILENAME
        sub(/.csv$/, "", file)
        print "filename", $0
    }
    NR > 1 && FNR > 1{
        file = FILENAME
        sub(/.csv$/, "", file)
        print file, $0
    }
' *.csv > merged.csv
