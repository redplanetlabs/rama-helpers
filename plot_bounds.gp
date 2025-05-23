# This can be used to display the level-n-bounds.txt files produced by
# uncoordinatedTest

if (ARGC < 1) {
    print "Usage: gnuplot -c plot_bounds.gp <datafile>"
    exit
}

# ARG0 is the script name, ARG1 is first argument, etc.
datafile = ARG1

set term png
set output "rectangles.png"

set style fill transparent solid 0.5
set style rectangle fc lt -1 fs solid 0.15 noborder
# set xrange [0:100]
# set yrange [0:1000]
set grid

plot datafile using (($1+$3)/2):(($2+$4)/2):(abs($3-$1)/2):(abs($4-$2)/2) \
              with boxxyerrorbars title "Bounds"
