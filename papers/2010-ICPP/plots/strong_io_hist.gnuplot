set term epslatex color
set output "strong_io_hist.tex"

set title "Strong Scaling - IO Bandwidth"
set key top left
set auto x
set xlabel "Cores"
set yrange [0:5.5]
set ylabel "Gigabytes/second"
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.9
#set xtic rotate by -45 scale 0
plot\
'strong.dat' u 3:xtic(1) ti col lt rgb "light-blue",\
'strong.dat' u 7 ti col lt rgb "turquoise",\
'strong.dat' u 4 ti col lt rgb "blue",\
'strong.dat' u 8 ti col lt rgb "dark-blue"

set term png
set output "strong_io_hist.png"
replot
