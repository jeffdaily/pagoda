set term epslatex color
set output "strong_hist.tex"

set title "Strong Scaling Test"
set xlabel "Cores"
set ylabel "Time (seconds)"

set auto x
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.9
#set logscale y

plot 'strong.dat' u 2:xtic(1) ti col lt rgb "green",\
     'strong.dat' u 5 ti col lt rgb "light-red"

set term png
set output "strong_hist.png"
replot
