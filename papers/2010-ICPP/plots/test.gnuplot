set term epslatex color
set output "test.tex"

set key inside left Left reverse
set title "Test" offset character 0, 0, 0 font "" norotate
set xlabel "X"
set ylabel "Y"

set data style linespoints

#set logscale xy
#set xtics (10, 20, 50, 100, 200, 500, 1000, 2000, 5000) rotate by -45 nomirror

plot 'test.dat' u 4:7 t "A:B" lc rgbcolor "#00ff00" pt 7 lt 1

set term png
set output "test.png"
replot
