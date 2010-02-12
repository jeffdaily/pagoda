set term epslatex color
set output "strong.tex"

#set key inside left Left reverse
#set title "Strong" offset character 0, 0, 0 font "" norotate
set xlabel "X"
set ylabel "Y"

set data style linespoints

set logscale xy
#set xtics (10, 20, 50, 100, 200, 500, 1000, 2000, 5000) rotate by -45 nomirror

#plot 'strong.dat' u 7:13 t "Cores:Time" lc rgbcolor "#00ff00" pt 7 lt 1
plot 'strong.dat' u 7:13 t "Cores:Time"

set term png
set output "test.png"
replot
