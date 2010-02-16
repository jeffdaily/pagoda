set term epslatex color
set output "strong.tex"

#set key inside left Left reverse
#set title "Strong" offset character 0, 0, 0 font "" norotate
set title "Strong Scaling Test"
set xlabel "Cores"
set ylabel "Time (s)"

set data style linespoints

set logscale xy
set xtics (16, 32, 64, 128, 256, 512, 1024, 2048)
#set xtics (16, 32, 64, 128, 256, 512, 1024, 2048) rotate by -45 nomirror

#plot 'strong.dat' u 7:13 t "Cores:Time" lc rgbcolor "#00ff00" pt 7 lt 1
plot 'strong.dat' u 7:13 t "Cores:Time"

set term png
set output "strong.png"
replot
