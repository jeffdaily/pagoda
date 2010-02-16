set term epslatex color
set output "strong_io.tex"

set title "Strong Scaling - IO - 48 OSTs"
set xlabel "Cores"
set ylabel "Bandwidth (GB/s)"

set data style linespoints

set logscale x
set xtics (16, 32, 64, 128, 256, 512, 1024, 2048)
#set xtics (16, 32, 64, 128, 256, 512, 1024, 2048) rotate by -45 nomirror

plot 'strong.dat' u 7:9  t "Read", \
     'strong.dat' u 7:10 t "Write"

set term png
set output "strong_io.png"
plot 'strong.dat' u 7:9  t "Read", \
     'strong.dat' u 7:10 t "Write"
