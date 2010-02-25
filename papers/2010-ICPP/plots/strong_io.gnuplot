set term epslatex color
set output "strong_io.tex"

set title "Strong Scaling - IO Bandwidth"
set xlabel "Cores"
set ylabel "Bandwidth (GB/s)"

set data style linespoints

set logscale x
set xtics (16, 32, 64, 128, 256, 512, 1024, 2048)
#set xtics (16, 32, 64, 128, 256, 512, 1024, 2048) rotate by -45 nomirror

plot 'strong.dat' u 1:3  t "Read",\
     'strong.dat' u 1:4  t "Write",\
     'strong.dat' u 1:7  t "ReadOpt",\
     'strong.dat' u 1:8  t "WriteOpt"

set term png
set output "strong_io.png"
replot
