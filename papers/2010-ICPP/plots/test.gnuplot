set term epslatex color
set output "test.tex"

set key inside left Left reverse
set title "Test" offset character 0, 0, 0 font "" norotate
set xlabel "X"
set ylabel "Y"

set data style linespoints

set logscale xy
set xtics (10, 20, 50, 100, 200, 500, 1000, 2000, 5000) rotate by -45 nomirror

plot \
        'test.dat' u 1:2 t "A:B" lc rgbcolor "#00ff00" pt 7 lt 1, \
        'test.dat' u 1:3 t "A:C" lc rgbcolor "#0000ff" pt 9 lt 1
